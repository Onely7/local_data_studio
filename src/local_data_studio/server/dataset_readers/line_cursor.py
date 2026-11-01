"""Block scanning and sparse byte cursor helpers shared by line-oriented readers."""

from __future__ import annotations

import os
from bisect import bisect_left
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from io import BufferedReader
from pathlib import Path

import numpy as np
from fastapi import HTTPException

from ..config import LINE_SCAN_BLOCK_BYTES, MAX_OFFSET_SCAN_BYTES, MAX_OFFSET_SCAN_ROWS
from ..line_index import LineOffsetIndex
from .contracts import ScanControl

# ``bytes.strip`` treats these ASCII bytes as whitespace. A physical line made
# only of them is skipped by every reader and never receives a row ID.
_WHITESPACE_BYTES = np.zeros(256, dtype=bool)
_WHITESPACE_BYTES[[9, 10, 11, 12, 13, 32]] = True
_LOWER_BYTES = np.arange(256, dtype=np.uint8)
_LOWER_BYTES[65:91] += 32
# Full scans analyze this many blocks concurrently; NumPy releases the GIL for
# the vectorized newline search, so the reader thread keeps the disk busy.
LINE_SCAN_WORKERS = max(1, min(4, os.cpu_count() or 1))


@dataclass(frozen=True, slots=True)
class LineBlock:
    """Complete physical lines read from one contiguous byte range.

    ``starts`` and ``ends`` hold block-relative offsets of the non-blank lines
    only, so position ``i`` is the one-based row ``first_row + i``. Each end
    points just after the line terminator. ``data`` may extend past ``size``
    with the start of a partial line that belongs to the next block.
    ``candidates`` lists row positions that may contain the scan's search
    needle and is ``None`` when the scan had no needle.
    """

    data: bytearray
    offset: int
    first_row: int
    starts: np.ndarray
    ends: np.ndarray
    size: int
    candidates: np.ndarray | None = None

    @property
    def row_count(self) -> int:
        """Return the number of non-blank lines in the block."""
        return len(self.starts)

    @property
    def end_offset(self) -> int:
        """Return the absolute file offset just after the block's complete lines."""
        return self.offset + self.size

    def line(self, index: int) -> bytearray:
        """Return one row's raw line, including its terminator."""
        return self.data[int(self.starts[index]) : int(self.ends[index])]

    def checkpoints(self, stride: int) -> list[tuple[int, int]]:
        """Return ``(row, byte offset)`` pairs for rows that are multiples of ``stride``."""
        return [(self.first_row + index, self.offset + int(self.starts[index])) for index in range((-self.first_row) % stride, self.row_count, stride)]


_LinePositions = tuple[np.ndarray, np.ndarray, np.ndarray | None]


def _candidate_positions(view: np.ndarray, starts: np.ndarray, ends: np.ndarray, needle: bytes) -> np.ndarray:
    """Return sorted row positions whose line contains ``needle`` with ASCII case folded.

    Only NumPy operations touch the whole block, so worker threads run without
    holding the GIL: anchor hits for the first byte in both ASCII cases are
    narrowed byte by byte, then mapped to the rows that fully contain them.
    """
    pattern = np.frombuffer(needle, dtype=np.uint8)
    if not len(pattern) or len(view) < len(pattern):
        return np.empty(0, dtype=np.int64)
    window = view[: len(view) - len(pattern) + 1]
    anchors = window == pattern[0]
    if 97 <= pattern[0] <= 122:
        anchors |= window == pattern[0] - 32
    hits = np.flatnonzero(anchors)
    for index in range(1, len(pattern)):
        if not len(hits):
            break
        hits = hits[_LOWER_BYTES[view[hits + index]] == pattern[index]]
    rows = np.searchsorted(ends, hits, side="right")
    inside = rows < len(starts)
    rows, hits = rows[inside], hits[inside]
    return np.unique(rows[(starts[rows] <= hits) & (hits + len(pattern) <= ends[rows])])


def _line_positions(data: bytearray, size: int, needle: bytes | None) -> _LinePositions:
    view = np.frombuffer(data, dtype=np.uint8, count=size)
    ends = np.flatnonzero(view == 10) + 1
    if size and view[size - 1] != 10:
        ends = np.append(ends, size)
    starts = np.zeros(len(ends), dtype=np.int64)
    starts[1:] = ends[:-1]
    keep = np.ones(len(starts), dtype=bool)
    # Only lines that begin with whitespace can be blank; confirm those few
    # candidates with the same ``bytes.strip`` rule the readers always used.
    for index in np.flatnonzero(_WHITESPACE_BYTES[view[starts]]):
        if not data[starts[index] : ends[index]].strip():
            keep[index] = False
    if not keep.all():
        starts, ends = starts[keep], ends[keep]
    candidates = _candidate_positions(view, starts, ends, needle) if needle is not None else None
    return starts, ends, candidates


def _iter_raw_chunks(file: BufferedReader, block_bytes: int, control: ScanControl | None) -> Iterator[tuple[bytearray, int]]:
    """Yield buffers ending with complete lines and the byte count of those lines.

    Bytes after the last newline carry over into the next buffer. A line with
    no newline in sight doubles the next read, so very long lines are copied a
    logarithmic number of times rather than once per block.
    """
    carry = b""
    while True:
        if control is not None:
            control.check_cancelled()
        buffer = bytearray(len(carry) + max(block_bytes, len(carry)))
        buffer[: len(carry)] = carry
        read = file.readinto(memoryview(buffer)[len(carry) :])
        filled = len(carry) + read
        if read == 0:
            if filled:
                yield buffer, filled
            return
        cut = buffer.rfind(b"\n", 0, filled) + 1
        if cut == 0:
            carry = bytes(buffer[:filled])
            continue
        yield buffer, cut
        carry = bytes(buffer[cut:filled])


def iter_line_blocks(
    path: Path,
    *,
    start_offset: int,
    first_row: int,
    control: ScanControl | None = None,
    block_bytes: int = LINE_SCAN_BLOCK_BYTES,
    needle: bytes | None = None,
    parallel: bool = False,
) -> Iterator[LineBlock]:
    """Yield blocks of complete lines from ``start_offset`` to the end of the file.

    Newline positions are located with vectorized byte scans instead of one
    Python call per line, and a block never splits a row. ``needle`` also
    records candidate rows for :func:`iter_candidate_rows`. ``parallel``
    analyzes up to ``LINE_SCAN_WORKERS`` blocks ahead on worker threads, which
    suits full scans; leave it off for short seeks that should not read ahead.
    Cancellation is checked before every read.
    """
    row = first_row
    offset = start_offset
    with ExitStack() as stack:
        file = stack.enter_context(path.open("rb"))
        file.seek(start_offset)
        pool = stack.enter_context(ThreadPoolExecutor(max_workers=LINE_SCAN_WORKERS, thread_name_prefix="local-data-studio-scan")) if parallel else None
        # Read-ahead grows one block per yield, so a search that finds its page
        # in the first block returns without waiting for later blocks.
        depth = 1
        pending: deque[tuple[bytearray, int, Future[_LinePositions]]] = deque()
        chunks = _iter_raw_chunks(file, block_bytes, control)
        while True:
            chunk = next(chunks, None)
            if chunk is not None:
                data, size = chunk
                if pool is not None:
                    positions = pool.submit(_line_positions, data, size, needle)
                else:
                    positions = Future()
                    positions.set_result(_line_positions(data, size, needle))
                pending.append((data, size, positions))
                if len(pending) < depth:
                    continue
            if not pending:
                return
            data, size, positions = pending.popleft()
            starts, ends, candidates = positions.result()
            block = LineBlock(data, offset, row, starts, ends, size, candidates)
            yield block
            offset += size
            row += block.row_count
            if pool is not None:
                depth = min(depth + 1, LINE_SCAN_WORKERS + 1)


def line_search_needle(query: str) -> bytes | None:
    """Return lowercase query bytes for fast case-insensitive candidate scans.

    Candidate scans fold only ASCII letters, so ASCII matches
    case-insensitively and characters without case, such as Japanese text,
    match exactly. ``None`` means the query contains
    cased non-ASCII letters, so callers must compare decoded lines one by one.
    """
    lowered = query.lower()
    if any(not char.isascii() and char.upper() != char.lower() for char in lowered) or "\N{REPLACEMENT CHARACTER}" in lowered:
        return None
    return lowered.encode("utf-8")


def iter_candidate_rows(block: LineBlock, needle: bytes | None) -> Iterator[int]:
    """Yield block row positions whose raw line may contain ``needle``, in file order."""
    if needle is None:
        yield from range(block.row_count)
        return
    candidates = block.candidates
    if candidates is None:
        candidates = _candidate_positions(np.frombuffer(block.data, dtype=np.uint8, count=block.size), block.starts, block.ends, needle)
    for position in candidates:
        yield int(position)


def _indexed_line_start(path: Path, target_row_number: int, hidden_row_ids: list[int]) -> tuple[int, int, int]:
    indexed = LineOffsetIndex(path).nearest_before(target_row_number)
    if indexed is None:
        return 0, 1, 0
    visible_rows = indexed.line_number - 1 - bisect_left(hidden_row_ids, indexed.line_number)
    return indexed.byte_offset, indexed.line_number, max(0, visible_rows)


def _line_cursor_for_offset(
    path: Path,
    offset: int,
    *,
    first_data_offset: int = 0,
    deleted_ids: set[int] | None = None,
) -> tuple[int, int]:
    """Return the byte offset and one-based row ID of the first visible row after ``offset`` rows.

    The scan starts at the nearest sparse checkpoint and records every
    checkpoint it passes, so later jumps into the same region are cheaper.

    Raises:
        HTTPException: The target is more than ``MAX_OFFSET_SCAN_ROWS`` rows
            past the nearest checkpoint and more than ``MAX_OFFSET_SCAN_BYTES``
            remain to be read.
    """
    if offset <= 0:
        return first_data_offset, 1

    hidden = deleted_ids or set()
    hidden_row_ids = sorted(hidden)
    start_offset, next_row_number, visible_rows_skipped = _indexed_line_start(path, offset + 1, hidden_row_ids)
    if start_offset < first_data_offset:
        start_offset, next_row_number, visible_rows_skipped = first_data_offset, 1, 0
    remaining = offset - visible_rows_skipped
    if remaining > MAX_OFFSET_SCAN_ROWS and path.stat().st_size - start_offset > MAX_OFFSET_SCAN_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"row {offset + 1:,} is too far from the nearest row index checkpoint; run Count Rows once to index this dataset",
        )

    index = LineOffsetIndex(path)
    end_offset, end_row = start_offset, next_row_number
    for block in iter_line_blocks(path, start_offset=start_offset, first_row=next_row_number):
        index.record_checkpoints(block.checkpoints(index.stride))
        block_end_row = block.first_row + block.row_count
        hidden_in_block = bisect_left(hidden_row_ids, block_end_row) - bisect_left(hidden_row_ids, block.first_row)
        if remaining < block.row_count - hidden_in_block:
            position = remaining
            if hidden_in_block:
                visible = -1
                for position in range(block.row_count):
                    if block.first_row + position not in hidden:
                        visible += 1
                        if visible == remaining:
                            break
            return block.offset + int(block.starts[position]), block.first_row + position
        remaining -= block.row_count - hidden_in_block
        end_offset, end_row = block.end_offset, block_end_row
    return end_offset, end_row


def _raw_line_value(path: Path, row_id: int, *, first_data_offset: int = 0) -> bytes:
    index = LineOffsetIndex(path)
    indexed = index.nearest_before(row_id)
    byte_offset = max(first_data_offset, indexed.byte_offset) if indexed else first_data_offset
    row_number = indexed.line_number if indexed else 1
    if row_id - row_number > MAX_OFFSET_SCAN_ROWS and path.stat().st_size - byte_offset > MAX_OFFSET_SCAN_BYTES:
        raise HTTPException(status_code=400, detail="row is too far from the nearest row index checkpoint; run Count Rows once to index this dataset")
    for block in iter_line_blocks(path, start_offset=byte_offset, first_row=row_number):
        index.record_checkpoints(block.checkpoints(index.stride))
        if row_id < block.first_row + block.row_count:
            return bytes(block.line(row_id - block.first_row))
    raise HTTPException(status_code=404, detail="row not found")

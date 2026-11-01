"""Bounded readers for ``.json`` datasets.

A ``.json`` file holds JSON Lines, one top-level array, or one other value.
JSON Lines files are read like ``.jsonl``. Arrays are streamed element by
element from byte cursors, so any page is read without loading the file.
Single values are parsed only when they fit ``MAX_JSON_PREVIEW_BYTES``.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, BinaryIO, Literal

from fastapi import HTTPException

from ..config import (
    COLUMN_LIMIT_WARNING,
    LINE_MAX_ROW_BYTES,
    LINE_SCAN_BLOCK_BYTES,
    LINE_SEEK_BLOCK_BYTES,
    MAX_COLUMNS,
    MAX_JSON_PREVIEW_BYTES,
    MAX_OFFSET_SCAN_BYTES,
    MAX_OFFSET_SCAN_ROWS,
    PREVIEW_PAGE_MAX_BYTES,
)
from ..db import build_table_response
from ..line_index import LineOffsetIndex
from ..serialization import serialize_rows
from . import jsonl
from .common import (
    JSON_NOT_TB_SAFE_WARNING,
    RECENT_ROWS,
    align_existing_rows,
    dataset_cache_key,
    decode_page_token_for,
    encode_page_token,
    extend_columns_from_value,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_column_type,
    merge_warnings,
    raw_row_values,
    row_from_mapping,
    token_int,
)
from .contracts import DatasetMetadata, ScanControl
from .line_cursor import row_index_required

JsonLayout = Literal["lines", "array", "value"]
JSON_LAYOUT_SAMPLE_BYTES = 1024 * 1024
JSON_SCHEMA_SAMPLE_ROWS = 100
JSON_SCHEMA_MAX_BYTES = 4 * 1024 * 1024
_WHITESPACE = " \t\r\n"
_DECODER = json.JSONDecoder()


def _first_value_end(text: str, start: int) -> int | None:
    try:
        return _DECODER.raw_decode(text, start)[1]
    except json.JSONDecodeError:
        return None


def _layout_from_first_line(text: str, whole_file: bool) -> JsonLayout:
    """Classify a document whose first value does not fit the sample.

    One long physical line is read as a row either way. A first line that is
    not valid JSON on its own starts a multi-line value.
    """
    if whole_file:
        return "value"
    first_line, newline, _ = text.partition("\n")
    if not newline:
        return "lines"
    try:
        json.loads(first_line)
    except json.JSONDecodeError:
        return "value"
    return "lines"


def _detect_layout(path: Path) -> JsonLayout:
    with path.open("rb") as file:
        sample = file.read(JSON_LAYOUT_SAMPLE_BYTES + 1)
    whole_file = len(sample) <= JSON_LAYOUT_SAMPLE_BYTES
    text = sample[:JSON_LAYOUT_SAMPLE_BYTES].decode("utf-8-sig", "replace")
    start = len(text) - len(text.lstrip(_WHITESPACE))
    if start >= len(text):
        return "value"
    end = _first_value_end(text, start)
    if end is None:
        return "array" if text[start] == "[" else _layout_from_first_line(text[start:], whole_file)
    following = text[end:].lstrip(" \t\r")
    if following.startswith("\n") and following.strip(_WHITESPACE):
        layout: JsonLayout = "lines"
    elif text[start] == "[":
        layout = "array"
    else:
        layout = "value" if whole_file or not following.strip(_WHITESPACE) else "lines"
    return layout


@lru_cache(maxsize=256)
def _cached_layout(path_text: str, size: int, modified_ns: int) -> JsonLayout:  # noqa: ARG001
    return _detect_layout(Path(path_text))


def json_layout(path: Path) -> JsonLayout:
    """Return whether a ``.json`` file holds JSON Lines, one array, or one value.

    The layout comes from the first ``JSON_LAYOUT_SAMPLE_BYTES`` and is cached
    by size and modification time.
    """
    stat = path.stat()
    return _cached_layout(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


@dataclass(frozen=True, slots=True)
class ArrayBlock:
    """Array elements found by one scan step, in the shape :class:`LineBlock` reports.

    ``starts`` holds absolute byte offsets of the elements; the first is row
    ``first_row``. ``end_offset`` is where the next element search begins.
    """

    first_row: int
    starts: list[int]
    end_offset: int

    @property
    def row_count(self) -> int:
        """Return the number of elements in the block."""
        return len(self.starts)

    def checkpoints(self, stride: int) -> list[tuple[int, int]]:
        """Return ``(row, byte offset)`` pairs for rows that are multiples of ``stride``."""
        return [(self.first_row + index, self.starts[index]) for index in range((-self.first_row) % stride, self.row_count, stride)]


class _ArrayCursor:
    """Read top-level array elements from a byte offset.

    Bytes are decoded as Latin-1, so string positions equal byte offsets and
    the structure parses correctly; returned elements are decoded again from
    their UTF-8 bytes.
    """

    def __init__(self, file: BinaryIO, byte_offset: int, *, initial_read: int, max_read: int, control: ScanControl | None = None) -> None:
        """Start reading at ``byte_offset``, which must precede an element or the closing bracket."""
        self._file = file
        self._file.seek(byte_offset)
        self._text = ""
        self._base = byte_offset
        self._position = 0
        self._read_size = initial_read
        self._max_read = max_read
        self._eof = False
        self._control = control

    @property
    def offset(self) -> int:
        """Return the byte offset of the next unread character."""
        return self._base + self._position

    def _fill(self) -> bool:
        if self._eof:
            return False
        if self._control is not None:
            self._control.check_cancelled()
        chunk = self._file.read(self._read_size)
        self._read_size = min(self._max_read, self._read_size * 2)
        if not chunk:
            self._eof = True
            return False
        if self._position:
            self._text = self._text[self._position :]
            self._base += self._position
            self._position = 0
        self._text += chunk.decode("latin-1")
        return True

    def next_element(self) -> tuple[int, int] | None:
        """Return the byte span of the next element, or ``None`` after the array ends.

        Raises:
            HTTPException: The array is malformed or an element is too large.
        """
        while True:
            while self._position < len(self._text) and self._text[self._position] in " \t\r\n,":
                self._position += 1
            if self._position < len(self._text):
                break
            if not self._fill():
                return None
        if self._text[self._position] == "]":
            return None
        while True:
            try:
                _, end = _DECODER.raw_decode(self._text, self._position)
            except json.JSONDecodeError as exc:
                if len(self._text) - self._position > LINE_MAX_ROW_BYTES:
                    raise HTTPException(status_code=413, detail=f"an element is larger than {LINE_MAX_ROW_BYTES // 2**20:,} MiB and cannot be read") from exc
                if self._fill():
                    continue
                raise HTTPException(status_code=400, detail=f"invalid JSON array near byte {self.offset:,}: {exc.msg}") from exc
            # A number at the end of the buffer may continue in the next read.
            if end == len(self._text) and self._fill():
                continue
            break
        span = (self._base + self._position, self._base + end)
        self._position = end
        return span

    def element_bytes(self, span: tuple[int, int]) -> bytes:
        """Return the original bytes of an element returned by the last call."""
        return self._text[span[0] - self._base : span[1] - self._base].encode("latin-1")


@lru_cache(maxsize=256)
def _cached_array_start(path_text: str, size: int, modified_ns: int) -> int:  # noqa: ARG001
    with Path(path_text).open("rb") as file:
        head = file.read(JSON_LAYOUT_SAMPLE_BYTES)
    start = head.find(b"[")
    if start < 0:
        raise HTTPException(status_code=400, detail="invalid JSON array")
    return start + 1


def _array_start(path: Path) -> int:
    """Return the byte offset just after the array's opening bracket."""
    stat = path.stat()
    return _cached_array_start(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


def iter_array_blocks(path: Path, *, start_offset: int, first_row: int, control: ScanControl | None = None) -> Iterator[ArrayBlock]:
    """Yield blocks of element offsets from a cursor to the end of the array.

    Each block covers about ``LINE_SCAN_BLOCK_BYTES`` of the file.
    """
    row = first_row
    with path.open("rb") as file:
        cursor = _ArrayCursor(file, start_offset, initial_read=LINE_SEEK_BLOCK_BYTES, max_read=LINE_SCAN_BLOCK_BYTES, control=control)
        while True:
            block_start = cursor.offset
            starts: list[int] = []
            while cursor.offset - block_start < LINE_SCAN_BLOCK_BYTES:
                span = cursor.next_element()
                if span is None:
                    if starts:
                        yield ArrayBlock(row, starts, cursor.offset)
                    return
                starts.append(span[0])
            yield ArrayBlock(row, starts, cursor.offset)
            row += len(starts)


def _array_cursor_for_offset(path: Path, offset: int, deleted_ids: set[int]) -> tuple[int, int]:
    """Return the byte offset and row ID of the first visible element after ``offset`` rows."""
    hidden = sorted(deleted_ids)
    index = LineOffsetIndex(path)
    indexed = index.nearest_before(offset + 1) if offset > 0 else None
    byte_offset, row = (indexed.byte_offset, indexed.line_number) if indexed else (_array_start(path), 1)
    visible_before = row - 1 - sum(1 for row_id in hidden if row_id < row)
    remaining = offset - visible_before
    if remaining > MAX_OFFSET_SCAN_ROWS and path.stat().st_size - byte_offset > MAX_OFFSET_SCAN_BYTES:
        raise row_index_required(offset + 1)
    checkpoints: list[tuple[int, int]] = []
    with path.open("rb") as file:
        cursor = _ArrayCursor(file, byte_offset, initial_read=LINE_SEEK_BLOCK_BYTES, max_read=LINE_SCAN_BLOCK_BYTES)
        while True:
            before = cursor.offset
            span = cursor.next_element()
            if span is None:
                index.record_checkpoints(checkpoints)
                return before, row
            if row % index.stride == 0:
                checkpoints.append((row, span[0]))
            if row not in deleted_ids:
                if remaining <= 0:
                    index.record_checkpoints(checkpoints)
                    return span[0], row
                remaining -= 1
            row += 1


def _read_array_page(path: Path, *, byte_offset: int, row_number: int, limit: int, deleted_ids: set[int]) -> tuple[list[tuple[int, bytes]], int, int, bool]:
    rows: list[tuple[int, bytes]] = []
    with path.open("rb") as file:
        cursor = _ArrayCursor(file, byte_offset, initial_read=LINE_SEEK_BLOCK_BYTES, max_read=LINE_SCAN_BLOCK_BYTES)
        row = row_number
        while True:
            before = cursor.offset
            if len(rows) >= limit or (rows and before - byte_offset >= PREVIEW_PAGE_MAX_BYTES):
                return rows, before, row, False
            span = cursor.next_element()
            if span is None:
                return rows, before, row, True
            if row not in deleted_ids:
                rows.append((row, cursor.element_bytes(span)))
            row += 1


def _decode_element(raw: bytes, row_id: int) -> Any:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"element {row_id:,} is not valid UTF-8 JSON") from exc


def _create_array_metadata(path: Path) -> DatasetMetadata:
    column_types: dict[str, str] = {}
    truncated = False
    start = _array_start(path)
    with path.open("rb") as file:
        cursor = _ArrayCursor(file, start, initial_read=LINE_SEEK_BLOCK_BYTES, max_read=JSON_SCHEMA_MAX_BYTES)
        for _ in range(JSON_SCHEMA_SAMPLE_ROWS):
            if cursor.offset - start > JSON_SCHEMA_MAX_BYTES:
                break
            span = cursor.next_element()
            if span is None:
                break
            try:
                value = json.loads(cursor.element_bytes(span))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            items = value.items() if isinstance(value, dict) else [("value", value)]
            for key, item in items:
                name = str(key)
                if name not in column_types and len(column_types) >= MAX_COLUMNS:
                    truncated = True
                    continue
                column_types[name] = merge_column_type(column_types.get(name), item)
    columns = [{"name": name, "type": type_name} for name, type_name in column_types.items()]
    return DatasetMetadata(file_format="json", columns=columns, warning=COLUMN_LIMIT_WARNING if truncated else None)


def _create_metadata(path: Path) -> DatasetMetadata:
    layout = json_layout(path)
    if layout == "lines":
        metadata = jsonl._create_jsonl_metadata(path)
        return DatasetMetadata(file_format="json", columns=metadata.columns, warning=metadata.warning)
    if layout == "array":
        return _create_array_metadata(path)
    warning = JSON_NOT_TB_SAFE_WARNING if path.stat().st_size > MAX_JSON_PREVIEW_BYTES else None
    return DatasetMetadata(file_format="json", columns=[{"name": "value", "type": "JSON"}], warning=warning)


def load_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    """Return metadata from a bounded sample of JSON Lines rows or array elements."""
    return load_or_create_metadata(path, _create_metadata, use_cache=use_cache)


def _preview_array(file_name: str, path: Path, limit: int, offset: int, page_token: str | None, deleted_ids: set[int]) -> dict[str, Any]:
    token = decode_page_token_for(page_token, "json")
    if page_token:
        byte_offset = token_int(token, "byte_offset", 0)
        row_number = token_int(token, "row_number", 1, minimum=1)
    elif offset > 0:
        byte_offset, row_number = _array_cursor_for_offset(path, offset, deleted_ids)
    else:
        byte_offset, row_number = _array_start(path), 1
    metadata = load_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    page_rows, next_offset, next_row, at_end = _read_array_page(path, byte_offset=byte_offset, row_number=row_number, limit=limit, deleted_ids=deleted_ids)
    cache_key = dataset_cache_key(path)
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    truncated = False
    for row_id, raw in page_rows:
        value = _decode_element(raw, row_id)
        old_count = len(columns)
        columns, row_truncated = extend_columns_from_value(columns, value)
        truncated = truncated or row_truncated
        align_existing_rows(rows, old_count, len(columns))
        rows.append([value.get(column) for column in columns] if isinstance(value, dict) else [value])
        row_ids.append(row_id)
        row_columns, row_values = raw_row_values(value)
        RECENT_ROWS.put(cache_key, row_id, row_columns, row_values, len(raw))
    next_token = None if at_end else encode_page_token({"kind": "json", "byte_offset": next_offset, "row_number": next_row})
    response = build_table_response(file_name, columns or ["value"], serialize_rows(rows), limit, max(0, row_number - 1), row_ids)
    response.update({"next_page_token": next_token, "has_next": not at_end})
    warning = merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if truncated:
        mark_columns_truncated(response)
    return response


def _load_value(path: Path) -> Any:
    if path.stat().st_size > MAX_JSON_PREVIEW_BYTES:
        raise HTTPException(status_code=400, detail=JSON_NOT_TB_SAFE_WARNING)
    try:
        with path.open("r", encoding="utf-8-sig") as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc


def preview(file_name: str, path: Path, limit: int, offset: int = 0, page_token: str | None = None, deleted_ids: set[int] | None = None) -> dict[str, Any]:
    """Return one bounded page of a JSON Lines file, array, or small single value."""
    hidden = deleted_ids or set()
    layout = json_layout(path)
    if layout == "lines":
        return jsonl.preview_jsonl(file_name, path, limit, offset, page_token, hidden, token_kind="json")
    if layout == "array":
        return _preview_array(file_name, path, limit, offset, page_token, hidden)
    if path.stat().st_size > MAX_JSON_PREVIEW_BYTES:
        response = build_table_response(file_name, ["value"], [], limit, 0, [])
        response.update({"next_page_token": None, "has_next": False, "warning": JSON_NOT_TB_SAFE_WARNING})
        return response
    value = _load_value(path)
    columns, truncated = extend_columns_from_value([], value)
    rows = [] if offset > 0 or 1 in hidden else [row_from_mapping(columns, value)]
    response = build_table_response(file_name, columns, rows, limit, 0, [1] if rows else [])
    if truncated:
        mark_columns_truncated(response)
    response.update({"next_page_token": None, "has_next": False})
    if truncated:
        response["warning"] = COLUMN_LIMIT_WARNING
    return response


def raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Return one one-based row without API response truncation."""
    layout = json_layout(path)
    if layout == "lines":
        return jsonl.raw_jsonl_row(path, row_id)
    if layout == "value":
        if row_id != 1:
            raise HTTPException(status_code=404, detail="row not found")
        return raw_row_values(_load_value(path))
    cached = RECENT_ROWS.get(dataset_cache_key(path), row_id)
    if cached is not None:
        return cached
    byte_offset, found_row = _array_cursor_for_offset(path, row_id - 1, set())
    page_rows, _, _, _ = _read_array_page(path, byte_offset=byte_offset, row_number=found_row, limit=1, deleted_ids=set())
    if not page_rows or page_rows[0][0] != row_id:
        raise HTTPException(status_code=404, detail="row not found")
    return raw_row_values(_decode_element(page_rows[0][1], row_id))


def array_start_offset(path: Path) -> int:
    """Return where element scans of a JSON array file begin."""
    return _array_start(path)

"""Sparse byte cursor helpers shared by line-oriented readers."""

from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException

from ..line_index import LineOffsetIndex


def _indexed_line_start(path: Path, target_row_number: int, hidden_row_ids: set[int]) -> tuple[int, int, int]:
    indexed = LineOffsetIndex(path).nearest_before(target_row_number)
    if indexed is None:
        return 0, 1, 0
    visible_rows = indexed.line_number - 1
    if hidden_row_ids:
        visible_rows -= sum(row_id < indexed.line_number for row_id in hidden_row_ids)
    return indexed.byte_offset, indexed.line_number, max(0, visible_rows)


def _line_cursor_for_offset(
    path: Path,
    offset: int,
    *,
    first_data_offset: int = 0,
    deleted_ids: set[int] | None = None,
) -> tuple[int, int]:
    """Return the byte offset and 1-based row ID after visible rows are skipped."""
    if offset <= 0:
        return first_data_offset, 1

    hidden_row_ids = deleted_ids or set()
    start_offset, next_row_number, visible_rows_skipped = _indexed_line_start(path, offset + 1, hidden_row_ids)
    byte_offset = max(first_data_offset, start_offset)
    if byte_offset == first_data_offset and start_offset < first_data_offset:
        next_row_number = 1
        visible_rows_skipped = 0

    index = LineOffsetIndex(path)
    with path.open("rb") as file:
        file.seek(byte_offset)
        while visible_rows_skipped < offset:
            line_start = file.tell()
            line = file.readline()
            if not line:
                return file.tell(), next_row_number
            if not line.strip():
                continue
            index.record(next_row_number, line_start)
            if next_row_number not in hidden_row_ids:
                visible_rows_skipped += 1
            next_row_number += 1
        return file.tell(), next_row_number


def _raw_line_value(path: Path, row_id: int, *, first_data_offset: int = 0) -> bytes:
    index = LineOffsetIndex(path)
    indexed = index.nearest_before(row_id)
    byte_offset = max(first_data_offset, indexed.byte_offset) if indexed else first_data_offset
    row_number = indexed.line_number if indexed else 1
    with path.open("rb") as file:
        file.seek(byte_offset)
        while True:
            line_start = file.tell()
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            index.record(row_number, line_start)
            if row_number == row_id:
                return line
            row_number += 1
    raise HTTPException(status_code=404, detail="row not found")

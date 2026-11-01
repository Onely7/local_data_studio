"""Bounded readers for comma- and tab-delimited datasets."""

from __future__ import annotations

import csv
import io
import sys
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any, Literal

import numpy as np

from ..config import COLUMN_LIMIT_WARNING, LINE_SEEK_BLOCK_BYTES, MAX_COLUMNS, PREVIEW_PAGE_MAX_BYTES
from ..db import build_table_response
from ..line_index import LineOffsetIndex
from ..serialization import serialize_value
from .common import (
    RECENT_ROWS,
    dataset_cache_key,
    decode_page_token_for,
    encode_page_token,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_warnings,
    token_int,
)
from .contracts import DatasetMetadata, ScanControl, SearchPage
from .line_cursor import _line_cursor_for_offset, _raw_line_value, iter_candidate_rows, iter_line_blocks, line_search_needle, read_line_page, record_terminators

CSV_FIELD_SIZE_LOCK = Lock()
# Row boundaries are decided from this much of the file's start.
CSV_ROW_MODE_SAMPLE_BYTES = 4 * 1024 * 1024
CsvRowMode = Literal["lines", "quoted", "inconsistent"]
INCONSISTENT_QUOTING_WARNING = "Some quoted fields span several lines, but quote characters are not balanced, so rows are split at every line break."


def parse_delimited_record(text: str, delimiter: str) -> list[str]:
    """Parse one CSV/TSV record without the standard library's small field cap."""
    with CSV_FIELD_SIZE_LOCK:
        previous_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(min(len(text), sys.maxsize))
            return next(csv.reader([text], delimiter=delimiter), [])
        finally:
            csv.field_size_limit(previous_limit)


def _count_records(text: str, delimiter: str) -> int | None:
    with CSV_FIELD_SIZE_LOCK:
        previous_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(min(max(len(text), 1), sys.maxsize))
            return sum(1 for _ in csv.reader(io.StringIO(text, newline=""), delimiter=delimiter))
        except csv.Error:
            return None
        finally:
            csv.field_size_limit(previous_limit)


def _detect_row_mode(path: Path, delimiter: str) -> CsvRowMode:
    with path.open("rb") as file:
        sample = file.read(CSV_ROW_MODE_SAMPLE_BYTES + 1)
    whole_file = len(sample) <= CSV_ROW_MODE_SAMPLE_BYTES
    sample = sample[:CSV_ROW_MODE_SAMPLE_BYTES]
    if b'"' not in sample:
        return "lines"
    view = np.frombuffer(sample, dtype=np.uint8)
    quoted_ends = record_terminators(view, quoted=True)
    end = len(sample) if whole_file else (int(quoted_ends[-1]) + 1 if len(quoted_ends) else 0)
    if end == 0:
        return "inconsistent"
    text = sample[:end].decode("utf-8-sig", "replace")
    lines = text.count("\n") + (0 if text.endswith("\n") else 1)
    records = _count_records(text, delimiter)
    if records is None or records == lines:
        return "lines"
    quoted_records = int(np.count_nonzero(quoted_ends < end)) + (0 if sample[end - 1 : end] == b"\n" else 1)
    return "quoted" if records == quoted_records else "inconsistent"


@lru_cache(maxsize=256)
def _cached_row_mode(path_text: str, size: int, modified_ns: int, delimiter: str) -> CsvRowMode:  # noqa: ARG001
    return _detect_row_mode(Path(path_text), delimiter)


def delimited_row_mode(path: Path, delimiter: str) -> CsvRowMode:
    """Return how rows of a CSV/TSV file are separated.

    ``"quoted"`` means quoted fields contain newlines, so rows end only at
    newlines outside double quotes, as in RFC 4180. ``"lines"`` means every
    newline ends a row. ``"inconsistent"`` means quoted fields span lines but
    quotes are unbalanced, so rows fall back to line breaks. The mode comes
    from the start of the file and is cached by size and modification time.
    """
    stat = path.stat()
    return _cached_row_mode(str(path.resolve()), stat.st_size, stat.st_mtime_ns, delimiter)


def delimited_rows_are_quoted(path: Path, delimiter: str) -> bool:
    """Return whether rows of this CSV/TSV file end only outside quoted fields."""
    return delimited_row_mode(path, delimiter) == "quoted"


def read_delimited_header(path: Path, delimiter: str) -> tuple[list[str], int]:
    """Return CSV/TSV columns and the byte offset immediately after the header."""
    if delimited_rows_are_quoted(path, delimiter):
        block = next(iter_line_blocks(path, start_offset=0, first_row=1, initial_block_bytes=LINE_SEEK_BLOCK_BYTES, quoted=True), None)
        header_bytes = bytes(block.line(0)) if block is not None and block.row_count else b""
        header_end = block.record_end(0) if block is not None and block.row_count else 0
    else:
        with path.open("rb") as file:
            header_bytes = file.readline()
            header_end = file.tell()
    if not header_bytes:
        return [], 0
    header = parse_delimited_record(header_bytes.decode("utf-8-sig", "replace").rstrip("\r\n"), delimiter)
    return [name or f"column_{index + 1}" for index, name in enumerate(header)], header_end


def _create_delimited_metadata(path: Path, delimiter: str) -> DatasetMetadata:
    columns, _ = read_delimited_header(path, delimiter)
    quoting_warning = INCONSISTENT_QUOTING_WARNING if delimited_row_mode(path, delimiter) == "inconsistent" else None
    return DatasetMetadata(
        file_format="tsv" if delimiter == "\t" else "csv",
        columns=[{"name": column, "type": "VARCHAR"} for column in columns[:MAX_COLUMNS]],
        warning=merge_warnings(COLUMN_LIMIT_WARNING if len(columns) > MAX_COLUMNS else None, quoting_warning),
    )


def load_delimited_metadata(path: Path, delimiter: str, *, use_cache: bool = True) -> DatasetMetadata:
    """Return header-only metadata for a CSV or TSV file."""
    return load_or_create_metadata(path, lambda source: _create_delimited_metadata(source, delimiter), use_cache=use_cache)


def _record_values(line: bytes, delimiter: str, width: int) -> list[Any]:
    values: list[Any] = parse_delimited_record(line.decode("utf-8-sig", "replace").rstrip("\r\n"), delimiter)
    return values + [None] * max(0, width - len(values))


def raw_delimited_row(path: Path, row_id: int, delimiter: str) -> tuple[list[str], list[Any]]:
    """Return one one-based CSV/TSV row using the nearest sparse checkpoint."""
    cached = RECENT_ROWS.get(dataset_cache_key(path), row_id)
    if cached is not None:
        return cached
    columns, first_data_offset = read_delimited_header(path, delimiter)
    line = _raw_line_value(path, row_id, first_data_offset=first_data_offset, quoted=delimited_rows_are_quoted(path, delimiter))
    return columns, _record_values(line, delimiter, len(columns))


def preview_delimited(
    file_name: str,
    path: Path,
    *,
    delimiter: str,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    """Return a bounded CSV/TSV page and byte-offset cursor for the next page.

    Parsed rows are kept in the recent-row cache for raw row requests.
    """
    source_columns, first_data_offset = read_delimited_header(path, delimiter)
    quoted = delimited_rows_are_quoted(path, delimiter)
    columns = source_columns[:MAX_COLUMNS]
    columns_truncated = len(source_columns) > MAX_COLUMNS
    kind = "tsv" if delimiter == "\t" else "csv"
    token = decode_page_token_for(page_token, kind)
    if page_token:
        byte_offset = max(first_data_offset, token_int(token, "byte_offset", first_data_offset))
        row_number = token_int(token, "row_number", 1, minimum=1)
    else:
        byte_offset, row_number = _line_cursor_for_offset(path, offset, first_data_offset=first_data_offset, deleted_ids=deleted_ids, quoted=quoted)
    page = read_line_page(
        path, byte_offset=byte_offset, row_number=row_number, limit=limit, max_bytes=PREVIEW_PAGE_MAX_BYTES, deleted_ids=deleted_ids, quoted=quoted
    )
    cache_key = dataset_cache_key(path)
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    for row_id, line in page.rows:
        values = _record_values(line, delimiter, len(source_columns))
        rows.append([serialize_value(value) for value in values[: len(columns)]])
        row_ids.append(row_id)
        RECENT_ROWS.put(cache_key, row_id, source_columns, values, len(line))

    has_next = not page.at_end
    next_token = encode_page_token({"kind": kind, "byte_offset": page.next_offset, "row_number": page.next_row}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, max(0, row_number - 1), row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = merge_warnings(load_delimited_metadata(path, delimiter).warning, COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated:
        mark_columns_truncated(response, len(source_columns))
    return response


def search_delimited(
    path: Path,
    query: str,
    delimiter: str,
    limit: int,
    deleted_ids: set[int],
    control: ScanControl,
    cursor: dict[str, Any] | None = None,
) -> SearchPage:
    """Scan CSV/TSV from ``cursor`` for at most ``limit`` matching visible rows.

    Raw blocks are filtered with a byte pattern first, so only candidate rows
    are decoded and parsed. The returned cursor resumes after the last match.
    """
    source_columns, first_data_offset = read_delimited_header(path, delimiter)
    quoted = delimited_rows_are_quoted(path, delimiter)
    columns = source_columns[:MAX_COLUMNS]
    warning = merge_warnings(load_delimited_metadata(path, delimiter).warning, COLUMN_LIMIT_WARNING if len(source_columns) > MAX_COLUMNS else None)
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    lowered = query.lower()
    needle = line_search_needle(query)
    size = max(path.stat().st_size, 1)
    index = LineOffsetIndex(path)
    start_offset = max(first_data_offset, token_int(cursor or {}, "byte_offset", first_data_offset))
    first_row = token_int(cursor or {}, "row_number", 1, minimum=1)
    for block in iter_line_blocks(path, start_offset=start_offset, first_row=first_row, control=control, needle=needle, parallel=True, quoted=quoted):
        index.record_checkpoints(block.checkpoints(index.stride))
        for position in iter_candidate_rows(block, needle):
            row_number = block.first_row + position
            if row_number in deleted_ids:
                continue
            text = block.line(position).decode("utf-8-sig", "replace").rstrip("\r\n")
            if lowered not in text.lower():
                continue
            values = parse_delimited_record(text, delimiter)
            padded = values + [None] * max(0, len(columns) - len(values))
            rows.append([serialize_value(value) for value in padded[: len(columns)]])
            row_ids.append(row_number)
            if len(rows) >= limit:
                next_cursor = {"byte_offset": block.record_end(position), "row_number": row_number + 1}
                return SearchPage(columns, rows, row_ids, warning, next_cursor)
        control.update(progress=min(block.end_offset / size, 0.999), message=f"Found {len(rows):,} rows")
    return SearchPage(columns, rows, row_ids, warning)

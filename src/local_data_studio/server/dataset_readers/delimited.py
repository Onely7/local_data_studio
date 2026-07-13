"""Bounded readers for comma- and tab-delimited datasets."""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from threading import Lock
from typing import Any

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS
from ..db import build_table_response
from ..line_index import LineOffsetIndex
from ..serialization import serialize_value
from .common import (
    decode_page_token_for,
    encode_page_token,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_warnings,
    token_int,
)
from .contracts import DatasetMetadata, ScanControl
from .line_cursor import _line_cursor_for_offset, _raw_line_value

CSV_FIELD_SIZE_LOCK = Lock()


def parse_delimited_record(text: str, delimiter: str) -> list[str]:
    """Parse one CSV/TSV record without the standard library's small field cap."""
    with CSV_FIELD_SIZE_LOCK:
        previous_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(min(len(text), sys.maxsize))
            return next(csv.reader([text], delimiter=delimiter), [])
        finally:
            csv.field_size_limit(previous_limit)


def read_delimited_header(path: Path, delimiter: str) -> tuple[list[str], int]:
    """Return CSV/TSV columns and the byte offset immediately after the header."""
    with path.open("rb") as file:
        header_bytes = file.readline()
        header_end = file.tell()
    if not header_bytes:
        return [], 0
    header = parse_delimited_record(header_bytes.decode("utf-8-sig", "replace").rstrip("\r\n"), delimiter)
    return [name or f"column_{index + 1}" for index, name in enumerate(header)], header_end


def _create_delimited_metadata(path: Path, delimiter: str) -> DatasetMetadata:
    columns, _ = read_delimited_header(path, delimiter)
    return DatasetMetadata(
        file_format="tsv" if delimiter == "\t" else "csv",
        columns=[{"name": column, "type": "VARCHAR"} for column in columns[:MAX_COLUMNS]],
        warning=COLUMN_LIMIT_WARNING if len(columns) > MAX_COLUMNS else None,
    )


def load_delimited_metadata(path: Path, delimiter: str, *, use_cache: bool = True) -> DatasetMetadata:
    """Return header-only metadata for a CSV or TSV file."""
    return load_or_create_metadata(path, lambda source: _create_delimited_metadata(source, delimiter), use_cache=use_cache)


def raw_delimited_row(path: Path, row_id: int, delimiter: str) -> tuple[list[str], list[Any]]:
    """Return one one-based CSV/TSV row using the nearest sparse checkpoint."""
    columns, first_data_offset = read_delimited_header(path, delimiter)
    line = _raw_line_value(path, row_id, first_data_offset=first_data_offset)
    values = parse_delimited_record(line.decode("utf-8-sig", "replace").rstrip("\r\n"), delimiter)
    return columns, values + [None] * max(0, len(columns) - len(values))


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
    """Return a bounded CSV/TSV page and byte-offset cursor for the next page."""
    source_columns, first_data_offset = read_delimited_header(path, delimiter)
    columns = source_columns[:MAX_COLUMNS]
    columns_truncated = len(source_columns) > MAX_COLUMNS
    kind = "tsv" if delimiter == "\t" else "csv"
    token = decode_page_token_for(page_token, kind)
    if page_token:
        byte_offset = token_int(token, "byte_offset", first_data_offset)
        row_number = token_int(token, "row_number", 1, minimum=1)
    else:
        byte_offset, row_number = _line_cursor_for_offset(
            path,
            offset,
            first_data_offset=first_data_offset,
            deleted_ids=deleted_ids,
        )
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    next_byte_offset = byte_offset
    next_row_number = row_number
    with path.open("rb") as file:
        file.seek(byte_offset)
        while len(rows) < limit:
            line = file.readline()
            if not line:
                next_byte_offset = file.tell()
                break
            next_byte_offset = file.tell()
            if not line.strip():
                continue
            current_row_id = next_row_number
            next_row_number += 1
            if current_row_id in deleted_ids:
                continue
            values = parse_delimited_record(line.decode("utf-8-sig", "replace").rstrip("\r\n"), delimiter)
            padded = values + [None] * max(0, len(columns) - len(values))
            rows.append([serialize_value(value) for value in padded[: len(columns)]])
            row_ids.append(current_row_id)

    has_next = next_byte_offset < path.stat().st_size
    next_token = encode_page_token({"kind": kind, "byte_offset": next_byte_offset, "row_number": next_row_number}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, max(0, row_number - 1), row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = merge_warnings(COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
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
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    """Scan CSV/TSV incrementally for at most ``limit`` matching visible rows."""
    source_columns, first_data_offset = read_delimited_header(path, delimiter)
    columns = source_columns[:MAX_COLUMNS]
    warning = COLUMN_LIMIT_WARNING if len(source_columns) > MAX_COLUMNS else None
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    lowered = query.lower()
    row_number = 0
    size = max(path.stat().st_size, 1)
    index = LineOffsetIndex(path)
    with path.open("rb") as file:
        file.seek(first_data_offset)
        while len(rows) < limit:
            control.check_cancelled()
            line_start = file.tell()
            line = file.readline()
            if not line:
                return columns, rows, row_ids, False, warning
            if not line.strip():
                continue
            row_number += 1
            index.record(row_number, line_start)
            if row_number in deleted_ids:
                continue
            text = line.decode("utf-8-sig", "replace").rstrip("\r\n")
            if lowered not in text.lower():
                continue
            values = parse_delimited_record(text, delimiter)
            padded = values + [None] * max(0, len(columns) - len(values))
            rows.append([serialize_value(value) for value in padded[: len(columns)]])
            row_ids.append(row_number)
            control.update(progress=min(file.tell() / size, 0.999), message=f"Found {len(rows):,} rows")
    return columns, rows, row_ids, True, warning

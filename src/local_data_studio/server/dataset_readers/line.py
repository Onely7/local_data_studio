"""Readers for newline-delimited JSON, CSV, and TSV datasets."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS
from ..db import build_table_response
from ..line_index import LineOffsetIndex
from ..serialization import serialize_value
from .common import (
    align_existing_rows,
    decode_page_token_for,
    encode_page_token,
    extend_columns_from_value,
    format_name,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_column_type,
    merge_warnings,
    raw_row_values,
    row_from_mapping,
    token_int,
)
from .contracts import DatasetMetadata, ScanControl

LINE_DATASET_EXTENSIONS = {".jsonl", ".csv", ".tsv"}
CSV_FIELD_SIZE_LOCK = Lock()


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
    return load_or_create_metadata(path, lambda source: _create_delimited_metadata(source, delimiter), use_cache=use_cache)


def _create_jsonl_metadata(path: Path, sample_rows: int = 100) -> DatasetMetadata:
    column_types: dict[str, str] = {}
    columns_truncated = False
    sampled = 0
    with path.open("rb") as file:
        while sampled < sample_rows:
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            sampled += 1
            if isinstance(value, dict):
                for key, item in value.items():
                    name = str(key)
                    if name not in column_types and len(column_types) >= MAX_COLUMNS:
                        columns_truncated = True
                        continue
                    column_types[name] = merge_column_type(column_types.get(name), item)
            else:
                column_types["value"] = merge_column_type(column_types.get("value"), value)
    columns = [{"name": name, "type": type_name} for name, type_name in column_types.items()]
    return DatasetMetadata(file_format="jsonl", columns=columns, warning=COLUMN_LIMIT_WARNING if columns_truncated else None)


def load_jsonl_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    return load_or_create_metadata(path, _create_jsonl_metadata, use_cache=use_cache)


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


def raw_jsonl_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    try:
        value = json.loads(_raw_line_value(path, row_id))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid jsonl format") from exc
    return raw_row_values(value)


def raw_delimited_row(path: Path, row_id: int, delimiter: str) -> tuple[list[str], list[Any]]:
    columns, first_data_offset = read_delimited_header(path, delimiter)
    line = _raw_line_value(path, row_id, first_data_offset=first_data_offset)
    values = parse_delimited_record(line.decode("utf-8-sig", "replace").rstrip("\r\n"), delimiter)
    return columns, values + [None] * max(0, len(columns) - len(values))


def preview_jsonl(
    file_name: str,
    path: Path,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    token = decode_page_token_for(page_token, "jsonl")
    if page_token:
        byte_offset = token_int(token, "byte_offset", 0)
        row_number = token_int(token, "row_number", 1, minimum=1)
    else:
        byte_offset, row_number = _line_cursor_for_offset(path, offset, deleted_ids=deleted_ids)
    metadata = load_jsonl_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    next_byte_offset = byte_offset
    next_row_number = row_number
    columns_truncated = False

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
            value = json.loads(line)
            old_column_count = len(columns)
            columns, row_columns_truncated = extend_columns_from_value(columns, value)
            columns_truncated = columns_truncated or row_columns_truncated
            align_existing_rows(rows, old_column_count, len(columns))
            rows.append(row_from_mapping(columns, value))
            row_ids.append(current_row_id)

    has_next = next_byte_offset < path.stat().st_size
    next_token = encode_page_token({"kind": "jsonl", "byte_offset": next_byte_offset, "row_number": next_row_number}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, max(0, row_number - 1), row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated or (metadata.warning and COLUMN_LIMIT_WARNING in metadata.warning):
        mark_columns_truncated(response)
    return response


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


def build_line_index_with_progress(path: Path, control: ScanControl) -> dict[str, Any]:
    """Build or refresh the sparse byte-offset index for a line-oriented dataset."""
    suffix = path.suffix.lower()
    if suffix not in LINE_DATASET_EXTENSIONS:
        raise HTTPException(status_code=400, detail="line index is only supported for jsonl, csv, and tsv")
    index = LineOffsetIndex(path)
    row_count = 0
    size = max(path.stat().st_size, 1)
    with path.open("rb") as file:
        if suffix in {".csv", ".tsv"}:
            file.readline()
        while True:
            control.check_cancelled()
            byte_offset = file.tell()
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            row_count += 1
            index.record(row_count, byte_offset)
            if row_count % 10_000 == 0:
                control.update(progress=min(file.tell() / size, 0.999), message=f"Indexed {row_count:,} rows")
    byte_count = path.stat().st_size
    index.mark_complete(row_count=row_count, byte_count=byte_count)
    status = index.status()
    control.update(progress=1.0, message=f"Indexed {row_count:,} rows")
    return {"format": format_name(path), "row_count": row_count, "byte_count": byte_count, "index": status}


def search_jsonl(
    path: Path,
    query: str,
    limit: int,
    deleted_ids: set[int],
    control: ScanControl,
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    metadata = load_jsonl_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    lowered = query.lower()
    row_number = 0
    size = max(path.stat().st_size, 1)
    columns_truncated = False
    index = LineOffsetIndex(path)
    with path.open("rb") as file:
        while len(rows) < limit:
            control.check_cancelled()
            line_start = file.tell()
            line = file.readline()
            if not line:
                warning = merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None)
                return columns, rows, row_ids, False, warning
            if not line.strip():
                continue
            row_number += 1
            index.record(row_number, line_start)
            if row_number in deleted_ids:
                continue
            if lowered not in line.decode("utf-8", "replace").lower():
                continue
            value = json.loads(line)
            old_column_count = len(columns)
            columns, row_columns_truncated = extend_columns_from_value(columns, value)
            columns_truncated = columns_truncated or row_columns_truncated
            align_existing_rows(rows, old_column_count, len(columns))
            rows.append(row_from_mapping(columns, value))
            row_ids.append(row_number)
            control.update(progress=min(file.tell() / size, 0.999), message=f"Found {len(rows):,} rows")
    warning = merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None)
    return columns, rows, row_ids, True, warning


def search_delimited(
    path: Path,
    query: str,
    delimiter: str,
    limit: int,
    deleted_ids: set[int],
    control: ScanControl,
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
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

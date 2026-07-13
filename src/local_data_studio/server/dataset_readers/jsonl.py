"""Bounded readers for newline-delimited JSON datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS
from ..db import build_table_response
from ..line_index import LineOffsetIndex
from .common import (
    align_existing_rows,
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
from .line_cursor import _line_cursor_for_offset, _raw_line_value

JSONL_SCHEMA_SAMPLE_ROWS = 100
JSONL_SCHEMA_MAX_SCANNED_LINES = 10_000
JSONL_SCHEMA_MAX_BYTES = 4 * 1024 * 1024


def _create_jsonl_metadata(path: Path, sample_rows: int = JSONL_SCHEMA_SAMPLE_ROWS) -> DatasetMetadata:
    column_types: dict[str, str] = {}
    columns_truncated = False
    sampled = 0
    scanned = 0
    with path.open("rb") as file:
        while sampled < sample_rows and scanned < JSONL_SCHEMA_MAX_SCANNED_LINES and file.tell() < JSONL_SCHEMA_MAX_BYTES:
            remaining_bytes = JSONL_SCHEMA_MAX_BYTES - file.tell()
            line = file.readline(remaining_bytes + 1)
            if not line:
                break
            if len(line) > remaining_bytes:
                break
            scanned += 1
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
    """Infer JSONL metadata within configured row and byte budgets."""
    return load_or_create_metadata(path, _create_jsonl_metadata, use_cache=use_cache)


def raw_jsonl_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Return one one-based JSONL row using the nearest sparse checkpoint."""
    try:
        value = json.loads(_raw_line_value(path, row_id))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid jsonl format") from exc
    return raw_row_values(value)


def preview_jsonl(
    file_name: str,
    path: Path,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    """Return a bounded JSONL page and byte-offset cursor for the next page."""
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


def search_jsonl(
    path: Path,
    query: str,
    limit: int,
    deleted_ids: set[int],
    control: ScanControl,
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    """Scan JSONL incrementally for at most ``limit`` matching visible rows."""
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

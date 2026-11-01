"""Bounded readers for newline-delimited JSON datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS, PREVIEW_PAGE_MAX_BYTES
from ..db import build_table_response
from ..line_index import LineOffsetIndex
from ..serialization import serialize_rows
from .common import (
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
from .contracts import DatasetMetadata, ScanControl, SearchPage
from .line_cursor import _line_cursor_for_offset, _raw_line_value, iter_candidate_rows, iter_line_blocks, line_search_needle, read_line_page

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


def _decode_jsonl_row(line: bytes, row_id: int) -> Any:
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"row {row_id:,} is not valid JSON: {exc.msg}") from exc


def raw_jsonl_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Return one one-based JSONL row using the nearest sparse checkpoint."""
    cached = RECENT_ROWS.get(dataset_cache_key(path), row_id)
    if cached is not None:
        return cached
    return raw_row_values(_decode_jsonl_row(_raw_line_value(path, row_id), row_id))


def preview_jsonl(
    file_name: str,
    path: Path,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    """Return a bounded JSONL page and byte-offset cursor for the next page.

    Decoded rows are kept in the recent-row cache for raw row requests.
    """
    token = decode_page_token_for(page_token, "jsonl")
    if page_token:
        byte_offset = token_int(token, "byte_offset", 0)
        row_number = token_int(token, "row_number", 1, minimum=1)
    else:
        byte_offset, row_number = _line_cursor_for_offset(path, offset, deleted_ids=deleted_ids)
    metadata = load_jsonl_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    page = read_line_page(path, byte_offset=byte_offset, row_number=row_number, limit=limit, max_bytes=PREVIEW_PAGE_MAX_BYTES, deleted_ids=deleted_ids)
    cache_key = dataset_cache_key(path)
    values: list[Any] = []
    row_ids: list[int] = []
    columns_truncated = False
    for row_id, line in page.rows:
        value = _decode_jsonl_row(line, row_id)
        columns, row_columns_truncated = extend_columns_from_value(columns, value)
        columns_truncated = columns_truncated or row_columns_truncated
        values.append(value)
        row_ids.append(row_id)
        row_columns, row_values = raw_row_values(value)
        RECENT_ROWS.put(cache_key, row_id, row_columns, row_values, len(line))

    raw_rows = [[value.get(column) for column in columns] if isinstance(value, dict) else [value] for value in values]
    has_next = not page.at_end
    next_token = encode_page_token({"kind": "jsonl", "byte_offset": page.next_offset, "row_number": page.next_row}) if has_next else None
    response = build_table_response(file_name, columns, serialize_rows(raw_rows), limit, max(0, row_number - 1), row_ids)
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
    cursor: dict[str, Any] | None = None,
) -> SearchPage:
    """Scan JSONL from ``cursor`` for at most ``limit`` matching visible rows.

    Raw blocks are filtered with a byte pattern first, so only candidate lines
    are decoded and parsed. The returned cursor resumes after the last match.
    """
    metadata = load_jsonl_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    lowered = query.lower()
    needle = line_search_needle(query)
    size = max(path.stat().st_size, 1)
    columns_truncated = False
    index = LineOffsetIndex(path)
    start_offset = token_int(cursor or {}, "byte_offset", 0)
    first_row = token_int(cursor or {}, "row_number", 1, minimum=1)
    for block in iter_line_blocks(path, start_offset=start_offset, first_row=first_row, control=control, needle=needle, parallel=True):
        index.record_checkpoints(block.checkpoints(index.stride))
        for position in iter_candidate_rows(block, needle):
            row_number = block.first_row + position
            if row_number in deleted_ids:
                continue
            line = block.line(position)
            if lowered not in line.decode("utf-8", "replace").lower():
                continue
            value = json.loads(line)
            old_column_count = len(columns)
            columns, row_columns_truncated = extend_columns_from_value(columns, value)
            columns_truncated = columns_truncated or row_columns_truncated
            align_existing_rows(rows, old_column_count, len(columns))
            rows.append(row_from_mapping(columns, value))
            row_ids.append(row_number)
            if len(rows) >= limit:
                warning = merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None)
                next_cursor = {"byte_offset": block.offset + int(block.ends[position]), "row_number": row_number + 1}
                return SearchPage(columns, rows, row_ids, warning, next_cursor)
        control.update(progress=min(block.end_offset / size, 0.999), message=f"Found {len(rows):,} rows")
    warning = merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None)
    return SearchPage(columns, rows, row_ids, warning)

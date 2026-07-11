"""Bounded Parquet metadata, preview, and row access."""

from __future__ import annotations

from bisect import bisect_right
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS, PARQUET_PREVIEW_BATCH_SIZE
from ..db import build_table_response
from ..serialization import serialize_value
from .common import (
    decode_page_token_for,
    encode_page_token,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_warnings,
    token_int,
)
from .contracts import DatasetMetadata

PARQUET_EXTENSION = ".parquet"


def _create_metadata(path: Path) -> DatasetMetadata:
    import pyarrow.parquet as pq  # noqa: PLC0415

    parquet_file = pq.ParquetFile(path)
    columns = [{"name": field.name, "type": str(field.type)} for index, field in enumerate(parquet_file.schema_arrow) if index < MAX_COLUMNS]
    warning = COLUMN_LIMIT_WARNING if len(parquet_file.schema_arrow) > MAX_COLUMNS else None
    return DatasetMetadata(file_format="parquet", columns=columns, warning=warning)


def load_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    return load_or_create_metadata(path, _create_metadata, use_cache=use_cache)


def raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    import pyarrow.parquet as pq  # noqa: PLC0415

    parquet_file = pq.ParquetFile(path)
    columns = parquet_file.schema_arrow.names
    target_offset = row_id - 1
    for row_group in range(parquet_file.num_row_groups):
        group_rows = parquet_file.metadata.row_group(row_group).num_rows
        if target_offset >= group_rows:
            target_offset -= group_rows
            continue
        records, _ = _read_group_slice(parquet_file, row_group, target_offset, 1, columns)
        if records:
            return columns, [records[0].get(column) for column in columns]
        break
    raise HTTPException(status_code=404, detail="row not found")


def _cursor_for_offset(parquet_file: Any, offset: int, deleted_ids: set[int]) -> tuple[int, int, int]:
    if offset <= 0:
        return 0, 0, 1
    group_ends: list[int] = []
    total_rows = 0
    for row_group in range(parquet_file.num_row_groups):
        total_rows += parquet_file.metadata.row_group(row_group).num_rows
        group_ends.append(total_rows)
    deleted = sorted(row_id for row_id in deleted_ids if 1 <= row_id <= total_rows)
    if offset >= total_rows - len(deleted):
        return parquet_file.num_row_groups, 0, total_rows + 1

    target_visible_row = offset + 1
    low, high = 1, total_rows
    while low < high:
        middle = (low + high) // 2
        visible_through_middle = middle - bisect_right(deleted, middle)
        if visible_through_middle >= target_visible_row:
            high = middle
        else:
            low = middle + 1
    absolute_row = low
    row_group = bisect_right(group_ends, absolute_row - 1)
    group_start = group_ends[row_group - 1] if row_group else 0
    return row_group, absolute_row - group_start - 1, absolute_row


def _read_group_slice(
    parquet_file: Any,
    row_group: int,
    row_offset: int,
    limit: int,
    columns: list[str],
) -> tuple[list[dict[str, Any]], int]:
    if limit <= 0:
        return [], row_offset
    records: list[dict[str, Any]] = []
    batch_start = 0
    next_row_offset = row_offset
    batch_size = max(PARQUET_PREVIEW_BATCH_SIZE, limit)
    for batch in parquet_file.iter_batches(batch_size=batch_size, row_groups=[row_group], columns=columns):
        batch_length = batch.num_rows
        batch_end = batch_start + batch_length
        if batch_end <= row_offset:
            batch_start = batch_end
            continue
        local_start = max(0, row_offset - batch_start)
        local_length = min(limit - len(records), batch_length - local_start)
        if local_length <= 0:
            break
        records.extend(batch.slice(local_start, local_length).to_pylist())
        next_row_offset = batch_start + local_start + local_length
        if len(records) >= limit:
            break
        batch_start = batch_end
    return records, next_row_offset


def preview(
    file_name: str,
    path: Path,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    import pyarrow.parquet as pq  # noqa: PLC0415

    token = decode_page_token_for(page_token, "parquet")
    parquet_file = pq.ParquetFile(path)
    source_columns = parquet_file.schema_arrow.names
    columns = source_columns[:MAX_COLUMNS]
    columns_truncated = len(source_columns) > MAX_COLUMNS
    if page_token:
        row_group = token_int(token, "row_group", 0)
        row_offset = token_int(token, "row_offset", 0)
        absolute_row = token_int(token, "absolute_row", 1, minimum=1)
    else:
        row_group, row_offset, absolute_row = _cursor_for_offset(parquet_file, offset, deleted_ids)
    current_row_group = row_group
    current_row_offset = row_offset
    current_absolute_row = absolute_row
    rows: list[list[Any]] = []
    row_ids: list[int] = []

    while len(rows) < limit and current_row_group < parquet_file.num_row_groups:
        group_rows = parquet_file.metadata.row_group(current_row_group).num_rows
        if current_row_offset >= group_rows:
            current_row_group += 1
            current_row_offset = 0
            continue
        remaining = limit - len(rows)
        records, next_group_offset = _read_group_slice(
            parquet_file,
            current_row_group,
            current_row_offset,
            remaining,
            columns,
        )
        for record in records:
            current_row_id = current_absolute_row
            current_absolute_row += 1
            current_row_offset += 1
            if current_row_id in deleted_ids:
                continue
            rows.append([serialize_value(record.get(column)) for column in columns])
            row_ids.append(current_row_id)
            if len(rows) >= limit:
                break
        current_row_offset = group_rows if not records else max(current_row_offset, next_group_offset)
        if current_row_offset >= group_rows:
            current_row_group += 1
            current_row_offset = 0

    has_next = current_row_group < parquet_file.num_row_groups
    next_token = (
        encode_page_token(
            {
                "kind": "parquet",
                "row_group": current_row_group,
                "row_offset": current_row_offset,
                "absolute_row": current_absolute_row,
            }
        )
        if has_next
        else None
    )
    response = build_table_response(file_name, columns, rows, limit, absolute_row - 1, row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = merge_warnings(COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated:
        mark_columns_truncated(response, len(source_columns))
    return response


def count_rows(path: Path) -> int:
    import pyarrow.parquet as pq  # noqa: PLC0415

    return int(pq.ParquetFile(path).metadata.num_rows)

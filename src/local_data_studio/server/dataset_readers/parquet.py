"""Bounded Parquet metadata, preview, and row access."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS, PARQUET_PREVIEW_BATCH_SIZE, PREVIEW_PAGE_MAX_BYTES
from ..db import build_table_response, open_connection, quote_ident
from ..serialization import serialize_rows
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
FILE_ROW_NUMBER = "file_row_number"


@dataclass(frozen=True, slots=True)
class ParquetLayout:
    """Footer facts used to address rows and bound page reads.

    ``row_bytes`` is the largest average uncompressed row size across row
    groups, so page budgets stay conservative for uneven files.
    """

    columns: tuple[str, ...]
    num_rows: int
    row_bytes: int


@lru_cache(maxsize=64)
def _cached_layout(path_text: str, size: int, modified_ns: int) -> ParquetLayout:  # noqa: ARG001
    import pyarrow.parquet as pq  # noqa: PLC0415

    metadata = pq.read_metadata(path_text)
    row_bytes = 1
    for row_group in range(metadata.num_row_groups):
        group = metadata.row_group(row_group)
        if group.num_rows:
            row_bytes = max(row_bytes, group.total_byte_size // group.num_rows)
    return ParquetLayout(tuple(metadata.schema.to_arrow_schema().names), int(metadata.num_rows), row_bytes)


def parquet_layout(path: Path) -> ParquetLayout:
    """Return cached footer facts keyed by the file's size and modification time."""
    stat = path.stat()
    return _cached_layout(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


def _create_metadata(path: Path) -> DatasetMetadata:
    import pyarrow.parquet as pq  # noqa: PLC0415

    schema = pq.read_schema(path)
    columns = [{"name": field.name, "type": str(field.type)} for field in list(schema)[:MAX_COLUMNS]]
    warning = COLUMN_LIMIT_WARNING if len(schema) > MAX_COLUMNS else None
    return DatasetMetadata(file_format="parquet", columns=columns, warning=warning)


def load_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    """Return schema metadata read from the Parquet footer only."""
    return load_or_create_metadata(path, _create_metadata, use_cache=use_cache)


def _read_rows_with_arrow(path: Path, columns: list[str], first_row: int, last_row: int) -> list[tuple[int, list[Any]]]:
    """Read physical rows with record batches sized from the footer's row width."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    parquet_file = pq.ParquetFile(path)
    batch_size = max(1, min(PARQUET_PREVIEW_BATCH_SIZE, PREVIEW_PAGE_MAX_BYTES // parquet_layout(path).row_bytes))
    rows: list[tuple[int, list[Any]]] = []
    group_start = 1
    for row_group in range(parquet_file.num_row_groups):
        group_end = group_start + parquet_file.metadata.row_group(row_group).num_rows - 1
        if group_end >= first_row:
            batch_start = group_start
            for batch in parquet_file.iter_batches(batch_size=batch_size, row_groups=[row_group], columns=columns):
                batch_end = batch_start + batch.num_rows - 1
                if batch_end >= first_row:
                    low = max(first_row, batch_start) - batch_start
                    high = min(last_row, batch_end) - batch_start + 1
                    for index, record in enumerate(batch.slice(low, high - low).to_pylist()):
                        rows.append((batch_start + low + index, [record.get(column) for column in columns]))
                if batch_end >= last_row:
                    return rows
                batch_start = batch_end + 1
        group_start = group_end + 1
    return rows


def _read_rows(path: Path, columns: list[str], first_row: int, last_row: int) -> list[tuple[int, list[Any]]]:
    """Return one-based physical rows in ``[first_row, last_row]`` in file order.

    DuckDB prunes row groups by ``file_row_number`` and decodes vector-sized
    slices, so memory stays bounded even for multi-gigabyte row groups. Files
    with a ``file_row_number`` column or types DuckDB cannot read fall back to
    bounded Arrow record batches.
    """
    if last_row < first_row:
        return []
    if FILE_ROW_NUMBER not in columns:
        select_list = ", ".join(quote_ident(column) for column in columns)
        try:
            with open_connection() as connection:
                result = connection.execute(
                    f"SELECT {FILE_ROW_NUMBER}, {select_list} FROM read_parquet(?, file_row_number = true) WHERE {FILE_ROW_NUMBER} BETWEEN ? AND ?",
                    [str(path), first_row - 1, last_row - 1],
                ).fetchall()
            return sorted(((int(row[0]) + 1, list(row[1:])) for row in result), key=lambda item: item[0])
        except duckdb.Error:
            pass
    return _read_rows_with_arrow(path, columns, first_row, last_row)


def raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Return one one-based row with every column and no preview truncation."""
    columns = list(parquet_layout(path).columns)
    rows = _read_rows(path, columns, row_id, row_id)
    if not rows:
        raise HTTPException(status_code=404, detail="row not found")
    return columns, rows[0][1]


def _absolute_row_for_offset(total_rows: int, offset: int, deleted_ids: set[int]) -> int:
    """Return the one-based physical row that is visible row ``offset + 1``."""
    if offset <= 0:
        return 1
    deleted = sorted(row_id for row_id in deleted_ids if 1 <= row_id <= total_rows)
    if offset >= total_rows - len(deleted):
        return total_rows + 1
    target_visible_row = offset + 1
    low, high = 1, total_rows
    while low < high:
        middle = (low + high) // 2
        if middle - bisect_right(deleted, middle) >= target_visible_row:
            high = middle
        else:
            low = middle + 1
    return low


def preview(
    file_name: str,
    path: Path,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    """Return a bounded page addressed by physical row number.

    Any row is reachable without scanning earlier rows. Pages hold fewer than
    ``limit`` rows when the footer's row width would exceed the page byte budget.
    """
    token = decode_page_token_for(page_token, "parquet")
    layout = parquet_layout(path)
    source_columns = list(layout.columns)
    columns = source_columns[:MAX_COLUMNS]
    columns_truncated = len(source_columns) > MAX_COLUMNS
    if page_token:
        start_row = token_int(token, "absolute_row", 1, minimum=1)
    else:
        start_row = _absolute_row_for_offset(layout.num_rows, offset, deleted_ids)
    page_rows = max(1, min(limit, PREVIEW_PAGE_MAX_BYTES // layout.row_bytes))
    raw_rows: list[list[Any]] = []
    row_ids: list[int] = []
    next_row = start_row
    while len(raw_rows) < page_rows and next_row <= layout.num_rows:
        last_row = min(layout.num_rows, next_row + page_rows - len(raw_rows) - 1)
        for row_id, values in _read_rows(path, columns, next_row, last_row):
            if row_id not in deleted_ids:
                raw_rows.append(values)
                row_ids.append(row_id)
        next_row = last_row + 1

    has_next = next_row <= layout.num_rows
    next_token = encode_page_token({"kind": "parquet", "absolute_row": next_row}) if has_next else None
    response = build_table_response(file_name, columns, serialize_rows(raw_rows), limit, start_row - 1, row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = merge_warnings(COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated:
        mark_columns_truncated(response, len(source_columns))
    return response


def count_rows(path: Path) -> int:
    """Return the physical row count from Parquet footer metadata."""
    return parquet_layout(path).num_rows

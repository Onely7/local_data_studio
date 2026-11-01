"""Bounded Parquet metadata, preview, and row access."""

from __future__ import annotations

import threading
from bisect import bisect_right
from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS, PARQUET_PREVIEW_BATCH_SIZE, PREVIEW_PAGE_MAX_BYTES
from ..db import build_table_response
from ..serialization import serialize_rows
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
from .contracts import DatasetMetadata

PARQUET_EXTENSION = ".parquet"
# Parsed footers of TB-scale files can take hundreds of megabytes, so only a
# few stay cached. Each hit saves a full footer parse.
PARQUET_FOOTER_CACHE_SIZE = 4
# Thrift limits large enough for footers with millions of column chunks.
PARQUET_THRIFT_STRING_LIMIT = 2**31 - 1
PARQUET_THRIFT_CONTAINER_LIMIT = 2**31 - 1
# Open readers positioned where a page ended let the next page continue
# without decoding its row group again from the start.
PARQUET_CURSOR_CACHE_SIZE = 4
# Buffered column streams read data pages on demand instead of whole column
# chunks. The buffer budget is shared by the columns of one reader.
PARQUET_READER_BUFFER_BYTES = 32 * 1024 * 1024
PARQUET_COLUMN_BUFFER_MIN_BYTES = 64 * 1024
PARQUET_COLUMN_BUFFER_MAX_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ParquetLayout:
    """Footer facts used to address rows and bound page reads.

    ``row_bytes`` is the largest average uncompressed row size across row
    groups, so page budgets stay conservative for uneven files.
    ``row_group_starts`` holds the one-based first row of each row group.
    ``metadata`` is the parsed footer, passed to readers so they never parse
    it again.
    """

    columns: tuple[str, ...]
    num_rows: int
    row_bytes: int
    row_group_starts: tuple[int, ...]
    metadata: Any = field(compare=False, repr=False)


@lru_cache(maxsize=PARQUET_FOOTER_CACHE_SIZE)
def _cached_layout(path_text: str, size: int, modified_ns: int) -> ParquetLayout:  # noqa: ARG001
    import pyarrow.parquet as pq  # noqa: PLC0415

    with pq.ParquetFile(
        path_text,
        thrift_string_size_limit=PARQUET_THRIFT_STRING_LIMIT,
        thrift_container_size_limit=PARQUET_THRIFT_CONTAINER_LIMIT,
    ) as parquet_file:
        metadata = parquet_file.metadata
    row_bytes = 1
    starts: list[int] = []
    next_start = 1
    for row_group in range(metadata.num_row_groups):
        group = metadata.row_group(row_group)
        starts.append(next_start)
        next_start += group.num_rows
        if group.num_rows:
            row_bytes = max(row_bytes, group.total_byte_size // group.num_rows)
    return ParquetLayout(tuple(metadata.schema.to_arrow_schema().names), int(metadata.num_rows), row_bytes, tuple(starts), metadata)


def parquet_layout(path: Path) -> ParquetLayout:
    """Return cached footer facts keyed by the file's size and modification time."""
    stat = path.stat()
    return _cached_layout(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


def _create_metadata(path: Path) -> DatasetMetadata:
    schema = parquet_layout(path).metadata.schema.to_arrow_schema()
    columns = [{"name": field.name, "type": str(field.type)} for field in list(schema)[:MAX_COLUMNS]]
    warning = COLUMN_LIMIT_WARNING if len(schema) > MAX_COLUMNS else None
    return DatasetMetadata(file_format="parquet", columns=columns, warning=warning)


def load_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    """Return schema metadata read from the Parquet footer only."""
    return load_or_create_metadata(path, _create_metadata, use_cache=use_cache)


@dataclass(slots=True)
class _RowCursor:
    """A batch reader whose next row is ``next_row``.

    ``pending`` holds rows of a partly returned batch; the reader yields the
    rows after them.
    """

    parquet_file: Any
    batches: Iterator[Any]
    next_row: int
    pending: Any = None

    def close(self) -> None:
        """Release the reader's file handle."""
        self.parquet_file.close()


class _CursorCache:
    """Small LRU of positioned readers, each lent to one request at a time."""

    def __init__(self, capacity: int) -> None:
        """Create an empty cache holding at most ``capacity`` readers."""
        self._capacity = capacity
        self._lock = threading.Lock()
        self._cursors: OrderedDict[tuple[Any, ...], _RowCursor] = OrderedDict()

    def take(self, key: tuple[Any, ...]) -> _RowCursor | None:
        """Remove and return the reader stored under ``key``, if any."""
        with self._lock:
            return self._cursors.pop(key, None)

    def put(self, key: tuple[Any, ...], cursor: _RowCursor) -> None:
        """Store a reader and close the least recently used ones beyond capacity."""
        with self._lock:
            replaced = self._cursors.pop(key, None)
            self._cursors[key] = cursor
            evicted = [self._cursors.popitem(last=False)[1] for _ in range(max(0, len(self._cursors) - self._capacity))]
        for stale in ([replaced] if replaced is not None else []) + evicted:
            stale.close()

    def clear(self) -> None:
        """Close every stored reader."""
        with self._lock:
            cursors = list(self._cursors.values())
            self._cursors.clear()
        for cursor in cursors:
            cursor.close()


_CURSORS = _CursorCache(PARQUET_CURSOR_CACHE_SIZE)


def close_parquet_readers() -> None:
    """Close every cached Parquet reader, for example before a file is rewritten."""
    _CURSORS.clear()


def _batch_rows(layout: ParquetLayout) -> int:
    return max(1, min(PARQUET_PREVIEW_BATCH_SIZE, PREVIEW_PAGE_MAX_BYTES // layout.row_bytes))


def _open_cursor(path: Path, layout: ParquetLayout, columns: list[str], first_row: int) -> _RowCursor:
    """Open a reader at the row group holding ``first_row`` and skip to that row.

    Rows before ``first_row`` in its row group are decoded and discarded,
    because Parquet pages can only be read from the start of a column chunk.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    group = max(0, bisect_right(layout.row_group_starts, first_row) - 1)
    buffer_size = max(PARQUET_COLUMN_BUFFER_MIN_BYTES, min(PARQUET_COLUMN_BUFFER_MAX_BYTES, PARQUET_READER_BUFFER_BYTES // max(1, len(columns))))
    parquet_file = pq.ParquetFile(path, metadata=layout.metadata, buffer_size=buffer_size, pre_buffer=False)
    batches = parquet_file.iter_batches(
        batch_size=_batch_rows(layout),
        row_groups=list(range(group, len(layout.row_group_starts))),
        columns=columns,
        use_threads=False,
    )
    cursor = _RowCursor(parquet_file, batches, layout.row_group_starts[group])
    try:
        while cursor.next_row < first_row:
            batch = next(batches, None)
            if batch is None:
                break
            if cursor.next_row + batch.num_rows > first_row:
                cursor.pending = batch.slice(first_row - cursor.next_row)
                cursor.next_row = first_row
                break
            cursor.next_row += batch.num_rows
    except BaseException:
        cursor.close()
        raise
    return cursor


def _read_rows(path: Path, columns: list[str], first_row: int, last_row: int) -> list[tuple[int, list[Any]]]:
    """Return one-based physical rows in ``[first_row, last_row]`` in file order.

    Readers use the cached footer and buffered column streams, so a page
    reads only the data pages it needs. The reader is kept where the page
    ended, so paging forward continues without decoding the row group again.
    """
    layout = parquet_layout(path)
    last_row = min(last_row, layout.num_rows)
    if last_row < first_row or not layout.row_group_starts:
        return []
    base_key = (dataset_cache_key(path), tuple(columns))
    cursor = _CURSORS.take((*base_key, first_row)) or _open_cursor(path, layout, columns, first_row)
    rows: list[tuple[int, list[Any]]] = []
    try:
        while cursor.next_row <= last_row:
            batch = cursor.pending if cursor.pending is not None else next(cursor.batches, None)
            cursor.pending = None
            if batch is None:
                break
            take = min(batch.num_rows, last_row - cursor.next_row + 1)
            values = [batch.column(index).slice(0, take).to_pylist() for index in range(batch.num_columns)]
            rows.extend((cursor.next_row + offset, list(row)) for offset, row in enumerate(zip(*values, strict=True)))
            if take < batch.num_rows:
                cursor.pending = batch.slice(take)
            cursor.next_row += take
    except BaseException:
        cursor.close()
        raise
    if cursor.next_row <= layout.num_rows:
        _CURSORS.put((*base_key, cursor.next_row), cursor)
    else:
        cursor.close()
    return rows


def raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Return one one-based row with every column and no preview truncation."""
    cached = RECENT_ROWS.get(dataset_cache_key(path), row_id)
    if cached is not None:
        return cached
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

    Any row is reachable without scanning earlier row groups. Pages hold
    fewer than ``limit`` rows when the footer's row width would exceed the
    page byte budget. Rows of files with at most ``MAX_COLUMNS`` columns are
    kept in the recent-row cache for raw row requests.
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
    cache_key = dataset_cache_key(path)
    raw_rows: list[list[Any]] = []
    row_ids: list[int] = []
    next_row = start_row
    while len(raw_rows) < page_rows and next_row <= layout.num_rows:
        last_row = min(layout.num_rows, next_row + page_rows - len(raw_rows) - 1)
        for row_id, values in _read_rows(path, columns, next_row, last_row):
            if row_id not in deleted_ids:
                raw_rows.append(values)
                row_ids.append(row_id)
                if not columns_truncated:
                    RECENT_ROWS.put(cache_key, row_id, columns, values, layout.row_bytes)
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

"""Dataset reader dispatch and DuckDB fallbacks."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, DEFAULT_LIMIT
from ..db import (
    build_table_response,
    describe_relation,
    fetch_rows_with_rowid,
    normalize_pagination,
    open_connection,
    quote_ident,
    relation_sql,
    relation_with_rowid_sql,
)
from . import json_reader, line, parquet
from .common import (
    TB_SAFE_EXTENSIONS,
    decode_page_token_for,
    encode_page_token,
    format_name,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_warnings,
    reject_large_offset_without_token,
    token_int,
    visible_row_count,
)
from .contracts import DatasetMetadata, ScanControl


def _create_duckdb_metadata(path: Path) -> DatasetMetadata:
    if path.suffix.lower() not in TB_SAFE_EXTENSIONS:
        return DatasetMetadata(
            file_format=format_name(path),
            columns=[{"name": "value", "type": "UNKNOWN"}],
            warning="This format is not TB-safe for schema inference.",
        )
    relation, params = relation_sql(path)
    with open_connection() as connection:
        columns = describe_relation(connection, relation, params)
    return DatasetMetadata(file_format=format_name(path), columns=columns)


def load_dataset_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    """Load lightweight metadata without scanning a full supported dataset."""
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return parquet.load_metadata(path, use_cache=use_cache)
    if suffix == ".jsonl":
        return line.load_jsonl_metadata(path, use_cache=use_cache)
    if suffix == ".csv":
        return line.load_delimited_metadata(path, ",", use_cache=use_cache)
    if suffix == ".tsv":
        return line.load_delimited_metadata(path, "\t", use_cache=use_cache)
    if suffix == ".json":
        return json_reader.load_metadata(path, use_cache=use_cache)
    return load_or_create_metadata(path, _create_duckdb_metadata, use_cache=use_cache)


def fetch_raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Read one user-selected row without Preview response truncation."""
    if row_id < 1:
        raise HTTPException(status_code=400, detail="row_id must be positive")
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return line.raw_jsonl_row(path, row_id)
    if suffix == ".csv":
        return line.raw_delimited_row(path, row_id, ",")
    if suffix == ".tsv":
        return line.raw_delimited_row(path, row_id, "\t")
    if suffix == ".parquet":
        return parquet.raw_row(path, row_id)
    if suffix == ".json":
        return json_reader.raw_row(path, row_id)
    raise HTTPException(status_code=400, detail="raw row retrieval is not supported for this file format")


def _duckdb_preview(
    file_name: str,
    path: Path,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: list[int],
) -> dict[str, Any]:
    token = decode_page_token_for(page_token, "duckdb")
    if page_token:
        offset = token_int(token, "offset", offset)
    relation, params = relation_with_rowid_sql(path, deleted_ids)
    with open_connection() as connection:
        query = f"SELECT * FROM ({relation}) LIMIT {limit} OFFSET {offset}"
        columns, rows, row_ids = fetch_rows_with_rowid(connection, query, params)
    next_offset = offset + len(rows)
    has_next = len(rows) == limit
    next_token = encode_page_token({"kind": "duckdb", "offset": next_offset}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, offset, row_ids)
    warning = merge_warnings(
        response.get("warning"),
        "This format uses OFFSET fallback. Convert TB-scale JSON arrays to JSONL or Parquet for responsive paging.",
    )
    response.update({"next_page_token": next_token, "has_next": has_next, "warning": warning})
    return response


def fetch_preview_page(
    file_name: str,
    path: Path,
    *,
    limit: int | None = DEFAULT_LIMIT,
    offset: int | None = 0,
    page_token: str | None = None,
    deleted_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Fetch one preview page using a format-specific cursor token."""
    limit_value, offset_value = normalize_pagination(limit, offset)
    reject_large_offset_without_token(offset_value, page_token)
    hidden = set(deleted_ids or [])
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return line.preview_jsonl(file_name, path, limit_value, offset_value, page_token, hidden)
    if suffix in {".csv", ".tsv"}:
        return line.preview_delimited(
            file_name,
            path,
            delimiter="\t" if suffix == ".tsv" else ",",
            limit=limit_value,
            offset=offset_value,
            page_token=page_token,
            deleted_ids=hidden,
        )
    if suffix == ".parquet":
        return parquet.preview(file_name, path, limit_value, offset_value, page_token, hidden)
    if suffix == ".json":
        return json_reader.preview(file_name, path, limit_value)
    return _duckdb_preview(file_name, path, limit_value, offset_value, page_token, deleted_ids or [])


def build_line_index_with_progress(path: Path, control: ScanControl) -> dict[str, Any]:
    """Build a sparse byte-offset index for JSONL, CSV, or TSV datasets."""
    return line.build_line_index_with_progress(path, control)


def count_rows_with_progress(path: Path, control: ScanControl, *, deleted_ids: list[int] | None = None) -> int:
    """Count rows with format-aware shortcuts and cooperative cancellation."""
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        control.check_cancelled()
        return visible_row_count(parquet.count_rows(path), deleted_ids)
    if suffix in line.LINE_DATASET_EXTENSIONS:
        result = line.build_line_index_with_progress(path, control)
        return visible_row_count(int(result["row_count"]), deleted_ids)
    hidden = deleted_ids or []
    if hidden:
        relation, params = relation_with_rowid_sql(path, hidden)
        query = f"SELECT COUNT(*) FROM ({relation})"
    else:
        relation, params = relation_sql(path)
        query = f"SELECT COUNT(*) FROM {relation}"
    with open_connection() as connection:
        control.check_cancelled()
        row = connection.execute(query, params).fetchone()
    return int(row[0]) if row else 0


def _search_duckdb(
    path: Path,
    query: str,
    limit: int,
    deleted_ids: list[int],
    control: ScanControl,
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    relation, params = relation_with_rowid_sql(path, deleted_ids)
    with open_connection() as connection:
        base_relation, base_params = relation_sql(path)
        columns_meta = describe_relation(connection, base_relation, base_params)
        text_columns = [column["name"] for column in columns_meta if "CHAR" in column["type"].upper() or "TEXT" in column["type"].upper()]
        if not text_columns:
            return [column["name"] for column in columns_meta], [], [], False, None
        clauses = " OR ".join(f"CAST({quote_ident(column)} AS VARCHAR) ILIKE ?" for column in text_columns)
        values = params + [f"%{query}%"] * len(text_columns)
        control.check_cancelled()
        result_columns, rows, row_ids = fetch_rows_with_rowid(
            connection,
            f"SELECT * FROM ({relation}) WHERE {clauses} LIMIT {limit}",
            values,
        )
    return result_columns, rows, row_ids, len(rows) == limit, None


def search_dataset(
    file_name: str,
    path: Path,
    *,
    query: str,
    limit: int | None,
    control: ScanControl,
    deleted_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Search a dataset in a background job and return first matching rows."""
    search_term = query.strip()
    if not search_term:
        raise HTTPException(status_code=400, detail="query must not be empty")
    limit_value, _ = normalize_pagination(limit, 0)
    hidden = set(deleted_ids or [])
    searchers: dict[str, Callable[[], tuple[list[str], list[list[Any]], list[int], bool, str | None]]] = {
        ".jsonl": lambda: line.search_jsonl(path, search_term, limit_value, hidden, control),
        ".csv": lambda: line.search_delimited(path, search_term, ",", limit_value, hidden, control),
        ".tsv": lambda: line.search_delimited(path, search_term, "\t", limit_value, hidden, control),
    }
    columns, rows, row_ids, truncated, warning = searchers.get(
        path.suffix.lower(),
        lambda: _search_duckdb(path, search_term, limit_value, deleted_ids or [], control),
    )()
    response = build_table_response(file_name, columns, rows, limit_value, 0, row_ids)
    response.update({"query": search_term, "truncated": truncated})
    warning = merge_warnings(response.get("warning"), warning)
    if warning:
        response["warning"] = warning
    if warning and COLUMN_LIMIT_WARNING in warning:
        mark_columns_truncated(response)
    return response

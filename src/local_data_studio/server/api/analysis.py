"""Synchronous analysis and SQL endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from ..config import DEFAULT_LIMIT, DEFAULT_SAMPLE
from ..db import (
    build_table_response,
    count_relation_rows,
    describe_relation,
    fetch_rows_with_rowid,
    normalize_pagination,
    open_connection,
    quote_ident,
    relation_sql,
    relation_with_rowid_sql,
)
from ..deleted_rows import deleted_row_ids_for
from ..files import resolve_data_file
from ..llm import generate_sql_from_prompt
from ..sql import execute_query_guarded
from ..stats import compute_column_stats
from .schemas import EdaRequest, NLQueryRequest, QueryRequest
from .services import eda_reports_service, reject_large_sync_operation

router = APIRouter()


@router.get("/api/column_stats")
async def column_stats(file: str = Query(...), sample: int | None = Query(DEFAULT_SAMPLE)) -> dict[str, Any]:
    path = resolve_data_file(file)
    reject_large_sync_operation(path, "synchronous column stats")
    return compute_column_stats(file, path, sample if sample is not None else DEFAULT_SAMPLE)


@router.post("/api/eda")
async def run_eda(payload: EdaRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    return eda_reports_service().generate_dataset_eda_report(
        file_name=payload.file,
        path=path,
        sample=payload.sample,
        mode=payload.mode,
        force=payload.force,
    )


@router.post("/api/nl_query")
async def nl_query(payload: NLQueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    prompt = payload.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="prompt must not be empty")
    relation, params = relation_sql(path)
    with open_connection() as connection:
        columns = describe_relation(connection, relation, params)
    return {"sql": generate_sql_from_prompt(prompt, columns, payload.sample)}


@router.get("/api/count")
async def count_rows(file: str = Query(...)) -> dict[str, Any]:
    path = resolve_data_file(file)
    reject_large_sync_operation(path, "synchronous row count")
    return {"file": file, "count": count_relation_rows(path)}


@router.get("/api/search")
async def search(
    file: str = Query(...),
    query: str = Query(...),
    limit: int | None = Query(DEFAULT_LIMIT),
    offset: int | None = Query(0),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    reject_large_sync_operation(path, "synchronous search")
    deleted_ids = deleted_row_ids_for(path)
    relation, params = relation_with_rowid_sql(path, deleted_ids)
    search_term = query.strip()
    if not search_term:
        raise HTTPException(status_code=400, detail="query must not be empty")
    limit_value, offset_value = normalize_pagination(limit, offset)

    with open_connection() as connection:
        base_relation, base_params = relation_sql(path)
        columns = describe_relation(connection, base_relation, base_params)
        text_columns = [column["name"] for column in columns if "CHAR" in column["type"].upper() or "TEXT" in column["type"].upper()]
        if not text_columns:
            return build_table_response(file, [column["name"] for column in columns], [], limit_value, offset_value, [])
        like_clauses = " OR ".join(f"CAST({quote_ident(column)} AS VARCHAR) ILIKE ?" for column in text_columns)
        values = params + [f"%{search_term}%"] * len(text_columns)
        search_sql = f"SELECT * FROM ({relation}) WHERE {like_clauses} LIMIT {limit_value} OFFSET {offset_value}"
        result_columns, rows, row_ids = fetch_rows_with_rowid(connection, search_sql, values)
    return build_table_response(file, result_columns, rows, limit_value, offset_value, row_ids)


@router.post("/api/query")
async def run_query(payload: QueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    return execute_query_guarded(
        file_name=payload.file,
        path=path,
        sql=payload.sql,
        limit=payload.limit,
        offset=payload.offset,
    )

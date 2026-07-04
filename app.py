import datetime
import json
import mimetypes
import re
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from server.cache import count_cache_path, search_cache_path, stats_cache_path
from server.config import (
    ALLOW_DELETE_DATA,
    BASE_DIR,
    CACHE_DIR,
    DATA_ROOT,
    DATA_SERVE_ROOT,
    DEFAULT_EDA_MODE,
    DEFAULT_EDA_SAMPLE,
    DEFAULT_LIMIT,
    DEFAULT_SAMPLE,
    DUCKDB_QUERY_MEMORY_LIMIT,
    DUCKDB_QUERY_POLL_SECONDS,
    DUCKDB_QUERY_TIMEOUT_SECONDS,
    FILE_SERVE_ROOTS,
    HARD_DELETE_MAX_BYTES,
    MAX_EDA_SAMPLE,
    SINGLE_FILE,
    SYNC_OPERATION_MAX_BYTES,
    UPLOAD_EXTENSIONS,
    VIS_EXCLUDE_DIRS,
    VIS_EXCLUDE_PATHS,
)
from server.db import (
    build_table_response,
    count_relation_rows,
    count_relation_rows_raw,
    describe_relation,
    fetch_rows,
    fetch_rows_with_rowid,
    normalize_pagination,
    open_connection,
    quote_ident,
    relation_sql,
    relation_sql_literal,
    relation_with_rowid_literal,
    relation_with_rowid_sql,
)
from server.delete_ops import delete_column_from_file, delete_row_from_file
from server.deleted_rows import (
    add_deleted_row_id,
    clear_deleted_row_ids,
    deleted_row_ids_for,
)
from server.eda import (
    build_eda_report,
    eda_cache_path,
    load_eda_dataframe_polars,
    sanitize_eda_dataframe,
)
from server.files import (
    discover_dataset_files,
    resolve_data_file,
    resolve_raw_image_file,
    unique_path,
)
from server.jobs import JOB_STORE, JobContext
from server.llm import generate_sql_from_prompt
from server.readers import (
    build_line_index_with_progress,
    count_rows_with_progress,
    fetch_preview_page,
    load_dataset_metadata,
    search_dataset,
)
from server.serialization import serialize_value
from server.stats import compute_column_stats

app = FastAPI(title="Data Viewer")
UPLOAD_FILES = File(...)
FULL_SCAN_SQL_PATTERN = re.compile(
    r"\b(order\s+by|group\s+by|join|distinct|union)\b|\b(count|sum|avg|min|max|over)\s*\(",
    re.IGNORECASE,
)


class QueryRequest(BaseModel):
    file: str
    sql: str
    limit: int | None = None
    offset: int | None = None


class EdaRequest(BaseModel):
    file: str
    sample: int | None = None
    force: bool | None = None
    mode: str | None = None  # "minimal" or "maximal"


class CountJobRequest(BaseModel):
    file: str


class IndexJobRequest(BaseModel):
    file: str


class SearchJobRequest(BaseModel):
    file: str
    query: str
    limit: int | None = None


class StatsJobRequest(BaseModel):
    file: str
    sample: int | None = None
    force: bool | None = None


class DeleteRowRequest(BaseModel):
    file: str
    row_id: int
    persist: bool | None = None


class DeleteColumnRequest(BaseModel):
    file: str
    column: str
    persist: bool | None = None


class NLQueryRequest(BaseModel):
    file: str
    prompt: str
    sample: dict[str, Any] | None = None


def _file_size(path: Path) -> int:
    return path.stat().st_size


def _is_large_dataset(path: Path) -> bool:
    return _file_size(path) > SYNC_OPERATION_MAX_BYTES


def _reject_large_sync_operation(path: Path, operation: str) -> None:
    if not _is_large_dataset(path):
        return
    raise HTTPException(
        status_code=400,
        detail=f"{operation} can scan the full dataset; use the background job endpoint for large files",
    )


def _reject_large_hard_delete(path: Path) -> None:
    if _file_size(path) <= HARD_DELETE_MAX_BYTES:
        return
    raise HTTPException(
        status_code=400,
        detail="delete from file rewrites the dataset and is disabled for large files; use hide only",
    )


def _configure_duckdb_limits(con: Any) -> None:
    con.execute(f"SET memory_limit='{DUCKDB_QUERY_MEMORY_LIMIT}'")


def _reject_high_risk_sql_for_large_file(path: Path, sql: str) -> None:
    if not _is_large_dataset(path):
        return
    if FULL_SCAN_SQL_PATTERN.search(sql):
        raise HTTPException(
            status_code=400,
            detail="this SQL can require a full scan on large datasets; use a simpler preview query or a background workflow",
        )


def _load_cached_result(cache_path: Path) -> dict[str, Any] | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_cached_result(cache_path: Path, result: dict[str, Any]) -> None:
    cache_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_warning(response: dict[str, Any], warning: str) -> dict[str, Any]:
    existing = response.get("warning")
    response["warning"] = f"{existing} {warning}" if isinstance(existing, str) and existing else warning
    return response


def _normalize_select_sql(sql: str) -> str:
    normalized = sql.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="sql must not be empty")

    if ";" in normalized:
        normalized = normalized.rstrip(";")
        if ";" in normalized:
            raise HTTPException(status_code=400, detail="multi-statement sql is not allowed")

    sql_lower = normalized.lower()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise HTTPException(status_code=400, detail="only SELECT queries are allowed")
    return normalized


def _interrupt_duckdb_connection(connection: Any | None) -> None:
    interrupt = getattr(connection, "interrupt", None)
    if callable(interrupt):
        interrupt()


def _execute_query_worker(
    *,
    path: Path,
    file_name: str,
    sql: str,
    limit_value: int,
    offset_value: int,
    deleted_ids: list[int],
    connection_holder: dict[str, Any],
) -> dict[str, Any]:
    rel_sql_literal = relation_sql_literal(path)
    with open_connection() as con:
        connection_holder["connection"] = con
        _configure_duckdb_limits(con)
        if deleted_ids:
            filtered = relation_with_rowid_literal(path, deleted_ids)
            view_sql = f"SELECT * EXCLUDE(__rowid) FROM ({filtered})"
        else:
            view_sql = f"SELECT * FROM {rel_sql_literal}"
        con.execute(f"CREATE OR REPLACE TEMP VIEW data AS {view_sql}")
        query_sql = f"SELECT * FROM ({sql}) AS q LIMIT {limit_value} OFFSET {offset_value}"
        columns, rows = fetch_rows(con, query_sql, [])
    return build_table_response(file_name, columns, rows, limit_value, offset_value, [])


def _execute_query_guarded(payload: QueryRequest, path: Path, context: JobContext | None = None) -> dict[str, Any]:
    deleted_ids = deleted_row_ids_for(path)
    sql = _normalize_select_sql(payload.sql)
    limit_value, offset_value = normalize_pagination(payload.limit, payload.offset)
    _reject_high_risk_sql_for_large_file(path, sql)

    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-query")
    started = time.monotonic()
    future = executor.submit(
        _execute_query_worker,
        path=path,
        file_name=payload.file,
        sql=sql,
        limit_value=limit_value,
        offset_value=offset_value,
        deleted_ids=deleted_ids,
        connection_holder=connection_holder,
    )
    try:
        while True:
            try:
                response = future.result(timeout=DUCKDB_QUERY_POLL_SECONDS)
                if _is_large_dataset(path):
                    _append_warning(
                        response,
                        "SQL Console used timeout, memory, and scan-risk guards for this large dataset.",
                    )
                return response
            except FutureTimeoutError:
                elapsed = time.monotonic() - started
                if context is not None:
                    context.check_cancelled()
                    context.update(message=f"Running SQL query for {int(elapsed)}s")
                if elapsed >= DUCKDB_QUERY_TIMEOUT_SECONDS:
                    _interrupt_duckdb_connection(connection_holder.get("connection"))
                    raise HTTPException(status_code=408, detail="SQL query timed out") from None
    except Exception:
        if not future.done():
            _interrupt_duckdb_connection(connection_holder.get("connection"))
            future.cancel()
        raise
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


@app.get("/api/files")
async def list_files() -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for path in discover_dataset_files(DATA_ROOT, SINGLE_FILE, VIS_EXCLUDE_PATHS):
        stat = path.stat()
        files.append(
            {
                "name": str(path.relative_to(DATA_ROOT)),
                "size": stat.st_size,
                "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        )
    return {"files": files}


@app.get("/api/config")
async def get_config() -> dict[str, Any]:
    return {
        "allow_delete_data": ALLOW_DELETE_DATA,
        "file_serve_roots": [str(p) for p in FILE_SERVE_ROOTS],
        "vis_exclude_dirs": VIS_EXCLUDE_DIRS,
    }


@app.get("/api/raw")
async def raw_file(path: str = Query(..., description="Absolute file path on the server")) -> FileResponse:
    resolved = resolve_raw_image_file(path, FILE_SERVE_ROOTS)
    media_type, _ = mimetypes.guess_type(str(resolved))
    return FileResponse(str(resolved), media_type=media_type or "application/octet-stream")


@app.post("/api/upload")
async def upload_files(files: list[UploadFile] = UPLOAD_FILES) -> dict[str, Any]:
    if SINGLE_FILE:
        raise HTTPException(status_code=400, detail="uploads are disabled")
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    skipped: list[str] = []

    for upload in files:
        filename = upload.filename or ""
        filename = Path(filename).name
        if not filename:
            continue
        ext = Path(filename).suffix.lower()
        if ext not in UPLOAD_EXTENSIONS:
            skipped.append(filename)
            await upload.close()
            continue
        dest = unique_path(DATA_ROOT, filename)
        with dest.open("wb") as out:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        await upload.close()
        saved.append(dest.name)

    if not saved:
        raise HTTPException(status_code=400, detail="unsupported file extension")

    return {"saved": saved, "skipped": skipped}


@app.get("/api/schema")
async def get_schema(file: str = Query(...)) -> dict[str, Any]:
    path = resolve_data_file(file)
    metadata = load_dataset_metadata(path)
    return metadata.to_response(file)


@app.get("/api/column_stats")
async def column_stats(
    file: str = Query(...),
    sample: int | None = Query(DEFAULT_SAMPLE),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    _reject_large_sync_operation(path, "synchronous column stats")
    sample = sample if sample is not None else DEFAULT_SAMPLE
    return compute_column_stats(file, path, sample)


def _load_cached_stats(path: Path, sample: int) -> dict[str, Any] | None:
    cache_path = stats_cache_path(path)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("sample_request") != sample:
        return None
    result = payload.get("result")
    return result if isinstance(result, dict) else None


def _write_cached_stats(path: Path, sample: int, result: dict[str, Any]) -> None:
    cache_path = stats_cache_path(path)
    payload = {"sample_request": sample, "result": result}
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _compute_cached_column_stats(file_name: str, path: Path, sample: int | None, force: bool) -> dict[str, Any]:
    sample_value = sample if sample is not None else DEFAULT_SAMPLE
    if not force:
        cached = _load_cached_stats(path, sample_value)
        if cached is not None:
            return {**cached, "cached": True}
    result = compute_column_stats(file_name, path, sample_value)
    _write_cached_stats(path, sample_value, result)
    return {**result, "cached": False}


def _generate_eda_report(file_name: str, path: Path, payload: EdaRequest, context: JobContext | None = None) -> dict[str, Any]:
    requested = payload.sample or DEFAULT_EDA_SAMPLE
    sample = max(100, min(MAX_EDA_SAMPLE, requested))
    mode = (payload.mode or DEFAULT_EDA_MODE or "minimal").strip().lower()
    minimal = mode != "maximal"
    cache_path = eda_cache_path(path, sample, mode)

    if cache_path.exists() and not payload.force:
        return {
            "file": file_name,
            "url": f"/cache/{cache_path.name}",
            "cached": True,
            "sample": sample,
            "mode": mode,
        }

    if context is not None:
        context.update(progress=0.1, message="Loading sampled rows")
    deleted_ids = deleted_row_ids_for(path)
    df = load_eda_dataframe_polars(path, sample, deleted_ids)
    if df is None:
        raise HTTPException(status_code=400, detail="failed to load dataset")

    if context is not None:
        context.check_cancelled()
        context.update(progress=0.45, message="Preparing sampled data")
    df = sanitize_eda_dataframe(df)

    try:
        if df.is_empty():
            raise HTTPException(status_code=400, detail="dataset is empty")
    except AttributeError:
        if getattr(df, "empty", False):
            raise HTTPException(status_code=400, detail="dataset is empty") from None

    try:
        if context is not None:
            context.check_cancelled()
            context.update(progress=0.7, message="Building EDA report")
        report = build_eda_report(df, title=f"EDA Report: {path.name}", minimal=minimal)
        report.to_file(str(cache_path))
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="zarque_profiling is not installed") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"EDA generation failed: {exc}") from exc

    return {
        "file": file_name,
        "url": f"/cache/{cache_path.name}",
        "cached": False,
        "sample": sample,
        "mode": mode,
    }


@app.post("/api/eda")
async def run_eda(payload: EdaRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    return _generate_eda_report(payload.file, path, payload)


@app.post("/api/jobs/count")
async def start_count_job(payload: CountJobRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    deleted_ids = deleted_row_ids_for(path)

    def work(context: JobContext) -> dict[str, Any]:
        cache_path = count_cache_path(path, deleted_ids=deleted_ids)
        cached = _load_cached_result(cache_path)
        if cached is not None and isinstance(cached.get("count"), int):
            context.update(progress=1.0, message="Count loaded from cache")
            return {**cached, "cached": True}

        total = count_rows_with_progress(path, context, deleted_ids=deleted_ids)
        result = {"file": payload.file, "count": total}
        _write_cached_result(cache_path, result)
        return {**result, "cached": False}

    return JOB_STORE.submit("count", work).to_response()


@app.post("/api/jobs/index")
async def start_index_job(payload: IndexJobRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        result = build_line_index_with_progress(path, context)
        return {"file": payload.file, **result}

    return JOB_STORE.submit("index", work).to_response()


@app.post("/api/jobs/search")
async def start_search_job(payload: SearchJobRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    deleted_ids = deleted_row_ids_for(path)
    search_term = payload.query.strip()
    limit_value, _ = normalize_pagination(payload.limit, 0)

    def work(context: JobContext) -> dict[str, Any]:
        cache_path = search_cache_path(path, query=search_term, limit=limit_value, deleted_ids=deleted_ids)
        cached = _load_cached_result(cache_path)
        if cached is not None and cached.get("query") == search_term:
            context.update(progress=1.0, message="Search loaded from cache")
            return {**cached, "cached": True}

        result = search_dataset(payload.file, path, query=search_term, limit=limit_value, control=context, deleted_ids=deleted_ids)
        _write_cached_result(cache_path, result)
        return {**result, "cached": False}

    return JOB_STORE.submit("search", work).to_response()


@app.post("/api/jobs/query")
async def start_query_job(payload: QueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        return _execute_query_guarded(payload, path, context)

    return JOB_STORE.submit("query", work).to_response()


@app.post("/api/jobs/stats")
async def start_stats_job(payload: StatsJobRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        context.update(progress=0.15, message="Sampling rows")
        result = _compute_cached_column_stats(payload.file, path, payload.sample, bool(payload.force))
        context.update(progress=1.0, message="Stats ready")
        return result

    return JOB_STORE.submit("stats", work).to_response()


@app.post("/api/jobs/eda")
async def start_eda_job(payload: EdaRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        return _generate_eda_report(payload.file, path, payload, context)

    return JOB_STORE.submit("eda", work).to_response()


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    record = JOB_STORE.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    return record.to_response()


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str) -> dict[str, Any]:
    record = JOB_STORE.cancel(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    return record.to_response()


@app.post("/api/nl_query")
async def nl_query(payload: NLQueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    prompt = payload.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="prompt must not be empty")

    rel_sql, params = relation_sql(path)
    with open_connection() as con:
        columns = describe_relation(con, rel_sql, params)

    sql = generate_sql_from_prompt(prompt, columns, payload.sample)
    return {"sql": sql}


@app.get("/api/preview")
async def preview(
    file: str = Query(...),
    limit: int | None = Query(DEFAULT_LIMIT),
    offset: int | None = Query(0),
    page_token: str | None = Query(None),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    deleted_ids = deleted_row_ids_for(path)
    return fetch_preview_page(file, path, limit=limit, offset=offset, page_token=page_token, deleted_ids=deleted_ids)


@app.get("/api/count")
async def count_rows(file: str = Query(...)) -> dict[str, Any]:
    path = resolve_data_file(file)
    _reject_large_sync_operation(path, "synchronous row count")
    total = count_relation_rows(path)
    return {"file": file, "count": total}


@app.post("/api/delete_row")
async def delete_row(payload: DeleteRowRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    try:
        row_id = int(payload.row_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="row_id must be an integer") from None
    if row_id < 1:
        raise HTTPException(status_code=400, detail="row_id must be positive")
    persist = bool(payload.persist)
    if persist:
        if not ALLOW_DELETE_DATA:
            raise HTTPException(status_code=403, detail="delete from file is disabled")
        _reject_large_hard_delete(path)
        total_rows = count_relation_rows_raw(path)
        if row_id > total_rows:
            raise HTTPException(status_code=404, detail="row not found")
        delete_row_from_file(path, row_id)
        clear_deleted_row_ids(path)
        return {
            "file": payload.file,
            "row_id": row_id,
            "persisted": True,
            "total_rows": max(0, total_rows - 1),
        }
    add_deleted_row_id(path, row_id)
    return {
        "file": payload.file,
        "row_id": row_id,
        "persisted": False,
        "deleted_count": len(deleted_row_ids_for(path)),
    }


@app.post("/api/delete_column")
async def delete_column(payload: DeleteColumnRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    column = (payload.column or "").strip()
    if not column:
        raise HTTPException(status_code=400, detail="column is required")
    persist = bool(payload.persist)
    if persist:
        if not ALLOW_DELETE_DATA:
            raise HTTPException(status_code=403, detail="delete from file is disabled")
        _reject_large_hard_delete(path)
        delete_column_from_file(path, column)
        return {
            "file": payload.file,
            "column": column,
            "persisted": True,
        }
    return {"file": payload.file, "column": column, "persisted": False}


@app.get("/api/search")
async def search(
    file: str = Query(...),
    query: str = Query(...),
    limit: int | None = Query(DEFAULT_LIMIT),
    offset: int | None = Query(0),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    _reject_large_sync_operation(path, "synchronous search")
    deleted_ids = deleted_row_ids_for(path)
    rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
    search_term = query.strip()
    if not search_term:
        raise HTTPException(status_code=400, detail="query must not be empty")

    limit_value, offset_value = normalize_pagination(limit, offset)

    with open_connection() as con:
        base_rel_sql, base_params = relation_sql(path)
        columns = describe_relation(con, base_rel_sql, base_params)
        text_columns = [col["name"] for col in columns if "CHAR" in col["type"].upper() or "TEXT" in col["type"].upper()]
        if not text_columns:
            return {
                **build_table_response(
                    file,
                    [col["name"] for col in columns],
                    [],
                    limit_value,
                    offset_value,
                    [],
                ),
            }

        like_clauses = " OR ".join([f"CAST({quote_ident(col)} AS VARCHAR) ILIKE ?" for col in text_columns])
        values = params + [f"%{search_term}%"] * len(text_columns)
        query = f"SELECT * FROM ({rel_sql}) WHERE {like_clauses} LIMIT {limit_value} OFFSET {offset_value}"
        result_columns, rows, row_ids = fetch_rows_with_rowid(con, query, values)

    return build_table_response(file, result_columns, rows, limit_value, offset_value, row_ids)


@app.get("/api/column_sample")
async def column_sample(
    file: str = Query(...),
    column: str = Query(...),
    limit: int | None = Query(20),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    _reject_large_sync_operation(path, "synchronous column sample")
    rel_sql, params = relation_sql(path)
    limit_value = max(1, min(100, limit or 20))

    with open_connection() as con:
        sample = con.execute(
            f"SELECT {quote_ident(column)} FROM {rel_sql} WHERE {quote_ident(column)} IS NOT NULL LIMIT {limit_value}",
            params,
        )
        rows = [serialize_value(row[0]) for row in sample.fetchall()]
    return {"file": file, "column": column, "values": rows}


@app.post("/api/query")
async def run_query(payload: QueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    return _execute_query_guarded(payload, path)


app.mount("/data", StaticFiles(directory=str(DATA_SERVE_ROOT), check_dir=False), name="data")
app.mount("/cache", StaticFiles(directory=str(CACHE_DIR), check_dir=False), name="cache")
app.mount("/", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="static")

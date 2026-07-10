import datetime
import json
import mimetypes
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .server.atlas import discover_embedder_models, run_atlas_visualization
from .server.cache import count_cache_path, search_cache_path, stats_cache_path
from .server.config import (
    ALLOW_DELETE_DATA,
    CACHE_DIR,
    DATA_ROOT,
    DATA_SERVE_ROOT,
    DEFAULT_LIMIT,
    DEFAULT_SAMPLE,
    FILE_SERVE_ROOTS,
    HARD_DELETE_MAX_BYTES,
    PACKAGE_DIR,
    SINGLE_FILE,
    UPLOAD_EXTENSIONS,
    VIS_EXCLUDE_DIRS,
    VIS_EXCLUDE_PATHS,
)
from .server.db import (
    build_table_response,
    count_relation_rows,
    count_relation_rows_raw,
    describe_relation,
    fetch_rows_with_rowid,
    normalize_pagination,
    open_connection,
    quote_ident,
    relation_sql,
    relation_with_rowid_sql,
)
from .server.delete_ops import delete_column_from_file, delete_row_from_file
from .server.deleted_rows import (
    add_deleted_row_id,
    clear_deleted_row_ids,
    deleted_row_ids_for,
)
from .server.eda_reports import generate_dataset_eda_report, generate_query_eda_report
from .server.files import (
    discover_dataset_files,
    resolve_data_file,
    resolve_raw_image_file,
    unique_path,
)
from .server.jobs import JOB_STORE, JobContext
from .server.llm import generate_sql_from_prompt
from .server.readers import (
    build_line_index_with_progress,
    count_rows_with_progress,
    fetch_preview_page,
    load_dataset_metadata,
    search_dataset,
)
from .server.serialization import serialize_value
from .server.sql import execute_query_guarded, is_large_dataset
from .server.stats import compute_column_stats

app = FastAPI(title="Data Viewer")
UPLOAD_FILES = File(...)


class NoCacheStaticFiles(StaticFiles):
    def file_response(self, full_path, stat_result, scope, status_code=200):
        response = super().file_response(full_path, stat_result, scope, status_code)
        response.headers["Cache-Control"] = "no-store, max-age=0"
        return response


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


class EdaQueryRequest(EdaRequest):
    sql: str


class AtlasRequest(BaseModel):
    file: str
    column: str
    model: str
    sample: int | None = None


class AtlasQueryRequest(AtlasRequest):
    sql: str


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


def _reject_large_sync_operation(path: Path, operation: str) -> None:
    if not is_large_dataset(path):
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


@app.get("/api/embedder_models")
async def embedder_models() -> dict[str, Any]:
    return {"models": discover_embedder_models()}


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


@app.post("/api/eda")
async def run_eda(payload: EdaRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    return generate_dataset_eda_report(
        file_name=payload.file,
        path=path,
        sample=payload.sample,
        mode=payload.mode,
        force=payload.force,
    )


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
        return execute_query_guarded(
            file_name=payload.file,
            path=path,
            sql=payload.sql,
            limit=payload.limit,
            offset=payload.offset,
            context=context,
        )

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
        return generate_dataset_eda_report(
            file_name=payload.file,
            path=path,
            sample=payload.sample,
            mode=payload.mode,
            force=payload.force,
            context=context,
        )

    return JOB_STORE.submit("eda", work).to_response()


@app.post("/api/jobs/eda_query")
async def start_eda_query_job(payload: EdaQueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        return generate_query_eda_report(
            file_name=payload.file,
            path=path,
            sql=payload.sql,
            sample=payload.sample,
            mode=payload.mode,
            force=payload.force,
            context=context,
        )

    return JOB_STORE.submit("eda_query", work).to_response()


@app.post("/api/jobs/atlas")
async def start_atlas_job(payload: AtlasRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        return run_atlas_visualization(
            file_name=payload.file,
            path=path,
            column=payload.column,
            model=payload.model,
            sql=None,
            sample=payload.sample,
            context=context,
        )

    return JOB_STORE.submit("atlas", work).to_response()


@app.post("/api/jobs/atlas_query")
async def start_atlas_query_job(payload: AtlasQueryRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)

    def work(context: JobContext) -> dict[str, Any]:
        return run_atlas_visualization(
            file_name=payload.file,
            path=path,
            column=payload.column,
            model=payload.model,
            sql=payload.sql,
            sample=payload.sample,
            context=context,
        )

    return JOB_STORE.submit("atlas_query", work).to_response()


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
    return execute_query_guarded(
        file_name=payload.file,
        path=path,
        sql=payload.sql,
        limit=payload.limit,
        offset=payload.offset,
    )


app.mount("/data", StaticFiles(directory=str(DATA_SERVE_ROOT), check_dir=False), name="data")
app.mount("/cache", StaticFiles(directory=str(CACHE_DIR), check_dir=False), name="cache")
app.mount("/", NoCacheStaticFiles(directory=str(PACKAGE_DIR / "static"), html=True), name="static")

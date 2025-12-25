import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from server.config import (
    ALLOW_DELETE_DATA,
    ALLOWED_EXTENSIONS,
    BASE_DIR,
    CACHE_DIR,
    DATA_ROOT,
    DATA_SERVE_ROOT,
    DEFAULT_EDA_MODE,
    DEFAULT_EDA_SAMPLE,
    DEFAULT_LIMIT,
    DEFAULT_SAMPLE,
    MAX_EDA_SAMPLE,
    SINGLE_FILE,
    UPLOAD_EXTENSIONS,
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
from server.files import resolve_data_file, unique_path
from server.llm import generate_sql_from_prompt
from server.serialization import serialize_value
from server.stats import compute_column_stats

app = FastAPI(title="Data Viewer")
UPLOAD_FILES = File(...)


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


@app.get("/api/files")
async def list_files() -> dict[str, Any]:
    if not DATA_ROOT.exists():
        return {"files": []}

    files: list[dict[str, Any]] = []
    paths = [SINGLE_FILE] if SINGLE_FILE else sorted(DATA_ROOT.rglob("*"))
    for path in paths:
        if path is None or not path.is_file():
            continue
        if path.suffix.lower() not in ALLOWED_EXTENSIONS:
            continue
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
    return {"allow_delete_data": ALLOW_DELETE_DATA}


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
    rel_sql, params = relation_sql(path)
    with open_connection() as con:
        columns = describe_relation(con, rel_sql, params)
    return {"file": file, "columns": columns}


@app.get("/api/column_stats")
async def column_stats(
    file: str = Query(...),
    sample: int | None = Query(DEFAULT_SAMPLE),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    sample = sample if sample is not None else DEFAULT_SAMPLE
    return compute_column_stats(file, path, sample)


@app.post("/api/eda")
async def run_eda(payload: EdaRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    requested = payload.sample or DEFAULT_EDA_SAMPLE
    requested = max(1, min(MAX_EDA_SAMPLE, requested))
    total_rows = count_relation_rows(path)
    if total_rows <= 0:
        raise HTTPException(status_code=400, detail="dataset is empty")
    if requested >= total_rows:
        sample = total_rows
    else:
        sample = max(100, requested)
        sample = min(sample, total_rows)

    mode = (payload.mode or DEFAULT_EDA_MODE or "minimal").strip().lower()
    minimal = mode != "maximal"

    cache_path = eda_cache_path(path, sample, mode)

    if cache_path.exists() and not payload.force:
        return {
            "file": payload.file,
            "url": f"/cache/{cache_path.name}",
            "cached": True,
            "sample": sample,
            "mode": mode,
        }

    deleted_ids = deleted_row_ids_for(path)
    df = load_eda_dataframe_polars(path, sample, deleted_ids)
    if df is None:
        raise HTTPException(status_code=400, detail="failed to load dataset")

    df = sanitize_eda_dataframe(df)

    try:
        if df.is_empty():
            raise HTTPException(status_code=400, detail="dataset is empty")
    except AttributeError:
        if getattr(df, "empty", False):
            raise HTTPException(status_code=400, detail="dataset is empty") from None

    try:
        report = build_eda_report(df, title=f"EDA Report: {path.name}", minimal=minimal)
        report.to_file(str(cache_path))
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="zarque_profiling is not installed") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"EDA generation failed: {exc}") from exc

    return {
        "file": payload.file,
        "url": f"/cache/{cache_path.name}",
        "cached": False,
        "sample": sample,
        "mode": mode,
    }


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
) -> dict[str, Any]:
    path = resolve_data_file(file)
    deleted_ids = deleted_row_ids_for(path)
    rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
    limit_value, offset_value = normalize_pagination(limit, offset)

    with open_connection() as con:
        query = f"SELECT * FROM ({rel_sql}) LIMIT {limit_value} OFFSET {offset_value}"
        columns, rows, row_ids = fetch_rows_with_rowid(con, query, params)

    return build_table_response(file, columns, rows, limit_value, offset_value, row_ids)


@app.get("/api/count")
async def count_rows(file: str = Query(...)) -> dict[str, Any]:
    path = resolve_data_file(file)
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
    deleted_ids = deleted_row_ids_for(path)
    rel_sql_literal = relation_sql_literal(path)

    sql = payload.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="sql must not be empty")

    if ";" in sql:
        sql = sql.rstrip(";")
        if ";" in sql:
            raise HTTPException(status_code=400, detail="multi-statement sql is not allowed")

    sql_lower = sql.lower()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise HTTPException(status_code=400, detail="only SELECT queries are allowed")

    limit_value, offset_value = normalize_pagination(payload.limit, payload.offset)

    with open_connection() as con:
        if deleted_ids:
            filtered = relation_with_rowid_literal(path, deleted_ids)
            view_sql = f"SELECT * EXCLUDE(__rowid) FROM ({filtered})"
        else:
            view_sql = f"SELECT * FROM {rel_sql_literal}"
        con.execute(f"CREATE OR REPLACE TEMP VIEW data AS {view_sql}")
        query_sql = f"SELECT * FROM ({sql}) AS q LIMIT {limit_value} OFFSET {offset_value}"
        columns, rows = fetch_rows(con, query_sql, [])

    return build_table_response(payload.file, columns, rows, limit_value, offset_value, [])


app.mount("/data", StaticFiles(directory=str(DATA_SERVE_ROOT), check_dir=False), name="data")
app.mount("/cache", StaticFiles(directory=str(CACHE_DIR), check_dir=False), name="cache")
app.mount("/", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="static")

"""Dataset discovery, upload, preview, and sample endpoints."""

import datetime
import mimetypes
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from ..config import (
    ALLOW_DELETE_DATA,
    DATA_ROOT,
    DEFAULT_LIMIT,
    EMBEDDER_MODELS_DIR,
    FILE_SERVE_ROOTS,
    SINGLE_FILE,
    UPLOAD_EXTENSIONS,
    VIS_EXCLUDE_DIRS,
)
from ..db import open_connection, quote_ident, relation_sql
from ..deleted_rows import deleted_row_ids_for
from ..embedder_models import discover_embedder_models
from ..files import refresh_dataset_file_catalog, resolve_data_file, resolve_raw_image_file, unique_path
from ..readers import fetch_preview_page, fetch_raw_row, load_dataset_metadata
from ..serialization import serialize_raw_value, serialize_value
from ..sql import fetch_raw_query_row_guarded
from .schemas import RawRowRequest
from .services import reject_large_sync_operation

router = APIRouter()
UPLOAD_FILES = File(...)


@router.get("/api/files")
async def list_files() -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for name, path in refresh_dataset_file_catalog().items():
        stat = path.stat()
        files.append(
            {
                "name": name,
                "size": stat.st_size,
                "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        )
    return {"files": files}


@router.get("/api/config")
async def get_config() -> dict[str, Any]:
    return {
        "allow_delete_data": ALLOW_DELETE_DATA,
        "file_serve_roots": [str(path) for path in FILE_SERVE_ROOTS],
        "vis_exclude_dirs": VIS_EXCLUDE_DIRS,
    }


@router.get("/api/embedder_models")
async def embedder_models() -> dict[str, Any]:
    return {"models": discover_embedder_models(EMBEDDER_MODELS_DIR)}


@router.get("/api/raw")
async def raw_file(path: str = Query(..., description="Absolute file path on the server")) -> FileResponse:
    resolved = resolve_raw_image_file(path, FILE_SERVE_ROOTS)
    media_type, _ = mimetypes.guess_type(str(resolved))
    return FileResponse(str(resolved), media_type=media_type or "application/octet-stream")


@router.post("/api/upload")
async def upload_files(files: list[UploadFile] = UPLOAD_FILES) -> dict[str, Any]:
    if SINGLE_FILE:
        raise HTTPException(status_code=400, detail="uploads are disabled")
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    skipped: list[str] = []

    for upload in files:
        filename = Path(upload.filename or "").name
        if not filename:
            continue
        if Path(filename).suffix.lower() not in UPLOAD_EXTENSIONS:
            skipped.append(filename)
            await upload.close()
            continue
        destination = unique_path(DATA_ROOT, filename)
        with destination.open("wb") as output:
            while chunk := await upload.read(1024 * 1024):
                output.write(chunk)
        await upload.close()
        saved.append(destination.name)

    if not saved:
        raise HTTPException(status_code=400, detail="unsupported file extension")
    refresh_dataset_file_catalog()
    return {"saved": saved, "skipped": skipped}


@router.get("/api/schema")
async def get_schema(file: str = Query(...)) -> dict[str, Any]:
    metadata = load_dataset_metadata(resolve_data_file(file))
    return metadata.to_response(file)


@router.get("/api/preview")
async def preview(
    file: str = Query(...),
    limit: int | None = Query(DEFAULT_LIMIT),
    offset: int | None = Query(0),
    page_token: str | None = Query(None),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    return fetch_preview_page(file, path, limit=limit, offset=offset, page_token=page_token, deleted_ids=deleted_row_ids_for(path))


@router.post("/api/raw_row")
async def raw_row(payload: RawRowRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    if payload.row_id is not None:
        columns, values = fetch_raw_row(path, payload.row_id)
        row_id = payload.row_id
    elif payload.sql:
        columns, values = fetch_raw_query_row_guarded(path=path, sql=payload.sql, offset=max(0, payload.offset or 0))
        row_id = None
    else:
        raise HTTPException(status_code=400, detail="row_id or sql is required")
    return {
        "file": payload.file,
        "row_id": row_id,
        "columns": columns,
        "row": [serialize_raw_value(value) for value in values],
    }


@router.get("/api/column_sample")
async def column_sample(
    file: str = Query(...),
    column: str = Query(...),
    limit: int | None = Query(20),
) -> dict[str, Any]:
    path = resolve_data_file(file)
    reject_large_sync_operation(path, "synchronous column sample")
    relation, params = relation_sql(path)
    limit_value = max(1, min(100, limit or 20))
    quoted_column = quote_ident(column)
    with open_connection() as connection:
        sample = connection.execute(
            f"SELECT {quoted_column} FROM {relation} WHERE {quoted_column} IS NOT NULL LIMIT {limit_value}",
            params,
        )
        rows = [serialize_value(row[0]) for row in sample.fetchall()]
    return {"file": file, "column": column, "values": rows}

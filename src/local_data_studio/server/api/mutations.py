"""Dataset row and column mutation endpoints."""

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from ..config import ALLOW_DELETE_DATA, HARD_DELETE_MAX_BYTES
from ..db import count_relation_rows_raw
from ..delete_ops import delete_column_from_file, delete_row_from_file
from ..deleted_rows import add_deleted_row_id, clear_deleted_row_ids, deleted_row_ids_for
from ..files import resolve_data_file
from .schemas import DeleteColumnRequest, DeleteRowRequest

router = APIRouter()


def _reject_large_hard_delete(path: Path) -> None:
    if path.stat().st_size > HARD_DELETE_MAX_BYTES:
        raise HTTPException(
            status_code=400,
            detail="delete from file rewrites the dataset and is disabled for large files; use hide only",
        )


@router.post("/api/delete_row")
def delete_row(payload: DeleteRowRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    row_id = int(payload.row_id)
    if row_id < 1:
        raise HTTPException(status_code=400, detail="row_id must be positive")
    if payload.persist:
        if not ALLOW_DELETE_DATA:
            raise HTTPException(status_code=403, detail="delete from file is disabled")
        _reject_large_hard_delete(path)
        total_rows = count_relation_rows_raw(path)
        if row_id > total_rows:
            raise HTTPException(status_code=404, detail="row not found")
        delete_row_from_file(path, row_id)
        clear_deleted_row_ids(path)
        return {"file": payload.file, "row_id": row_id, "persisted": True, "total_rows": max(0, total_rows - 1)}
    add_deleted_row_id(path, row_id)
    return {"file": payload.file, "row_id": row_id, "persisted": False, "deleted_count": len(deleted_row_ids_for(path))}


@router.post("/api/delete_column")
def delete_column(payload: DeleteColumnRequest) -> dict[str, Any]:
    path = resolve_data_file(payload.file)
    column = payload.column.strip()
    if not column:
        raise HTTPException(status_code=400, detail="column is required")
    if payload.persist:
        if not ALLOW_DELETE_DATA:
            raise HTTPException(status_code=403, detail="delete from file is disabled")
        _reject_large_hard_delete(path)
        delete_column_from_file(path, column)
        return {"file": payload.file, "column": column, "persisted": True}
    return {"file": payload.file, "column": column, "persisted": False}

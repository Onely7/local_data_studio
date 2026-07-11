"""Bounded support for non-streaming JSON datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..config import COLUMN_LIMIT_WARNING, MAX_JSON_PREVIEW_BYTES
from ..db import build_table_response
from .common import (
    JSON_NOT_TB_SAFE_WARNING,
    align_existing_rows,
    extend_columns_from_value,
    load_or_create_metadata,
    mark_columns_truncated,
    merge_warnings,
    raw_row_values,
    row_from_mapping,
)
from .contracts import DatasetMetadata


def _create_metadata(path: Path) -> DatasetMetadata:
    return DatasetMetadata(
        file_format="json",
        columns=[{"name": "value", "type": "JSON"}],
        warning=JSON_NOT_TB_SAFE_WARNING,
    )


def load_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    return load_or_create_metadata(path, _create_metadata, use_cache=use_cache)


def preview(file_name: str, path: Path, limit: int) -> dict[str, Any]:
    if path.stat().st_size > MAX_JSON_PREVIEW_BYTES:
        response = build_table_response(file_name, ["value"], [], limit, 0, [])
        response.update({"next_page_token": None, "has_next": False, "warning": JSON_NOT_TB_SAFE_WARNING})
        return response
    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc

    values = payload[:limit] if isinstance(payload, list) else [payload]
    columns: list[str] = []
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    columns_truncated = False
    for index, value in enumerate(values, start=1):
        old_column_count = len(columns)
        columns, row_columns_truncated = extend_columns_from_value(columns, value)
        columns_truncated = columns_truncated or row_columns_truncated
        align_existing_rows(rows, old_column_count, len(columns))
        rows.append(row_from_mapping(columns, value))
        row_ids.append(index)
    response = build_table_response(file_name, columns or ["value"], rows, limit, 0, row_ids)
    warning = merge_warnings(JSON_NOT_TB_SAFE_WARNING, COLUMN_LIMIT_WARNING if columns_truncated else None)
    if columns_truncated:
        mark_columns_truncated(response)
    response.update({"next_page_token": None, "has_next": isinstance(payload, list) and len(payload) > limit, "warning": warning})
    return response


def raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    if path.stat().st_size > MAX_JSON_PREVIEW_BYTES:
        raise HTTPException(status_code=400, detail=JSON_NOT_TB_SAFE_WARNING)
    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc
    if isinstance(payload, list):
        if row_id > len(payload):
            raise HTTPException(status_code=404, detail="row not found")
        return raw_row_values(payload[row_id - 1])
    if row_id != 1:
        raise HTTPException(status_code=404, detail="row not found")
    return raw_row_values(payload)

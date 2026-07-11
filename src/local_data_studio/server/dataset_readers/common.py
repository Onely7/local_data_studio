"""Shared token, metadata, and response helpers for dataset readers."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..cache import metadata_cache_path
from ..config import COLUMN_LIMIT_WARNING, MAX_COLUMNS, MAX_OFFSET_FALLBACK
from ..serialization import serialize_value
from .contracts import DatasetMetadata

TB_SAFE_EXTENSIONS = {".jsonl", ".csv", ".tsv", ".parquet"}
JSON_NOT_TB_SAFE_WARNING = "JSON array/object files are not TB-friendly. Convert large datasets to JSONL or Parquet for bounded preview."
MetadataLoader = Callable[[Path], DatasetMetadata]


def merge_warnings(*warnings: str | None) -> str | None:
    """Join distinct non-empty warnings while preserving their first-seen order."""
    active = [warning for warning in warnings if warning]
    return " ".join(dict.fromkeys(active)) if active else None


def limit_metadata_columns(metadata: DatasetMetadata) -> DatasetMetadata:
    """Return metadata capped to the API column limit with a warning when truncated."""
    if len(metadata.columns) <= MAX_COLUMNS:
        return metadata
    return DatasetMetadata(
        file_format=metadata.file_format,
        columns=metadata.columns[:MAX_COLUMNS],
        warning=merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING),
    )


def mark_columns_truncated(response: dict[str, Any], total_columns: int | None = None) -> None:
    """Mark a response mapping in place when its column list was truncated."""
    response["columns_truncated"] = True
    if total_columns is not None:
        response["total_columns"] = total_columns


def encode_page_token(payload: dict[str, Any] | None) -> str | None:
    """Encode cursor state as an opaque URL-safe token, or preserve ``None``."""
    if not payload:
        return None
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_page_token(token: str | None) -> dict[str, Any]:
    """Decode an opaque cursor token into a new mapping.

    Raises:
        HTTPException: The token is malformed or does not contain an object.
    """
    if not token:
        return {}
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii"))
        payload = json.loads(decoded.decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="invalid page token") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid page token")
    return payload


def decode_page_token_for(token: str | None, expected_kind: str) -> dict[str, Any]:
    """Decode cursor state and reject tokens created for another reader format."""
    payload = decode_page_token(token)
    if payload.get("kind") not in (None, expected_kind):
        raise HTTPException(status_code=400, detail="invalid page token")
    return payload


def token_int(payload: dict[str, Any], key: str, default: int, *, minimum: int = 0) -> int:
    """Read a bounded integer cursor field, falling back to ``default``."""
    try:
        value = int(payload.get(key, default))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="invalid page token") from exc
    if value < minimum:
        raise HTTPException(status_code=400, detail="invalid page token")
    return value


def reject_large_offset_without_token(offset: int, page_token: str | None) -> None:
    """Reject deep OFFSET pagination unless the request supplies a cursor token."""
    if not page_token and offset > MAX_OFFSET_FALLBACK:
        raise HTTPException(status_code=400, detail="offset pagination is limited for large datasets; use page_token navigation")


def format_name(path: Path) -> str:
    """Return the normalized dataset format name for a supported suffix."""
    names = {".jsonl": "jsonl", ".tsv": "tsv", ".csv": "csv", ".parquet": "parquet", ".json": "json"}
    return names.get(path.suffix.lower(), path.suffix.lower().lstrip(".") or "unknown")


def infer_type_name(value: Any) -> str:
    """Infer a stable lightweight schema label from one sampled value."""
    if value is None:
        return "UNKNOWN"
    type_names = (
        (bool, "BOOLEAN"),
        (int, "BIGINT"),
        (float, "DOUBLE"),
        (list, "LIST"),
        (dict, "STRUCT"),
    )
    for value_type, type_name in type_names:
        if isinstance(value, value_type):
            return type_name
    return "VARCHAR"


def merge_column_type(current: str | None, value: Any) -> str:
    """Merge one sampled value into an existing lightweight type label."""
    inferred = infer_type_name(value)
    if current is None or current == "UNKNOWN":
        return inferred
    return current if inferred in {"UNKNOWN", current} else "VARCHAR"


def load_cached_metadata(path: Path) -> DatasetMetadata | None:
    """Return fingerprinted metadata, treating invalid cache data as a miss."""
    cache_path = metadata_cache_path(path)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    columns = payload.get("columns")
    if not isinstance(columns, list):
        return None
    metadata = DatasetMetadata(
        file_format=str(payload.get("format") or format_name(path)),
        columns=[{"name": str(item["name"]), "type": str(item["type"])} for item in columns if isinstance(item, dict) and "name" in item and "type" in item],
        warning=payload.get("warning") if isinstance(payload.get("warning"), str) else None,
    )
    return limit_metadata_columns(metadata)


def write_cached_metadata(path: Path, metadata: DatasetMetadata) -> None:
    """Replace the fingerprinted metadata cache without mutating ``metadata``."""
    payload = {"format": metadata.file_format, "columns": metadata.columns, "warning": metadata.warning}
    metadata_cache_path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_or_create_metadata(path: Path, loader: MetadataLoader, *, use_cache: bool = True) -> DatasetMetadata:
    """Return cached metadata or create, limit, and persist a fresh value."""
    if use_cache:
        cached = load_cached_metadata(path)
        if cached is not None:
            return cached
    metadata = limit_metadata_columns(loader(path))
    write_cached_metadata(path, metadata)
    return metadata


def row_from_mapping(columns: list[str], value: Any) -> list[Any]:
    """Align an object or scalar value to the supplied column order."""
    if isinstance(value, dict):
        return [serialize_value(value.get(column)) for column in columns]
    return [serialize_value(value)]


def extend_columns_from_value(columns: list[str], value: Any) -> tuple[list[str], bool]:
    """Return a new column list extended by unseen object keys.

    The input list is not modified. The boolean reports whether the API limit
    prevented one or more keys from being included.
    """
    if not isinstance(value, dict):
        return columns or ["value"], False
    next_columns = list(columns)
    existing = set(next_columns)
    truncated = False
    for key in value:
        name = str(key)
        if name in existing:
            continue
        if len(next_columns) >= MAX_COLUMNS:
            truncated = True
            continue
        next_columns.append(name)
        existing.add(name)
    return next_columns, truncated


def align_existing_rows(rows: list[list[Any]], old_column_count: int, new_column_count: int) -> None:
    """Pad previously collected rows in place after sampled columns expand."""
    if new_column_count <= old_column_count:
        return
    for row in rows:
        row.extend([None] * (new_column_count - old_column_count))


def raw_row_values(value: Any) -> tuple[list[str], list[Any]]:
    """Return untruncated columns and values for one decoded record."""
    if isinstance(value, dict):
        columns = [str(key) for key in value]
        return columns, [value[key] for key in value]
    return ["value"], [value]


def visible_row_count(total_rows: int, deleted_ids: list[int] | None) -> int:
    """Subtract valid one-based soft-deleted IDs from a physical row count."""
    deleted = {row_id for row_id in deleted_ids or [] if 1 <= row_id <= total_rows}
    return max(0, total_rows - len(deleted))

"""Atlas display-frame sanitization and projection coordinate attachment."""

from __future__ import annotations

import json
from typing import Any

from ..config import ATLAS_TEXT_MAX_CHARS
from .contracts import (
    ATLAS_EMBED_INPUT_COLUMN,
    ATLAS_PROJECTION_NEIGHBORS,
    ATLAS_PROJECTION_X,
    ATLAS_PROJECTION_Y,
    AtlasProjectionCoordinates,
)
from .image_values import (
    ATLAS_TRUNCATION_SUFFIX,
    decode_data_image,
    decode_image_bytes_string,
    is_image_like_value,
)


def _truncate_atlas_text(value: str) -> str:
    if not ATLAS_TEXT_MAX_CHARS or len(value) <= ATLAS_TEXT_MAX_CHARS:
        return value
    return f"{value[:ATLAS_TEXT_MAX_CHARS]}{ATLAS_TRUNCATION_SUFFIX}"


def _json_default_for_atlas(value: Any) -> str:
    return f"<binary {len(value)} bytes>" if isinstance(value, bytes | bytearray) else str(value)


def sanitize_atlas_cell(value: Any) -> Any:
    """Convert non-image values to Parquet-safe bounded display values."""
    if value is None:
        return None
    if isinstance(value, str):
        return _truncate_atlas_text(value)
    if isinstance(value, bytes | bytearray):
        return f"<binary {len(value)} bytes>"
    if isinstance(value, dict | list | tuple | set):
        try:
            text = json.dumps(value, ensure_ascii=False, default=_json_default_for_atlas)
        except (TypeError, ValueError):
            text = str(value)
        return _truncate_atlas_text(text)
    return value


def _normalize_image_display_bytes(value: Any) -> Any:
    if isinstance(value, bytes | bytearray):
        return bytes(value)
    if isinstance(value, str):
        decoded = decode_data_image(value) or decode_image_bytes_string(value)
        return decoded if decoded is not None else value
    return value


def normalize_image_display_value(value: Any, *, key: str | None = None) -> Any:
    """Preserve image objects while normalizing nested Python bytes to binary."""
    if key == "bytes":
        return _normalize_image_display_bytes(value)
    if isinstance(value, bytes | bytearray):
        return bytes(value)
    if isinstance(value, dict):
        return {str(item_key): normalize_image_display_value(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list | tuple):
        return [normalize_image_display_value(item) for item in value]
    return value


def normalize_image_display_columns(data_frame: Any, columns: set[str]) -> Any:
    """Return a copy with selected image columns normalized for Parquet storage."""
    if not columns or not hasattr(data_frame, "copy") or not hasattr(data_frame, "columns"):
        return data_frame
    normalized = data_frame.copy()
    for column in normalized.columns:
        if str(column) in columns:
            normalized[column] = normalized[column].map(normalize_image_display_value)
    return normalized


def drop_atlas_embed_input(data_frame: Any) -> Any:
    """Return a frame without the hidden encoder-only input column."""
    if hasattr(data_frame, "drop") and ATLAS_EMBED_INPUT_COLUMN in data_frame.columns:
        return data_frame.drop(columns=[ATLAS_EMBED_INPUT_COLUMN])
    return data_frame


def sanitize_atlas_output_frame(data_frame: Any, *, preserve_columns: set[str] | None = None) -> Any:
    """Return a Parquet-safe display copy while preserving image object columns."""
    output = drop_atlas_embed_input(data_frame)
    if not hasattr(output, "copy") or not hasattr(output, "columns"):
        return output
    sanitized = output.copy()
    projection_columns = {ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, ATLAS_PROJECTION_NEIGHBORS}
    preserved = preserve_columns or set()
    for column in sanitized.columns:
        if column not in projection_columns and str(column) not in preserved:
            sanitized[column] = sanitized[column].map(sanitize_atlas_cell)
    return sanitized


def image_like_columns(data_frame: Any, *, sample_size: int = 50) -> set[str]:
    """Identify image-like columns from at most ``sample_size`` non-null values."""
    if not hasattr(data_frame, "columns"):
        return set()
    columns: set[str] = set()
    for column in data_frame.columns:
        try:
            values = data_frame[column].dropna().head(sample_size).tolist()
        except Exception:
            continue
        if any(is_image_like_value(value) for value in values):
            columns.add(str(column))
    return columns


def attach_projection_columns(base_frame: Any, projected_frame: Any) -> Any:
    """Return a display copy with two float32 projection coordinate columns."""
    output = base_frame.copy()
    if isinstance(projected_frame, AtlasProjectionCoordinates):
        output[ATLAS_PROJECTION_X] = projected_frame.values[:, 0]
        output[ATLAS_PROJECTION_Y] = projected_frame.values[:, 1]
        return output
    for column in (ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y):
        if column in projected_frame.columns:
            output[column] = projected_frame[column].to_list()
    return output


def build_atlas_output_frame(base_frame: Any, coordinates: Any, preserve_columns: set[str]) -> Any:
    """Build the final Atlas frame with one owned DataFrame copy.

    Image columns retain their object structure and binary payloads. Other display
    cells are bounded for Parquet and browser safety. ``base_frame`` is not mutated.
    """
    if ATLAS_EMBED_INPUT_COLUMN in base_frame.columns:
        output = base_frame.drop(columns=[ATLAS_EMBED_INPUT_COLUMN])
    else:
        output = base_frame.copy()
    if isinstance(coordinates, AtlasProjectionCoordinates):
        output[ATLAS_PROJECTION_X] = coordinates.values[:, 0]
        output[ATLAS_PROJECTION_Y] = coordinates.values[:, 1]
    else:
        output[ATLAS_PROJECTION_X] = coordinates[ATLAS_PROJECTION_X].to_list()
        output[ATLAS_PROJECTION_Y] = coordinates[ATLAS_PROJECTION_Y].to_list()
    projection_columns = {ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, ATLAS_PROJECTION_NEIGHBORS}
    for column in output.columns:
        if column in projection_columns:
            continue
        if str(column) in preserve_columns:
            output[column] = output[column].map(normalize_image_display_value)
        else:
            output[column] = output[column].map(sanitize_atlas_cell)
    return output

"""Image detection, loading, and Atlas display-value handling."""

from __future__ import annotations

import json
import re
import time
from base64 import b64decode
from binascii import Error as BinasciiError
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pandas as pd

from ..config import ATLAS_TEXT_MAX_CHARS
from .contracts import (
    ATLAS_EMBED_INPUT_COLUMN,
    ATLAS_PROJECTION_NEIGHBORS,
    ATLAS_PROJECTION_X,
    ATLAS_PROJECTION_Y,
    AtlasModality,
)

IMAGE_REFERENCE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp|svg)(?:\?.*)?$", re.IGNORECASE)
IMAGE_HEX_PREFIXES = ("89504e47", "ffd8ff", "47494638", "52494646")
IMAGE_BASE64_PREFIXES = ("ivborw0kggo", "/9j/", "r0lgod", "uklgr")
IMAGE_COLUMN_HINTS = ("image", "img", "photo", "picture", "thumbnail", "patch")
ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS = 20
ATLAS_IMAGE_FETCH_RETRIES = 3
ATLAS_IMAGE_MAX_BYTES = 50 * 1024 * 1024
ATLAS_TRUNCATION_SUFFIX = "... (truncated for Atlas)"


def is_image_reference(value: str) -> bool:
    text = value.strip()
    return bool(text and (text.startswith("data:image") or IMAGE_REFERENCE_PATTERN.search(text)))


def is_image_bytes_string(value: str) -> bool:
    compact = re.sub(r"\s+", "", value.strip())
    if not compact:
        return False
    lowered = compact.lower()
    return lowered.startswith(IMAGE_HEX_PREFIXES) or lowered.startswith(IMAGE_BASE64_PREFIXES)


def is_image_bytes(value: bytes) -> bool:
    return (
        value.startswith(b"\x89PNG\r\n\x1a\n")
        or value.startswith(b"\xff\xd8\xff")
        or value.startswith(b"GIF87a")
        or value.startswith(b"GIF89a")
        or (value.startswith(b"RIFF") and value[8:12] == b"WEBP")
        or value.lstrip().startswith(b"<svg")
    )


def is_image_like_value(value: Any) -> bool:
    if isinstance(value, bytes):
        return is_image_bytes(value)
    if isinstance(value, bytearray):
        return is_image_bytes(bytes(value))
    if isinstance(value, str):
        return is_image_reference(value) or is_image_bytes_string(value)
    if isinstance(value, dict):
        raw_bytes = value.get("bytes")
        path = value.get("path")
        return is_image_like_value(raw_bytes) or (isinstance(path, str) and is_image_reference(path))
    if isinstance(value, list):
        return any(is_image_like_value(item) for item in value[:3])
    return False


def decode_data_image(value: str) -> bytes | None:
    text = value.strip()
    if not text.startswith("data:image"):
        return None
    _, _, payload = text.partition(",")
    if not payload:
        return None
    try:
        return b64decode(payload, validate=False)
    except BinasciiError:
        return None


def decode_image_bytes_string(value: str) -> bytes | None:
    text = re.sub(r"\s+", "", value.strip())
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith(IMAGE_HEX_PREFIXES):
        try:
            return bytes.fromhex(text)
        except ValueError:
            return None
    if lowered.startswith(IMAGE_BASE64_PREFIXES):
        try:
            return b64decode(text, validate=False)
        except BinasciiError:
            return None
    return None


def _read_url_bytes_once(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "local-data-studio/atlas"})
    with urlopen(request, timeout=ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS) as response:
        chunks: list[bytes] = []
        total = 0
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            if total > ATLAS_IMAGE_MAX_BYTES:
                raise ValueError(f"image exceeds {ATLAS_IMAGE_MAX_BYTES} bytes")
            chunks.append(chunk)
    return b"".join(chunks)


def read_url_bytes(url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(ATLAS_IMAGE_FETCH_RETRIES):
        try:
            return _read_url_bytes_once(url)
        except (OSError, URLError, ValueError) as exc:
            last_error = exc
            if attempt + 1 < ATLAS_IMAGE_FETCH_RETRIES:
                time.sleep(0.4 * (attempt + 1))
    raise ValueError(f"failed to read image URL {url}: {last_error}") from last_error


def _resolve_image_path(reference: str, dataset_path: Path) -> Path:
    text = reference.strip()
    if text.startswith("file://"):
        text = text[len("file://") :]
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = dataset_path.parent / candidate
    return candidate.resolve()


def _read_path_bytes(reference: str, dataset_path: Path) -> bytes:
    candidate = _resolve_image_path(reference, dataset_path)
    try:
        stat = candidate.stat()
    except OSError as exc:
        raise ValueError(f"image path does not exist: {reference}") from exc
    if not candidate.is_file():
        raise ValueError(f"image path is not a file: {reference}")
    if stat.st_size > ATLAS_IMAGE_MAX_BYTES:
        raise ValueError(f"image exceeds {ATLAS_IMAGE_MAX_BYTES} bytes: {reference}")
    return candidate.read_bytes()


def image_value_to_bytes(value: Any, dataset_path: Path) -> bytes:
    result: bytes | None = None
    if isinstance(value, bytes):
        result = value
    elif isinstance(value, bytearray):
        result = bytes(value)
    elif isinstance(value, dict):
        raw_bytes = value.get("bytes")
        if raw_bytes not in (None, ""):
            try:
                result = image_value_to_bytes(raw_bytes, dataset_path)
            except ValueError:
                pass
        if result is None and isinstance(value.get("path"), str) and value["path"].strip():
            result = image_value_to_bytes(value["path"], dataset_path)
    elif isinstance(value, list) and all(isinstance(item, int) for item in value):
        result = bytes(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty image value")
        result = decode_data_image(text) or decode_image_bytes_string(text)
        if result is None:
            parsed = urlparse(text)
            if parsed.scheme in {"http", "https"}:
                result = read_url_bytes(text)
            elif parsed.scheme in {"", "file"} and is_image_reference(text):
                result = _read_path_bytes(text, dataset_path)
    if result is None:
        raise ValueError(f"Cannot convert value of type {type(value)} to image/audio format")
    return result


def text_for_embedding(value: Any) -> str:
    text = "null" if value is None else str(value)
    return text[:ATLAS_TEXT_MAX_CHARS] if ATLAS_TEXT_MAX_CHARS and len(text) > ATLAS_TEXT_MAX_CHARS else text


def _truncate_atlas_text(value: str) -> str:
    if not ATLAS_TEXT_MAX_CHARS or len(value) <= ATLAS_TEXT_MAX_CHARS:
        return value
    return f"{value[:ATLAS_TEXT_MAX_CHARS]}{ATLAS_TRUNCATION_SUFFIX}"


def _json_default_for_atlas(value: Any) -> str:
    return f"<binary {len(value)} bytes>" if isinstance(value, bytes | bytearray) else str(value)


def sanitize_atlas_cell(value: Any) -> Any:
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
    if not columns or not hasattr(data_frame, "copy") or not hasattr(data_frame, "columns"):
        return data_frame
    normalized = data_frame.copy()
    for column in normalized.columns:
        if str(column) in columns:
            normalized[column] = normalized[column].map(normalize_image_display_value)
    return normalized


def drop_atlas_embed_input(data_frame: Any) -> Any:
    if hasattr(data_frame, "drop") and ATLAS_EMBED_INPUT_COLUMN in data_frame.columns:
        return data_frame.drop(columns=[ATLAS_EMBED_INPUT_COLUMN])
    return data_frame


def sanitize_atlas_output_frame(data_frame: Any, *, preserve_columns: set[str] | None = None) -> Any:
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


def prepare_image_projection_input(data_frame: Any, *, column: str, dataset_path: Path) -> tuple[Any, Any]:
    try:
        values = data_frame[column].tolist()
    except Exception as exc:
        raise ValueError(f"failed to read image column {column}: {exc}") from exc
    kept_indices: list[int] = []
    embedding_items: list[dict[str, bytes]] = []
    first_error: str | None = None
    for index, value in enumerate(values):
        try:
            image_bytes = image_value_to_bytes(value, dataset_path)
        except ValueError as exc:
            if first_error is None:
                first_error = f"row {index + 1}: {exc}"
            continue
        kept_indices.append(index)
        embedding_items.append({"bytes": image_bytes})
    if not kept_indices:
        raise ValueError(f"no readable images in column {column}; first error: {first_error or 'no image values found'}")
    output_frame = data_frame.iloc[kept_indices].copy().reset_index(drop=True)
    return pd.DataFrame({ATLAS_EMBED_INPUT_COLUMN: embedding_items}), output_frame


def prepare_projection_input(
    data_frame: Any,
    *,
    column: str,
    modality: AtlasModality,
    dataset_path: Path,
) -> tuple[Any, str, Any]:
    if modality == "text":
        values = data_frame[column].tolist()
        frame = pd.DataFrame({ATLAS_EMBED_INPUT_COLUMN: [text_for_embedding(value) for value in values]})
        return frame, ATLAS_EMBED_INPUT_COLUMN, data_frame
    if modality != "image":
        return data_frame, column, data_frame
    frame, output = prepare_image_projection_input(data_frame, column=column, dataset_path=dataset_path)
    return frame, ATLAS_EMBED_INPUT_COLUMN, output


def attach_projection_columns(base_frame: Any, projected_frame: Any) -> Any:
    output = base_frame.copy()
    for column in (ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y):
        if column in projected_frame.columns:
            output[column] = projected_frame[column].to_list()
    return output

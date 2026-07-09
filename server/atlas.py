"""Embedding Atlas job orchestration."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from base64 import b64decode
from binascii import Error as BinasciiError
from dataclasses import dataclass, replace
from importlib.metadata import PackageNotFoundError, version
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
import umap
from embedding_atlas.cli import load_datasets
from embedding_atlas.embedding import create_embedder
from embedding_atlas.projection import Projection, _run_embedding, _run_umap
from fastapi import HTTPException
from PIL import Image
from sentence_transformers import SentenceTransformer

from .atlas_cache import prune_cache_dir
from .cache import DatasetFingerprint
from .config import (
    ATLAS_ANCHOR_SAMPLE,
    ATLAS_BATCH_SIZE,
    ATLAS_CACHE_DIR,
    ATLAS_CACHE_MAX_BYTES,
    ATLAS_CACHE_ROOT,
    ATLAS_DATA_CACHE_DIR,
    ATLAS_EMBEDDING_DTYPE,
    ATLAS_HOST,
    ATLAS_IMAGE_EMBEDDER,
    ATLAS_PORT,
    ATLAS_PROJECTION_MODE,
    ATLAS_SAMPLE,
    ATLAS_TEXT_EMBEDDER,
    ATLAS_TEXT_MAX_CHARS,
    ATLAS_TRUST_REMOTE_CODE,
    BASE_DIR,
    EMBEDDER_MODELS_DIR,
)
from .db import open_connection, quote_ident
from .deleted_rows import deleted_row_ids_for
from .jobs import JobContext
from .sql import configure_duckdb_limits, create_data_view, guard_select_sql_for_dataset

AtlasModality = str

ATLAS_URL_PATTERN = re.compile(
    r"https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\])(?::\d+)?"
    r"(?:/[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)?"
)
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
IMAGE_REFERENCE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp|svg)(?:\?.*)?$", re.IGNORECASE)
IMAGE_HEX_PREFIXES = ("89504e47", "ffd8ff", "47494638", "52494646")
IMAGE_BASE64_PREFIXES = ("ivborw0kggo", "/9j/", "r0lgod", "uklgr")
IMAGE_COLUMN_HINTS = ("image", "img", "photo", "picture", "thumbnail", "patch")
QWEN3_VL_EMBEDDING_MARKER = "qwen3-vl-embedding"
RUNNING_ATLAS_PROCESSES: list[subprocess.Popen[str]] = []
ATLAS_DATASET_CACHE_VERSION = 10
ATLAS_UMAP_RANDOM_STATE = 42
ATLAS_UMAP_N_JOBS = 1
ATLAS_PROJECTION_X = "__local_data_studio_atlas_x"
ATLAS_PROJECTION_Y = "__local_data_studio_atlas_y"
ATLAS_PROJECTION_NEIGHBORS = "__local_data_studio_atlas_neighbors"
ATLAS_EMBED_INPUT_COLUMN = "__local_data_studio_atlas_embed_input"
ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS = 20
ATLAS_IMAGE_FETCH_RETRIES = 3
ATLAS_IMAGE_MAX_BYTES = 50 * 1024 * 1024
ATLAS_TRUNCATION_SUFFIX = "... (truncated for Atlas)"
ATLAS_PORT_LOCK = threading.Lock()
ATLAS_PORT_STATE = {"next": ATLAS_PORT}
MODEL_MARKER_FILES = (
    "config.json",
    "modules.json",
    "tokenizer_config.json",
    "preprocessor_config.json",
    "model.safetensors",
    "pytorch_model.bin",
)


@dataclass(frozen=True, slots=True)
class AtlasOptions:
    """Resolved options for launching Embedding Atlas."""

    sample: int | None
    host: str
    port: int
    batch_size: int | None
    text_embedder: str | None
    image_embedder: str | None
    trust_remote_code: bool
    embedding_dtype: str = "float32"
    projection_mode: str = "full"
    anchor_sample: int | None = None

    @classmethod
    def from_request(cls, sample: int | None = None) -> AtlasOptions:
        requested_sample = sample if sample is not None else ATLAS_SAMPLE
        anchor_sample = ATLAS_ANCHOR_SAMPLE if ATLAS_ANCHOR_SAMPLE > 0 else None
        return cls(
            sample=requested_sample if requested_sample and requested_sample > 0 else None,
            host=ATLAS_HOST,
            port=ATLAS_PORT,
            batch_size=ATLAS_BATCH_SIZE if ATLAS_BATCH_SIZE > 0 else None,
            text_embedder=ATLAS_TEXT_EMBEDDER,
            image_embedder=ATLAS_IMAGE_EMBEDDER,
            trust_remote_code=ATLAS_TRUST_REMOTE_CODE,
            embedding_dtype=ATLAS_EMBEDDING_DTYPE,
            projection_mode=ATLAS_PROJECTION_MODE,
            anchor_sample=anchor_sample,
        )


@dataclass(frozen=True, slots=True)
class AtlasPreparedDataset:
    """Materialized Atlas input with precomputed projection columns."""

    path: Path
    x: str
    y: str
    neighbors: str | None
    cache_hit: bool


def _embedding_atlas_executable() -> list[str]:
    return [sys.executable, "-m", "embedding_atlas.cli"]


def _is_model_directory(path: Path) -> bool:
    if not path.is_dir() or path.name.startswith("."):
        return False
    return any((path / marker).exists() for marker in MODEL_MARKER_FILES)


def _model_label(path: Path) -> str:
    relative = path.relative_to(EMBEDDER_MODELS_DIR)
    return relative.as_posix()


def discover_embedder_models() -> list[dict[str, str]]:
    """Return locally installed encoder models under models/embedder."""
    EMBEDDER_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    models: list[dict[str, str]] = []
    discovered_roots: list[Path] = []
    paths = sorted(
        EMBEDDER_MODELS_DIR.rglob("*"),
        key=lambda item: (len(item.relative_to(EMBEDDER_MODELS_DIR).parts), item.as_posix()),
    )
    for path in paths:
        if any(root == path or root in path.parents for root in discovered_roots):
            continue
        if not _is_model_directory(path):
            continue
        discovered_roots.append(path)
        label = _model_label(path)
        models.append(
            {
                "name": label,
                "value": label,
                "path": str(path),
            }
        )
    return models


def resolve_embedder_model(model: str) -> Path:
    """Resolve a model dropdown value to a local model directory."""
    model_name = model.strip()
    if not model_name:
        raise HTTPException(status_code=400, detail="model is required")
    candidate = (EMBEDDER_MODELS_DIR / model_name).resolve()
    root = EMBEDDER_MODELS_DIR.resolve()
    if root != candidate and root not in candidate.parents:
        raise HTTPException(status_code=400, detail="invalid model path")
    if not _is_model_directory(candidate):
        raise HTTPException(status_code=404, detail="model not found under models/embedder")
    return candidate


def _normalize_model_identity(value: str) -> str:
    return value.lower().replace("_", "-").replace("/", "-")


def _is_qwen3_vl_embedding_model(model_path: Path) -> bool:
    """Return True for Qwen3-VL-Embedding models that need SentenceTransformer inputs."""
    normalized_path = _normalize_model_identity(model_path.as_posix())
    if QWEN3_VL_EMBEDDING_MARKER in normalized_path:
        return True

    config_path = model_path / "config.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False

    candidates = [str(config.get("model_type", ""))]
    architectures = config.get("architectures")
    if isinstance(architectures, list):
        candidates.extend(str(item) for item in architectures)
    normalized_config = _normalize_model_identity(" ".join(candidates))
    return "qwen3-vl" in normalized_config and "embedding" in normalized_config


def _is_image_reference(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if text.startswith("data:image"):
        return True
    return IMAGE_REFERENCE_PATTERN.search(text) is not None


def _is_image_bytes_string(value: str) -> bool:
    compact = re.sub(r"\s+", "", value.strip())
    if not compact:
        return False
    lowered = compact.lower()
    if lowered.startswith(IMAGE_HEX_PREFIXES):
        return True
    return lowered.startswith(IMAGE_BASE64_PREFIXES)


def _is_image_bytes(value: bytes) -> bool:
    return (
        value.startswith(b"\x89PNG\r\n\x1a\n")
        or value.startswith(b"\xff\xd8\xff")
        or value.startswith(b"GIF87a")
        or value.startswith(b"GIF89a")
        or (value.startswith(b"RIFF") and value[8:12] == b"WEBP")
        or value.lstrip().startswith(b"<svg")
    )


def _is_image_like_value(value: Any) -> bool:
    result = False
    if isinstance(value, bytes):
        result = _is_image_bytes(value)
    elif isinstance(value, bytearray):
        result = _is_image_bytes(bytes(value))
    elif isinstance(value, str):
        result = _is_image_reference(value) or _is_image_bytes_string(value)
    elif isinstance(value, dict):
        raw_bytes = value.get("bytes")
        path = value.get("path")
        result = _is_image_like_value(raw_bytes) or (isinstance(path, str) and _is_image_reference(path))
    elif isinstance(value, list):
        result = any(_is_image_like_value(item) for item in value[:3])
    return result


def _sample_column_values(path: Path, column: str, sql: str | None, deleted_ids: list[int]) -> list[Any]:
    with open_connection() as connection:
        configure_duckdb_limits(connection)
        create_data_view(connection, path, deleted_ids)
        quoted_column = quote_ident(column)
        source_sql = f"({sql}) AS atlas_source" if sql else "data"
        query = f"SELECT {quoted_column} FROM {source_sql} WHERE {quoted_column} IS NOT NULL LIMIT 50"
        try:
            rows = connection.execute(query).fetchall()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"failed to read selected column: {exc}") from exc
    return [row[0] for row in rows]


def infer_atlas_modality(path: Path, column: str, sql: str | None = None, deleted_ids: list[int] | None = None) -> AtlasModality:
    """Infer whether Embedding Atlas should treat the selected column as image or text."""
    values = _sample_column_values(path, column, sql, deleted_ids or [])
    if any(_is_image_like_value(value) for value in values):
        return "image"
    lowered_column = column.lower()
    if any(hint in lowered_column for hint in IMAGE_COLUMN_HINTS):
        return "image"
    return "text"


def reserve_atlas_start_port(options: AtlasOptions) -> AtlasOptions:
    """Return options with a unique preferred port to avoid concurrent Atlas races."""
    with ATLAS_PORT_LOCK:
        port = max(options.port, ATLAS_PORT_STATE["next"])
        ATLAS_PORT_STATE["next"] = port + 1
    return replace(options, port=port)


def _decode_data_image(value: str) -> bytes | None:
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


def _decode_image_bytes_string(value: str) -> bytes | None:
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
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > ATLAS_IMAGE_MAX_BYTES:
                raise ValueError(f"image exceeds {ATLAS_IMAGE_MAX_BYTES} bytes")
            chunks.append(chunk)
    return b"".join(chunks)


def _read_url_bytes(url: str) -> bytes:
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


def _image_value_to_bytes(value: Any, dataset_path: Path) -> bytes:
    result: bytes | None = None
    if isinstance(value, bytes):
        result = value
    elif isinstance(value, bytearray):
        result = bytes(value)
    elif isinstance(value, dict):
        raw_bytes = value.get("bytes")
        if raw_bytes not in (None, ""):
            try:
                result = _image_value_to_bytes(raw_bytes, dataset_path)
            except ValueError:
                pass
        if result is None:
            raw_path = value.get("path")
            if isinstance(raw_path, str) and raw_path.strip():
                result = _image_value_to_bytes(raw_path, dataset_path)
    elif isinstance(value, list) and all(isinstance(item, int) for item in value):
        result = bytes(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty image value")
        decoded = _decode_data_image(text) or _decode_image_bytes_string(text)
        if decoded is not None:
            result = decoded
        else:
            parsed = urlparse(text)
            if parsed.scheme in {"http", "https"}:
                result = _read_url_bytes(text)
            elif parsed.scheme in {"", "file"} and _is_image_reference(text):
                result = _read_path_bytes(text, dataset_path)
    if result is None:
        raise ValueError(f"Cannot convert value of type {type(value)} to image/audio format")
    return result


def _text_for_embedding(value: Any) -> str:
    text = "null" if value is None else str(value)
    if ATLAS_TEXT_MAX_CHARS and len(text) > ATLAS_TEXT_MAX_CHARS:
        return text[:ATLAS_TEXT_MAX_CHARS]
    return text


def _truncate_atlas_text(value: str) -> str:
    if not ATLAS_TEXT_MAX_CHARS or len(value) <= ATLAS_TEXT_MAX_CHARS:
        return value
    return f"{value[:ATLAS_TEXT_MAX_CHARS]}{ATLAS_TRUNCATION_SUFFIX}"


def _json_default_for_atlas(value: Any) -> str:
    if isinstance(value, bytes | bytearray):
        return f"<binary {len(value)} bytes>"
    return str(value)


def _sanitize_atlas_cell(value: Any) -> Any:
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
        decoded = _decode_data_image(value) or _decode_image_bytes_string(value)
        return decoded if decoded is not None else value
    return value


def _normalize_image_display_value(value: Any, *, key: str | None = None) -> Any:
    if key == "bytes":
        return _normalize_image_display_bytes(value)
    if isinstance(value, bytes | bytearray):
        return bytes(value)
    if isinstance(value, dict):
        return {str(item_key): _normalize_image_display_value(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_image_display_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_image_display_value(item) for item in value]
    return value


def _normalize_image_display_columns(data_frame: Any, columns: set[str]) -> Any:
    if not columns or not hasattr(data_frame, "copy") or not hasattr(data_frame, "columns"):
        return data_frame
    normalized = data_frame.copy()
    for column in normalized.columns:
        if str(column) in columns:
            normalized[column] = normalized[column].map(_normalize_image_display_value)
    return normalized


def _sanitize_atlas_output_frame(data_frame: Any, *, preserve_columns: set[str] | None = None) -> Any:
    output = _drop_atlas_embed_input(data_frame)
    if not hasattr(output, "copy") or not hasattr(output, "columns"):
        return output
    sanitized = output.copy()
    projection_columns = {ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, ATLAS_PROJECTION_NEIGHBORS}
    preserved = preserve_columns or set()
    for column in sanitized.columns:
        if column in projection_columns or str(column) in preserved:
            continue
        sanitized[column] = sanitized[column].map(_sanitize_atlas_cell)
    return sanitized


def _image_like_columns(data_frame: Any, *, sample_size: int = 50) -> set[str]:
    if not hasattr(data_frame, "columns"):
        return set()
    columns: set[str] = set()
    for column in data_frame.columns:
        try:
            values = data_frame[column].dropna().head(sample_size).tolist()
        except Exception:
            continue
        if any(_is_image_like_value(value) for value in values):
            columns.add(str(column))
    return columns


def _prepare_image_projection_input(data_frame: Any, *, column: str, dataset_path: Path) -> tuple[Any, Any]:
    try:
        values = data_frame[column].tolist()
    except Exception as exc:
        raise ValueError(f"failed to read image column {column}: {exc}") from exc

    kept_indices: list[int] = []
    embedding_items: list[dict[str, bytes]] = []
    conversion_errors: list[str] = []
    for index, value in enumerate(values):
        try:
            image_bytes = _image_value_to_bytes(value, dataset_path)
        except ValueError as exc:
            conversion_errors.append(f"row {index + 1}: {exc}")
            continue
        kept_indices.append(index)
        embedding_items.append({"bytes": image_bytes})

    if not kept_indices:
        detail = conversion_errors[0] if conversion_errors else "no image values found"
        raise ValueError(f"no readable images in column {column}; first error: {detail}")

    output_frame = data_frame.iloc[kept_indices].copy().reset_index(drop=True)
    projection_frame = pd.DataFrame({ATLAS_EMBED_INPUT_COLUMN: embedding_items})
    return projection_frame, output_frame


def _prepare_projection_input(data_frame: Any, *, column: str, modality: AtlasModality, dataset_path: Path) -> tuple[Any, str, Any]:
    if modality == "text":
        values = data_frame[column].tolist()
        projection_frame = pd.DataFrame({ATLAS_EMBED_INPUT_COLUMN: [_text_for_embedding(value) for value in values]})
        return projection_frame, ATLAS_EMBED_INPUT_COLUMN, data_frame
    if modality != "image":
        return data_frame, column, data_frame

    projection_frame, output_frame = _prepare_image_projection_input(
        data_frame,
        column=column,
        dataset_path=dataset_path,
    )
    return projection_frame, ATLAS_EMBED_INPUT_COLUMN, output_frame


def _drop_atlas_embed_input(data_frame: Any) -> Any:
    if hasattr(data_frame, "drop") and ATLAS_EMBED_INPUT_COLUMN in data_frame.columns:
        return data_frame.drop(columns=[ATLAS_EMBED_INPUT_COLUMN])
    return data_frame


def _attach_projection_columns(base_frame: Any, projected_frame: Any) -> Any:
    output = base_frame.copy()
    for column in (ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y):
        if column in projected_frame.columns:
            output[column] = projected_frame[column].to_list()
    return output


def _projection_dtype(options: AtlasOptions) -> Any:
    return np.float16 if options.embedding_dtype == "float16" else np.float32


def _default_embedder_for_modality(modality: AtlasModality) -> str:
    return "sentence-transformers" if modality == "text" else "transformers"


def _effective_embedder_for_modality(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> str:
    if _is_qwen3_vl_embedding_model(model_path):
        return "sentence-transformers"
    configured = _atlas_embedder_for_modality(modality, options)
    return configured or _default_embedder_for_modality(modality)


def _load_sentence_transformer_model(model_path: Path, options: AtlasOptions) -> Any:
    kwargs = {"trust_remote_code": True} if options.trust_remote_code else {}
    return SentenceTransformer(str(model_path), **kwargs)


def _qwen3_vl_image_input(item: Any) -> Any:
    if isinstance(item, dict) and isinstance(item.get("bytes"), bytes | bytearray):
        return Image.open(BytesIO(bytes(item["bytes"]))).convert("RGB")
    if isinstance(item, dict) and item.get("image") is not None:
        return item["image"]
    return item


def _qwen3_vl_sentence_transformer_input(item: Any, modality: AtlasModality) -> Any:
    if modality == "image":
        return {"image": _qwen3_vl_image_input(item)}
    return item


def _create_qwen3_vl_sentence_transformer_embedder(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> Any:
    st_model = _load_sentence_transformer_model(model_path, options)

    async def _embed(batch: list[Any], *, model: str | None, embedder_args: dict) -> np.ndarray:  # noqa: ARG001
        encoded_inputs = [_qwen3_vl_sentence_transformer_input(item, modality) for item in batch]
        embeddings = st_model.encode(encoded_inputs, show_progress_bar=False, batch_size=max(len(encoded_inputs), 1))
        return np.asarray(embeddings, dtype=np.float32)

    return _embed


def _resolve_embedder_callable(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> Any:
    embedder_name = _effective_embedder_for_modality(modality, model_path, options)
    embedder_args = {"trust_remote_code": True} if options.trust_remote_code else {}
    if embedder_name == "sentence-transformers" and _is_qwen3_vl_embedding_model(model_path):
        return _create_qwen3_vl_sentence_transformer_embedder(modality, model_path, options), embedder_args
    return create_embedder(embedder_name, modality=modality, model=str(model_path), embedder_args=embedder_args), embedder_args


def _embedding_items(projection_input: Any, input_column: str) -> list[Any]:
    try:
        return projection_input[input_column].tolist()
    except Exception as exc:
        raise ValueError(f"failed to read Atlas embedding input: {exc}") from exc


def _cast_embeddings(embeddings: np.ndarray, options: AtlasOptions) -> np.ndarray:
    return np.asarray(embeddings, dtype=_projection_dtype(options))


def _embed_items(items: list[Any], *, modality: AtlasModality, model_path: Path, options: AtlasOptions) -> np.ndarray:
    if not items:
        return np.empty((0, 0), dtype=_projection_dtype(options))
    if modality == "vector":
        return _cast_embeddings(np.asarray(items), options)
    embedder, embedder_args = _resolve_embedder_callable(modality, model_path, options)
    embeddings = asyncio.run(
        _run_embedding(
            embedder,
            items,
            model=str(model_path),
            embedder_args=embedder_args,
            batch_size=options.batch_size,
            max_concurrency=1,
        )
    )
    return _cast_embeddings(embeddings, options)


def _zero_projection(row_count: int) -> Projection:
    return Projection(
        projection=np.zeros((row_count, 2), dtype=np.float32),
        knn_indices=np.zeros((row_count, 0), dtype=np.int64),
        knn_distances=np.zeros((row_count, 0), dtype=np.float32),
    )


def _run_full_projection(embeddings: np.ndarray) -> Projection:
    if len(embeddings) <= 1:
        return _zero_projection(len(embeddings))
    n_neighbors = min(15, max(2, len(embeddings) - 1))
    return _run_umap(
        embeddings,
        umap_args={
            "metric": "cosine",
            "n_neighbors": n_neighbors,
            "random_state": ATLAS_UMAP_RANDOM_STATE,
            "n_jobs": ATLAS_UMAP_N_JOBS,
        },
    )


def _compute_full_projection(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
) -> Any:
    items = _embedding_items(projection_input, input_column)
    embeddings = _embed_items(items, modality=modality, model_path=model_path, options=options)
    projection_result = _run_full_projection(embeddings)
    output = projection_input.copy()
    output[ATLAS_PROJECTION_X] = projection_result.projection[:, 0].tolist()
    output[ATLAS_PROJECTION_Y] = projection_result.projection[:, 1].tolist()
    output[ATLAS_PROJECTION_NEIGHBORS] = [
        {
            "ids": np.asarray(ids, dtype=np.int64).tolist(),
            "distances": np.asarray(distances, dtype=float).tolist(),
        }
        for ids, distances in zip(projection_result.knn_indices, projection_result.knn_distances)
    ]
    return output


def _anchor_indices(row_count: int, anchor_sample: int | None) -> np.ndarray:
    anchor_count = min(anchor_sample or row_count, row_count)
    if anchor_count >= row_count:
        return np.arange(row_count)
    rng = np.random.default_rng(42)
    return np.sort(rng.choice(row_count, size=anchor_count, replace=False))


def _compute_anchor_transform_projection(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
) -> Any:
    items = _embedding_items(projection_input, input_column)
    row_count = len(items)
    if row_count <= 1:
        output = projection_input.copy()
        output[ATLAS_PROJECTION_X] = [0.0] * row_count
        output[ATLAS_PROJECTION_Y] = [0.0] * row_count
        return output

    anchor_indices = _anchor_indices(row_count, options.anchor_sample)
    anchor_set = set(anchor_indices.tolist())
    anchor_items = [items[index] for index in anchor_indices]
    anchor_embeddings = _embed_items(anchor_items, modality=modality, model_path=model_path, options=options)

    n_neighbors = min(15, max(2, len(anchor_indices) - 1))
    reducer = umap.UMAP(
        metric="cosine",
        n_neighbors=n_neighbors,
        random_state=ATLAS_UMAP_RANDOM_STATE,
        n_jobs=ATLAS_UMAP_N_JOBS,
    )
    anchor_projection = reducer.fit_transform(anchor_embeddings)
    projection_values = np.empty((row_count, 2), dtype=np.float32)
    projection_values[anchor_indices] = np.asarray(anchor_projection, dtype=np.float32)

    transform_batch_size = max(options.batch_size or 256, 1)
    remaining_indices = [index for index in range(row_count) if index not in anchor_set]
    for start in range(0, len(remaining_indices), transform_batch_size):
        batch_indices = remaining_indices[start : start + transform_batch_size]
        batch_items = [items[index] for index in batch_indices]
        batch_embeddings = _embed_items(batch_items, modality=modality, model_path=model_path, options=options)
        projection_values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)

    output = projection_input.copy()
    output[ATLAS_PROJECTION_X] = projection_values[:, 0].tolist()
    output[ATLAS_PROJECTION_Y] = projection_values[:, 1].tolist()
    return output


def project_atlas_frame(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
) -> Any:
    if options.projection_mode == "anchor_transform":
        return _compute_anchor_transform_projection(
            projection_input,
            input_column=input_column,
            modality=modality,
            model_path=model_path,
            options=options,
        )
    return _compute_full_projection(
        projection_input,
        input_column=input_column,
        modality=modality,
        model_path=model_path,
        options=options,
    )


def build_atlas_command(
    *,
    path: Path,
    column: str,
    modality: AtlasModality,
    sql: str | None,
    model_path: Path,
    options: AtlasOptions,
    projection_columns: tuple[str, str, str | None] | None = None,
) -> list[str]:
    command = [
        *_embedding_atlas_executable(),
        str(path.resolve()),
        f"--{modality}",
        column,
        "--host",
        options.host,
        "--port",
        str(options.port),
        "--auto-port",
    ]
    if projection_columns is not None:
        x_column, y_column, neighbors_column = projection_columns
        command.extend(
            [
                "--disable-projection",
                "--x",
                x_column,
                "--y",
                y_column,
            ]
        )
        if neighbors_column is not None:
            command.extend(["--neighbors", neighbors_column])
        return command

    if sql:
        command.extend(["--query", sql])
    if options.sample is not None:
        command.extend(["--sample", str(options.sample)])
    if options.batch_size is not None:
        command.extend(["--batch-size", str(options.batch_size)])

    command.extend(["--with", "server.atlas_cache_patch"])
    command.extend(["--model", str(model_path)])

    embedder = _effective_embedder_for_modality(modality, model_path, options)
    if embedder:
        command.extend(["--embedder", embedder])

    if options.trust_remote_code:
        command.append("--trust-remote-code")
    return command


def _reader_thread(stream: Any, output: queue.Queue[str]) -> None:
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            output.put(line.rstrip())
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def _normalize_atlas_url(url: str) -> str:
    cleaned = ANSI_ESCAPE_PATTERN.sub("", url).strip().rstrip(".,);")
    return cleaned.replace("://0.0.0.0", "://127.0.0.1")


def _embedding_atlas_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    env["PYTHONPATH"] = f"{BASE_DIR}{os.pathsep}{env.get('PYTHONPATH', '')}"
    env["LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR"] = str(ATLAS_CACHE_DIR)
    env["LOCAL_DATA_STUDIO_ATLAS_CACHE_PRUNE_DIR"] = str(ATLAS_CACHE_ROOT)
    env["LOCAL_DATA_STUDIO_ATLAS_CACHE_MAX_BYTES"] = str(ATLAS_CACHE_MAX_BYTES)
    env.setdefault("TOKENIZERS_PARALLELISM", "false")
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("OPENBLAS_NUM_THREADS", "1")
    env.setdefault("MKL_NUM_THREADS", "1")
    env.setdefault("VECLIB_MAXIMUM_THREADS", "1")
    env.setdefault("NUMEXPR_NUM_THREADS", "1")
    return env


def _format_process_returncode(returncode: int | None) -> str:
    if returncode is None:
        return "still running"
    if returncode >= 0:
        return f"exit code {returncode}"
    try:
        signal_name = signal.Signals(-returncode).name
    except ValueError:
        signal_name = f"signal {-returncode}"
    return f"{signal_name} ({returncode})"


def _spawn_embedding_atlas(command: list[str], env: dict[str, str]) -> subprocess.Popen[str]:
    # Keep this call eligible for Python's posix_spawn path on macOS. Passing
    # cwd/preexec_fn/pass_fds/start_new_session or using close_fds=True can
    # reintroduce child-side fork crashes; see docs/atlas_sigsegv_incident_log_ja.md.
    return subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        close_fds=False,
    )


def launch_embedding_atlas(command: list[str], context: JobContext) -> tuple[str, int]:
    """Start Embedding Atlas and return its browser URL once available."""
    prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
    env = _embedding_atlas_env()

    try:
        process = _spawn_embedding_atlas(command, env)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail="embedding-atlas is not installed; run uv sync") from exc

    output: queue.Queue[str] = queue.Queue()
    if process.stdout is not None:
        reader = threading.Thread(target=_reader_thread, args=(process.stdout, output), daemon=True)
        reader.start()

    started = time.monotonic()
    recent_lines: list[str] = []
    try:
        while True:
            context.check_cancelled()
            while True:
                try:
                    line = output.get_nowait()
                except queue.Empty:
                    break
                if line:
                    recent_lines.append(line)
                    recent_lines = recent_lines[-12:]
                    match = ATLAS_URL_PATTERN.search(line)
                    if match:
                        url = _normalize_atlas_url(match.group(0))
                        prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
                        RUNNING_ATLAS_PROCESSES.append(process)
                        context.update(progress=1.0, message="Embedding Atlas is ready")
                        return url, process.pid
                    context.update(progress=None, message=line[-180:])

            if process.poll() is not None:
                details = "\n".join(recent_lines[-6:]) or _format_process_returncode(process.returncode)
                raise HTTPException(status_code=500, detail=f"embedding-atlas exited before producing a URL: {details}")

            elapsed = time.monotonic() - started
            progress = min(0.95, 0.05 + elapsed / 300)
            context.update(progress=progress, message=f"Running Embedding Atlas for {int(elapsed)}s")
            time.sleep(0.5)
    except Exception:
        _terminate_process(process)
        raise


def _safe_cache_stem(path: Path, source: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "-", path.stem).strip("-")
    return f"{stem or 'dataset'}-{source}"


def _embedding_atlas_version() -> str:
    try:
        return version("embedding-atlas")
    except PackageNotFoundError:
        return "unknown"


def _model_cache_identity(model_path: Path) -> dict[str, Any]:
    try:
        stat = model_path.stat()
        modified_ns = stat.st_mtime_ns
    except OSError:
        modified_ns = 0
    return {
        "label": _model_label(model_path),
        "path": str(model_path.resolve()),
        "modified_ns": modified_ns,
    }


def _atlas_embedder_for_modality(modality: AtlasModality, options: AtlasOptions) -> str | None:
    return options.image_embedder if modality == "image" else options.text_embedder


def atlas_dataset_cache_path(
    *,
    path: Path,
    column: str,
    modality: AtlasModality,
    sql: str | None,
    model_path: Path,
    options: AtlasOptions,
) -> Path:
    """Return the materialized projected Atlas parquet path for identical inputs/settings."""
    fingerprint = DatasetFingerprint.from_path(path)
    payload = {
        "version": ATLAS_DATASET_CACHE_VERSION,
        "embedding_atlas_version": _embedding_atlas_version(),
        "dataset": {
            "key": fingerprint.key,
            "path": str(fingerprint.path),
            "size": fingerprint.size,
            "modified_ns": fingerprint.modified_ns,
        },
        "source": "query" if sql else "dataset",
        "sql": sql,
        "column": column,
        "modality": modality,
        "model": _model_cache_identity(model_path),
        "embedder": _effective_embedder_for_modality(modality, model_path, options),
        "sample": options.sample,
        "batch_size": options.batch_size,
        "text_max_chars": ATLAS_TEXT_MAX_CHARS,
        "embedding_dtype": options.embedding_dtype,
        "projection_mode": options.projection_mode,
        "anchor_sample": options.anchor_sample,
        "trust_remote_code": options.trust_remote_code,
        "projection_columns": {
            "x": ATLAS_PROJECTION_X,
            "y": ATLAS_PROJECTION_Y,
            "neighbors": ATLAS_PROJECTION_NEIGHBORS,
        },
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return ATLAS_DATA_CACHE_DIR / f"{_safe_cache_stem(path, payload['source'])}-{digest[:16]}.parquet"


def prepare_atlas_dataset(
    *,
    path: Path,
    column: str,
    modality: AtlasModality,
    sql: str | None,
    model_path: Path,
    options: AtlasOptions,
    context: JobContext,
) -> AtlasPreparedDataset:
    """Create or reuse a projected parquet that Atlas can open without recomputing embeddings."""
    cache_path = atlas_dataset_cache_path(
        path=path,
        column=column,
        modality=modality,
        sql=sql,
        model_path=model_path,
        options=options,
    )
    if cache_path.exists():
        prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES, preserve=(cache_path,))
        context.update(progress=0.08, message="Using cached Atlas dataset")
        return AtlasPreparedDataset(
            path=cache_path,
            x=ATLAS_PROJECTION_X,
            y=ATLAS_PROJECTION_Y,
            neighbors=None,
            cache_hit=True,
        )

    context.update(progress=0.08, message="Building Atlas dataset cache")
    prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)

    try:
        data_frame = load_datasets([str(path)], query=sql, sample=options.sample)
        projection_input, input_column, output_frame = _prepare_projection_input(
            data_frame,
            column=column,
            modality=modality,
            dataset_path=path,
        )
        projected = project_atlas_frame(
            projection_input,
            input_column=input_column,
            modality=modality,
            model_path=model_path,
            options=options,
        )
        projected = _attach_projection_columns(output_frame, projected)
        preserve_columns = _image_like_columns(output_frame)
        projected = _normalize_image_display_columns(projected, preserve_columns)
        projected = _sanitize_atlas_output_frame(projected, preserve_columns=preserve_columns)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        projected.to_parquet(tmp_path)
        tmp_path.replace(cache_path)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Atlas dataset cache generation failed: {exc}") from exc
    finally:
        prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES, preserve=(cache_path,))

    return AtlasPreparedDataset(
        path=cache_path,
        x=ATLAS_PROJECTION_X,
        y=ATLAS_PROJECTION_Y,
        neighbors=None,
        cache_hit=False,
    )


def run_atlas_visualization(
    *,
    file_name: str,
    path: Path,
    column: str,
    model: str,
    sql: str | None,
    sample: int | None,
    context: JobContext,
) -> dict[str, Any]:
    """Launch Embedding Atlas for the selected dataset column or SQL query results."""
    selected_column = column.strip()
    if not selected_column:
        raise HTTPException(status_code=400, detail="column is required")

    model_path = resolve_embedder_model(model)
    guarded_sql = guard_select_sql_for_dataset(path, sql) if sql else None
    deleted_ids = deleted_row_ids_for(path)
    context.update(progress=0.02, message="Inspecting selected column")
    modality = infer_atlas_modality(path, selected_column, guarded_sql, deleted_ids)
    options = reserve_atlas_start_port(AtlasOptions.from_request(sample=sample))
    prepared = prepare_atlas_dataset(
        path=path,
        column=selected_column,
        modality=modality,
        sql=guarded_sql,
        model_path=model_path,
        options=options,
        context=context,
    )

    command = build_atlas_command(
        path=prepared.path,
        column=selected_column,
        modality=modality,
        sql=None,
        model_path=model_path,
        options=options,
        projection_columns=(prepared.x, prepared.y, prepared.neighbors),
    )

    source = "query" if guarded_sql else "dataset"
    context.update(progress=0.15, message="Starting Embedding Atlas")
    url, pid = launch_embedding_atlas(command, context)
    return {
        "file": file_name,
        "column": selected_column,
        "modality": modality,
        "model": _model_label(model_path),
        "source": source,
        "url": url,
        "pid": pid,
        "sample": options.sample,
        "embedding_dtype": options.embedding_dtype,
        "projection_mode": options.projection_mode,
        "anchor_sample": options.anchor_sample,
        "cache_hit": prepared.cache_hit,
        "cache_path": str(prepared.path),
    }

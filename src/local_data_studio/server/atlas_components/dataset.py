"""Atlas projected-dataset cache construction."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from embedding_atlas.cli import load_datasets
from fastapi import HTTPException

from .. import embedder_models
from ..atlas_cache import prune_cache_dir
from ..cache import DatasetFingerprint
from ..config import (
    ATLAS_CACHE_MAX_BYTES,
    ATLAS_CACHE_ROOT,
    ATLAS_DATA_CACHE_DIR,
    ATLAS_TEXT_MAX_CHARS,
    EMBEDDER_MODELS_DIR,
)
from ..jobs import JobCancelledError, JobContext
from .contracts import (
    ATLAS_PROJECTION_NEIGHBORS,
    ATLAS_PROJECTION_X,
    ATLAS_PROJECTION_Y,
    AtlasModality,
    AtlasOptions,
    AtlasPreparedDataset,
)
from .embedding_backends import effective_embedder_for_modality
from .images import (
    build_atlas_output_frame,
    image_like_columns,
    prepare_projection_input,
)
from .projection import project_atlas_frame
from .prompts import PromptTemplateError, compile_prompt_template
from .sampling import sample_atlas_frame

ATLAS_DATASET_CACHE_VERSION = 11


def _check_cancelled(context: JobContext) -> None:
    checker = getattr(context, "check_cancelled", None)
    if checker is not None:
        checker()


@lru_cache(maxsize=256)
def _cache_generation_lock(cache_path: str) -> threading.Lock:
    """Return a bounded, process-local single-flight lock for one cache key."""
    return threading.Lock()


def model_label(path: Path) -> str:
    """Return a stable model label relative to the configured model root."""
    return embedder_models.model_label(path, EMBEDDER_MODELS_DIR)


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
        modified_ns = model_path.stat().st_mtime_ns
    except OSError:
        modified_ns = 0
    return {"label": model_label(model_path), "path": str(model_path.resolve()), "modified_ns": modified_ns}


def atlas_dataset_cache_path(
    *,
    path: Path,
    column: str,
    modality: AtlasModality,
    sql: str | None,
    model_path: Path,
    options: AtlasOptions,
) -> Path:
    """Return the projected parquet path for identical inputs and settings."""
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
        "embedder": effective_embedder_for_modality(modality, model_path, options),
        "prompt": options.prompt,
        "capability_fingerprint": options.capability_fingerprint,
        "sample": options.sample,
        "batch_size": options.batch_size,
        "text_max_chars": ATLAS_TEXT_MAX_CHARS,
        "embedding_dtype": options.embedding_dtype,
        "projection_method": options.projection_method,
        "umap": (
            {
                "projection_mode": options.umap_projection_mode,
                "anchor_sample": options.umap_anchor_sample,
            }
            if options.projection_method == "umap"
            else None
        ),
        "trust_remote_code": options.trust_remote_code,
        "projection_columns": {
            "x": ATLAS_PROJECTION_X,
            "y": ATLAS_PROJECTION_Y,
            "neighbors": ATLAS_PROJECTION_NEIGHBORS,
        },
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
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
    """Create or reuse a projected parquet that Atlas can open directly."""
    cache_path = atlas_dataset_cache_path(
        path=path,
        column=column,
        modality=modality,
        sql=sql,
        model_path=model_path,
        options=options,
    )
    generation_lock = _cache_generation_lock(str(cache_path))
    while not generation_lock.acquire(timeout=0.25):
        _check_cancelled(context)
        context.update(progress=0.04, message="Waiting for an identical Atlas cache job")
    tmp_path: Path | None = None
    try:
        _check_cancelled(context)
        if cache_path.exists():
            prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES, preserve=(cache_path,))
            context.update(progress=0.90, message="Using cached Atlas dataset")
            import pyarrow.parquet as pq  # noqa: PLC0415

            row_count = pq.ParquetFile(cache_path).metadata.num_rows
            return AtlasPreparedDataset(cache_path, ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, None, True, row_count)

        context.update(progress=0.05, message="Loading the selected Atlas rows")
        prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
        try:
            data_frame = load_datasets([str(path)], query=sql, sample=None)
            _check_cancelled(context)
            context.update(progress=0.09, message=f"Loaded {len(data_frame):,} candidate rows")
            data_frame = sample_atlas_frame(data_frame, options.sample)
            context.update(progress=0.12, message=f"Preparing {len(data_frame):,} embedding inputs")
            prompt_template = (
                compile_prompt_template(options.prompt, [str(column_name) for column_name in data_frame.columns], ATLAS_TEXT_MAX_CHARS)
                if options.prompt
                else None
            )
            projection_input, input_column, output_frame = prepare_projection_input(
                data_frame,
                column=column,
                modality=modality,
                dataset_path=path,
                prompt_template=prompt_template,
            )
            coordinates = project_atlas_frame(
                projection_input,
                input_column=input_column,
                modality=modality,
                model_path=model_path,
                options=options,
                context=context,
            )
            _check_cancelled(context)
            context.update(progress=0.86, message="Preparing the Atlas display dataset")
            preserve_columns = image_like_columns(output_frame)
            projected = build_atlas_output_frame(output_frame, coordinates, preserve_columns)
            _check_cancelled(context)
            context.update(progress=0.90, message="Writing the Atlas dataset cache")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
            projected.to_parquet(tmp_path)
            _check_cancelled(context)
            tmp_path.replace(cache_path)
            tmp_path = None
            context.update(progress=0.94, message="Atlas dataset cache is ready")
        except JobCancelledError:
            raise
        except HTTPException:
            raise
        except PromptTemplateError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Atlas dataset cache generation failed: {exc}") from exc
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
            prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES, preserve=(cache_path,))
        return AtlasPreparedDataset(cache_path, ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, None, False, len(projected))
    finally:
        generation_lock.release()

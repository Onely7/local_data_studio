"""Top-level Embedding Atlas job orchestration."""

from __future__ import annotations

import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from fastapi import HTTPException

from .. import embedder_models
from ..config import ATLAS_PORT, ATLAS_TRUST_REMOTE_CODE, EMBEDDER_MODELS_DIR
from ..db import open_connection, quote_ident
from ..deleted_rows import deleted_row_ids_for
from ..embedder_capabilities import BackendName, ModelCapabilities, analyze_model_capabilities
from ..jobs import JobContext
from ..sql import configure_duckdb_limits, create_data_view, guard_select_sql_for_dataset
from .contracts import AtlasModality, AtlasOptions
from .dataset import model_label, prepare_atlas_dataset
from .images import IMAGE_COLUMN_HINTS, is_image_like_value
from .process import build_atlas_command, launch_embedding_atlas

ATLAS_PORT_LOCK = threading.Lock()
ATLAS_PORT_STATE = {"next": ATLAS_PORT}


def discover_embedder_models() -> list[dict[str, Any]]:
    """Return locally installed encoder models under models/embedder."""
    return embedder_models.discover_embedder_models(
        EMBEDDER_MODELS_DIR,
        allow_remote_code=ATLAS_TRUST_REMOTE_CODE,
    )


def resolve_embedder_model(model: str) -> Path:
    """Resolve a model dropdown value to a local model directory."""
    return embedder_models.resolve_embedder_model(model, EMBEDDER_MODELS_DIR)


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


def infer_atlas_modality(
    path: Path,
    column: str,
    sql: str | None = None,
    deleted_ids: list[int] | None = None,
) -> AtlasModality:
    """Infer whether Atlas should treat the selected column as image or text."""
    values = _sample_column_values(path, column, sql, deleted_ids or [])
    if any(is_image_like_value(value) for value in values):
        return "image"
    return "image" if any(hint in column.lower() for hint in IMAGE_COLUMN_HINTS) else "text"


def reserve_atlas_start_port(options: AtlasOptions) -> AtlasOptions:
    """Return options with a unique preferred port for concurrent Atlas jobs."""
    with ATLAS_PORT_LOCK:
        port = max(options.port, ATLAS_PORT_STATE["next"])
        ATLAS_PORT_STATE["next"] = port + 1
    return replace(options, port=port)


def run_atlas_visualization(
    *,
    file_name: str,
    path: Path,
    column: str,
    model: str,
    backend: str | None = None,
    prompt: str | None = None,
    sql: str | None,
    sample: int | None,
    context: JobContext,
) -> dict[str, Any]:
    """Launch Atlas for a selected dataset column or SQL query result."""
    selected_column = column.strip()
    if not selected_column:
        raise HTTPException(status_code=400, detail="column is required")
    model_path = resolve_embedder_model(model)
    base_options = AtlasOptions.from_request(sample=sample)
    capabilities = analyze_model_capabilities(
        model_path,
        allow_remote_code=base_options.trust_remote_code,
    )
    guarded_sql = guard_select_sql_for_dataset(path, sql) if sql else None
    deleted_ids = deleted_row_ids_for(path)
    context.update(progress=0.02, message="Inspecting selected column")
    modality = infer_atlas_modality(path, selected_column, guarded_sql, deleted_ids)
    selected_backend = _resolve_backend(backend, modality, capabilities, base_options)
    normalized_prompt = prompt if prompt and prompt.strip() else None
    if normalized_prompt and selected_backend != "sentence-transformers":
        raise HTTPException(status_code=400, detail="prompt is supported only by the sentence-transformers backend")
    options = reserve_atlas_start_port(
        replace(
            base_options,
            backend=selected_backend,
            prompt=normalized_prompt,
            capability_fingerprint=capabilities.fingerprint,
        )
    )
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
        "model": model_label(model_path),
        "backend": selected_backend,
        "prompt_applied": normalized_prompt is not None,
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


def _resolve_backend(
    requested: str | None,
    modality: AtlasModality,
    capabilities: ModelCapabilities,
    options: AtlasOptions,
) -> BackendName:
    selected = requested or capabilities.default_backend
    if selected not in {"transformers", "sentence-transformers"}:
        raise HTTPException(status_code=400, detail="no supported embedding backend is available for this model")
    backend = cast(BackendName, selected)
    capability = capabilities.backend(backend)
    if not capability.available:
        raise HTTPException(status_code=400, detail=f"{backend} is unavailable for this model: {capability.reason}")
    if modality not in capability.modalities:
        supported = ", ".join(capability.modalities) or "none"
        raise HTTPException(status_code=400, detail=f"{backend} does not support {modality} input for this model; supported: {supported}")
    return backend

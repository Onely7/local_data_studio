"""Embedding backend resolution and two-dimensional projection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import umap

from ..jobs import JobContext
from .contracts import AtlasModality, AtlasOptions, AtlasProjectionCoordinates
from .embedding_session import (
    AtlasEmbeddingSession,
    create_embedding_session,
    embedding_batch,
    embedding_sequence,
)
from .reducers import ATLAS_PROJECTION_RANDOM_STATE, ATLAS_UMAP_N_JOBS, Projection, reduce_embeddings, run_umap_projection

DEFAULT_EMBEDDING_BATCH_SIZE = 64


def _publish_progress(
    context: JobContext | None,
    *,
    progress: float,
    message: str,
) -> None:
    if context is None:
        return
    context.check_cancelled()
    context.update(progress=progress, message=message)


def _embed_in_batches(
    session: AtlasEmbeddingSession,
    items: Any,
    indices: Any,
    *,
    options: AtlasOptions,
    context: JobContext | None,
    progress_start: float,
    progress_end: float,
    message: str,
) -> np.ndarray:
    """Embed selected rows with cooperative cancellation between batches."""
    selected = np.asarray(indices, dtype=np.int64)
    if selected.size == 0:
        return session.embed([])
    batch_size = max(options.batch_size or DEFAULT_EMBEDDING_BATCH_SIZE, 1)
    embeddings: np.ndarray | None = None
    total = len(selected)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        _publish_progress(
            context,
            progress=progress_start + (progress_end - progress_start) * (start / total),
            message=f"{message}: {start:,}/{total:,} rows",
        )
        batch = np.asarray(session.embed(embedding_batch(items, selected[start:end])))
        expected_rows = end - start
        if batch.ndim != 2 or batch.shape[0] != expected_rows:
            raise ValueError(f"encoder returned shape {batch.shape}; expected ({expected_rows}, embedding_dimension)")
        if embeddings is None:
            embeddings = np.empty((total, batch.shape[1]), dtype=batch.dtype)
        elif batch.shape[1] != embeddings.shape[1]:
            raise ValueError(f"encoder embedding dimension changed from {embeddings.shape[1]} to {batch.shape[1]}")
        embeddings[start:end] = batch
    _publish_progress(context, progress=progress_end, message=f"{message}: {total:,}/{total:,} rows")
    if embeddings is None:
        return session.embed([])
    return embeddings


def run_full_projection(embeddings: np.ndarray) -> Projection:
    """Compatibility alias for deterministic full-fit UMAP."""
    return run_umap_projection(embeddings)


def compute_full_projection(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
    session: AtlasEmbeddingSession,
    context: JobContext | None = None,
) -> AtlasProjectionCoordinates:
    """Embed all input rows and return owned float32 projection coordinates."""
    items = embedding_sequence(projection_input, input_column)
    embeddings = _embed_in_batches(
        session,
        items,
        np.arange(len(items)),
        options=options,
        context=context,
        progress_start=0.22,
        progress_end=0.68,
        message="Creating embeddings",
    )
    _publish_progress(context, progress=0.72, message=f"Calculating {options.projection_method.upper()} projection")
    values = reduce_embeddings(embeddings, options.projection_method)
    _publish_progress(context, progress=0.84, message="Projection coordinates are ready")
    return AtlasProjectionCoordinates(values)


def anchor_indices(row_count: int, anchor_sample: int | None) -> np.ndarray:
    """Return deterministic sorted anchor indices without replacement."""
    anchor_count = min(anchor_sample or row_count, row_count)
    if anchor_count >= row_count:
        return np.arange(row_count)
    return np.sort(np.random.default_rng(42).choice(row_count, size=anchor_count, replace=False))


def compute_anchor_transform_projection(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
    session: AtlasEmbeddingSession,
    context: JobContext | None = None,
) -> AtlasProjectionCoordinates:
    """Fit UMAP on anchors and transform remaining rows into the same space."""
    items = embedding_sequence(projection_input, input_column)
    row_count = len(items)
    if row_count <= 1:
        return AtlasProjectionCoordinates(np.zeros((row_count, 2), dtype=np.float32))
    selected = anchor_indices(row_count, options.umap_anchor_sample)
    if len(selected) <= 1:
        return compute_full_projection(
            projection_input,
            input_column=input_column,
            modality=modality,
            model_path=model_path,
            options=options,
            session=session,
            context=context,
        )
    selected_set = set(selected.tolist())
    anchor_embeddings = _embed_in_batches(
        session,
        items,
        selected,
        options=options,
        context=context,
        progress_start=0.22,
        progress_end=0.52,
        message="Creating UMAP anchor embeddings",
    )
    _publish_progress(context, progress=0.56, message="Fitting UMAP anchor projection")
    reducer = umap.UMAP(
        metric="cosine",
        init="random",
        n_neighbors=min(15, max(2, len(selected) - 1)),
        random_state=ATLAS_PROJECTION_RANDOM_STATE,
        n_jobs=ATLAS_UMAP_N_JOBS,
    )
    values = np.empty((row_count, 2), dtype=np.float32)
    values[selected] = np.asarray(reducer.fit_transform(anchor_embeddings), dtype=np.float32)
    _publish_progress(context, progress=0.64, message="UMAP anchor projection is ready")
    transform_batch_size = max(options.batch_size or 256, 1)
    batch_indices: list[int] = []
    remaining_count = row_count - len(selected)
    transformed_count = 0
    for index in range(row_count):
        if index in selected_set:
            continue
        batch_indices.append(index)
        if len(batch_indices) < transform_batch_size:
            continue
        _publish_progress(
            context,
            progress=0.64 + 0.18 * (transformed_count / max(remaining_count, 1)),
            message=f"Projecting remaining rows: {transformed_count:,}/{remaining_count:,}",
        )
        batch_embeddings = session.embed(embedding_batch(items, batch_indices))
        values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)
        transformed_count += len(batch_indices)
        batch_indices = []
    if batch_indices:
        _publish_progress(
            context,
            progress=0.64 + 0.18 * (transformed_count / max(remaining_count, 1)),
            message=f"Projecting remaining rows: {transformed_count:,}/{remaining_count:,}",
        )
        batch_embeddings = session.embed(embedding_batch(items, batch_indices))
        values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)
        transformed_count += len(batch_indices)
    _publish_progress(context, progress=0.84, message=f"Projected {transformed_count:,} remaining rows")
    return AtlasProjectionCoordinates(values)


def project_atlas_frame(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
    context: JobContext | None = None,
) -> AtlasProjectionCoordinates:
    """Create one encoder session and project the selected input column."""
    _publish_progress(context, progress=0.18, message="Loading the encoder model")
    session = create_embedding_session(modality, model_path, options)
    _publish_progress(context, progress=0.20, message="Encoder model loaded")
    project = (
        compute_anchor_transform_projection
        if options.projection_method == "umap" and options.umap_projection_mode == "anchor_transform"
        else compute_full_projection
    )
    return project(
        projection_input,
        input_column=input_column,
        modality=modality,
        model_path=model_path,
        options=options,
        session=session,
        context=context,
    )

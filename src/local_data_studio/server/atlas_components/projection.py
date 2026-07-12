"""Embedding backend resolution and two-dimensional projection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import umap

from .contracts import AtlasModality, AtlasOptions, AtlasProjectionCoordinates
from .embedding_session import (
    AtlasEmbeddingSession,
    create_embedding_session,
    embed_items,
    embedding_batch,
    embedding_items,
    embedding_sequence,
)
from .reducers import ATLAS_PROJECTION_RANDOM_STATE, ATLAS_UMAP_N_JOBS, Projection, reduce_embeddings, run_umap_projection


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
) -> AtlasProjectionCoordinates:
    """Embed all input rows and return owned float32 projection coordinates."""
    embeddings = session.embed(embedding_items(projection_input, input_column))
    values = reduce_embeddings(embeddings, options.projection_method)
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
        )
    selected_set = set(selected.tolist())
    anchor_embeddings = session.embed(embedding_batch(items, selected))
    reducer = umap.UMAP(
        metric="cosine",
        init="random",
        n_neighbors=min(15, max(2, len(selected) - 1)),
        random_state=ATLAS_PROJECTION_RANDOM_STATE,
        n_jobs=ATLAS_UMAP_N_JOBS,
    )
    values = np.empty((row_count, 2), dtype=np.float32)
    values[selected] = np.asarray(reducer.fit_transform(anchor_embeddings), dtype=np.float32)
    transform_batch_size = max(options.batch_size or 256, 1)
    batch_indices: list[int] = []
    for index in range(row_count):
        if index in selected_set:
            continue
        batch_indices.append(index)
        if len(batch_indices) < transform_batch_size:
            continue
        batch_embeddings = session.embed(embedding_batch(items, batch_indices))
        values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)
        batch_indices = []
    if batch_indices:
        batch_embeddings = session.embed(embedding_batch(items, batch_indices))
        values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)
    return AtlasProjectionCoordinates(values)


def project_atlas_frame(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
) -> AtlasProjectionCoordinates:
    """Create one encoder session and project the selected input column."""
    session = create_embedding_session(modality, model_path, options)
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
    )

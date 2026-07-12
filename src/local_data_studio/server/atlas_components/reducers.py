"""Two-dimensional reducers used by Atlas projections."""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
from embedding_atlas.projection import Projection, _run_umap
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE

from .contracts import AtlasProjectionMethod

ATLAS_PROJECTION_RANDOM_STATE = 42
ATLAS_UMAP_N_JOBS = 1

__all__ = [
    "ATLAS_PROJECTION_RANDOM_STATE",
    "ATLAS_UMAP_N_JOBS",
    "Projection",
    "reduce_embeddings",
    "reduce_pca",
    "reduce_tsne",
    "reduce_umap",
    "run_umap_projection",
]


def zero_coordinates(row_count: int) -> np.ndarray:
    """Return two zero-valued coordinates for every input row."""
    return np.zeros((row_count, 2), dtype=np.float32)


def validate_coordinates(values: object, row_count: int) -> np.ndarray:
    """Return finite two-dimensional float32 coordinates.

    Raises:
        ValueError: The reducer returned an invalid shape or non-finite value.
    """
    coordinates = np.asarray(values, dtype=np.float32)
    if coordinates.shape != (row_count, 2):
        raise ValueError(f"projection returned shape {coordinates.shape}; expected {(row_count, 2)}")
    if not np.isfinite(coordinates).all():
        raise ValueError("projection returned non-finite coordinates")
    return coordinates


def run_umap_projection(embeddings: np.ndarray) -> Projection:
    """Run deterministic single-threaded UMAP over all embeddings."""
    if len(embeddings) <= 1:
        row_count = len(embeddings)
        return Projection(
            projection=zero_coordinates(row_count),
            knn_indices=np.zeros((row_count, 0), dtype=np.int64),
            knn_distances=np.zeros((row_count, 0), dtype=np.float32),
        )
    n_neighbors = min(15, max(2, len(embeddings) - 1))
    return _run_umap(
        embeddings,
        umap_args={
            "metric": "cosine",
            "init": "random",
            "n_neighbors": n_neighbors,
            "random_state": ATLAS_PROJECTION_RANDOM_STATE,
            "n_jobs": ATLAS_UMAP_N_JOBS,
        },
    )


def reduce_umap(embeddings: np.ndarray) -> np.ndarray:
    """Return full-fit UMAP coordinates."""
    return validate_coordinates(run_umap_projection(embeddings).projection, len(embeddings))


def reduce_tsne(embeddings: np.ndarray) -> np.ndarray:
    """Return deterministic cosine t-SNE coordinates."""
    row_count = len(embeddings)
    if row_count <= 1:
        return zero_coordinates(row_count)
    perplexity = float(min(30, max(1, row_count - 1)))
    values = TSNE(
        n_components=2,
        metric="cosine",
        perplexity=perplexity,
        learning_rate="auto",
        init="random",
        random_state=ATLAS_PROJECTION_RANDOM_STATE,
    ).fit_transform(np.asarray(embeddings, dtype=np.float32))
    return validate_coordinates(values, row_count)


def reduce_pca(embeddings: np.ndarray) -> np.ndarray:
    """Return PCA coordinates, padding unavailable components with zeros."""
    values = np.asarray(embeddings, dtype=np.float32)
    row_count = len(values)
    if row_count <= 1:
        return zero_coordinates(row_count)
    if values.ndim != 2 or values.shape[1] == 0:
        raise ValueError("PCA requires a non-empty two-dimensional embedding array")
    component_count = min(2, row_count, values.shape[1])
    reduced = PCA(n_components=component_count, svd_solver="auto", random_state=ATLAS_PROJECTION_RANDOM_STATE).fit_transform(values)
    coordinates = zero_coordinates(row_count)
    coordinates[:, :component_count] = reduced
    return validate_coordinates(coordinates, row_count)


FULL_REDUCERS: dict[AtlasProjectionMethod, Callable[[np.ndarray], np.ndarray]] = {
    "umap": reduce_umap,
    "tsne": reduce_tsne,
    "pca": reduce_pca,
}


def reduce_embeddings(embeddings: np.ndarray, method: AtlasProjectionMethod) -> np.ndarray:
    """Dispatch an embedding matrix to the selected full-fit reducer."""
    return FULL_REDUCERS[method](embeddings)

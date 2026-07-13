"""Immutable contracts shared by Atlas runtime components."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from ..config import (
    ATLAS_BATCH_SIZE,
    ATLAS_EMBEDDING_DTYPE,
    ATLAS_HOST,
    ATLAS_PORT,
    ATLAS_SAMPLE,
    ATLAS_TRUST_REMOTE_CODE,
    ATLAS_UMAP_ANCHOR_SAMPLE,
    ATLAS_UMAP_PROJECTION_MODE,
)

AtlasModality = Literal["text", "image", "vector"]
AtlasProjectionMethod = Literal["umap", "tsne", "pca"]
AtlasEmbeddingDtype = Literal["float16", "float32"]
AtlasUmapProjectionMode = Literal["full", "anchor_transform"]
AtlasBackend = Literal["transformers", "sentence-transformers"]
ATLAS_PROJECTION_X = "__local_data_studio_atlas_x"
ATLAS_PROJECTION_Y = "__local_data_studio_atlas_y"
ATLAS_PROJECTION_NEIGHBORS = "__local_data_studio_atlas_neighbors"
ATLAS_EMBED_INPUT_COLUMN = "__local_data_studio_atlas_embed_input"


@dataclass(frozen=True, slots=True)
class AtlasOptions:
    """Immutable embedding, projection, and child-process options.

    Instances are safe to share with worker threads. Paths and mutable model
    objects are deliberately excluded; callers retain ownership of those inputs.
    """

    sample: int | None
    host: str
    port: int
    batch_size: int | None
    text_embedder: str | None
    image_embedder: str | None
    trust_remote_code: bool
    embedding_dtype: AtlasEmbeddingDtype = "float32"
    projection_method: AtlasProjectionMethod = "umap"
    umap_projection_mode: AtlasUmapProjectionMode | None = "full"
    umap_anchor_sample: int | None = None
    backend: AtlasBackend | None = None
    prompt: str | None = None
    capability_fingerprint: str | None = None

    @classmethod
    def from_request(cls, sample: int | None = None, projection_method: AtlasProjectionMethod = "umap") -> AtlasOptions:
        """Resolve request sampling over environment-backed Atlas defaults.

        Non-positive sample and anchor values are represented as ``None``.
        """
        requested_sample = sample if sample is not None else ATLAS_SAMPLE
        anchor_sample = ATLAS_UMAP_ANCHOR_SAMPLE if ATLAS_UMAP_ANCHOR_SAMPLE > 0 else None
        umap_projection_mode = ATLAS_UMAP_PROJECTION_MODE if projection_method == "umap" else None
        return cls(
            sample=requested_sample if requested_sample and requested_sample > 0 else None,
            host=ATLAS_HOST,
            port=ATLAS_PORT,
            batch_size=ATLAS_BATCH_SIZE if ATLAS_BATCH_SIZE > 0 else None,
            text_embedder=None,
            image_embedder=None,
            trust_remote_code=ATLAS_TRUST_REMOTE_CODE,
            embedding_dtype=cast("AtlasEmbeddingDtype", ATLAS_EMBEDDING_DTYPE),
            projection_method=projection_method,
            umap_projection_mode=cast("AtlasUmapProjectionMode | None", umap_projection_mode),
            umap_anchor_sample=anchor_sample if projection_method == "umap" else None,
        )


@dataclass(frozen=True, slots=True)
class AtlasPreparedDataset:
    """Materialized Atlas cache owned by the application cache directory.

    ``path`` remains valid until cache eviction. Callers must not mutate or
    delete it and should use ``cache_hit`` only for user-facing status.
    """

    path: Path
    x: str
    y: str
    neighbors: str | None
    cache_hit: bool
    row_count: int


@dataclass(frozen=True, slots=True)
class AtlasProjectionCoordinates:
    """Owned two-dimensional projection coordinates.

    ``values`` is an owned finite float32 array with shape ``(rows, 2)``.
    """

    values: object

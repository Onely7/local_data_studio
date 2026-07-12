"""Reusable embedding sessions for Atlas projection jobs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from embedding_atlas.projection import _run_embedding

from .contracts import AtlasModality, AtlasOptions
from .embedding_backends import resolve_embedder_callable


def embedding_items(projection_input: Any, input_column: str) -> list[Any]:
    """Materialize one input column for full-projection compatibility."""
    try:
        return projection_input[input_column].tolist()
    except Exception as exc:
        raise ValueError(f"failed to read Atlas embedding input: {exc}") from exc


def embedding_sequence(projection_input: Any, input_column: str) -> Any:
    """Return an indexable input column without materializing it."""
    try:
        return projection_input[input_column]
    except Exception as exc:
        raise ValueError(f"failed to read Atlas embedding input: {exc}") from exc


def embedding_batch(items: Any, indices: Any) -> list[Any]:
    """Copy only the selected input rows into an owned embedding batch."""
    if hasattr(items, "iloc"):
        return [items.iloc[int(index)] for index in indices]
    return [items[int(index)] for index in indices]


def projection_dtype(options: AtlasOptions) -> Any:
    """Return the NumPy dtype selected for intermediate embeddings."""
    return np.float16 if options.embedding_dtype == "float16" else np.float32


def cast_embeddings(embeddings: np.ndarray, options: AtlasOptions) -> np.ndarray:
    """Return embeddings in the configured dtype, copying only as needed."""
    return np.asarray(embeddings, dtype=projection_dtype(options))


@dataclass(slots=True)
class AtlasEmbeddingSession:
    """One reusable encoder instance for all batches in an Atlas job."""

    modality: AtlasModality
    model_path: Path
    options: AtlasOptions
    embedder: Any = None
    embedder_args: dict[str, bool] | None = None

    def embed(self, items: list[Any]) -> np.ndarray:
        """Encode one owned batch with the session's reusable model callable."""
        if not items:
            return np.empty((0, 0), dtype=projection_dtype(self.options))
        if self.modality == "vector":
            return cast_embeddings(np.asarray(items), self.options)
        embeddings = asyncio.run(
            _run_embedding(
                self.embedder,
                items,
                model=str(self.model_path),
                embedder_args=self.embedder_args or {},
                batch_size=self.options.batch_size,
                max_concurrency=1,
            )
        )
        return cast_embeddings(embeddings, self.options)


def create_embedding_session(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> AtlasEmbeddingSession:
    """Create one encoder session to be reused for every projection batch."""
    if modality == "vector":
        return AtlasEmbeddingSession(modality, model_path, options)
    embedder, embedder_args = resolve_embedder_callable(modality, model_path, options)
    return AtlasEmbeddingSession(modality, model_path, options, embedder, embedder_args)


def embed_items(items: list[Any], *, modality: AtlasModality, model_path: Path, options: AtlasOptions) -> np.ndarray:
    """Compatibility helper for callers embedding a single batch."""
    return create_embedding_session(modality, model_path, options).embed(items)

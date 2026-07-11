"""Embedding backend resolution and two-dimensional projection."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
import umap
from embedding_atlas.embedding import create_embedder
from embedding_atlas.projection import Projection, _run_embedding, _run_umap
from PIL import Image
from sentence_transformers import SentenceTransformer

from .contracts import AtlasModality, AtlasOptions, AtlasProjectionCoordinates

QWEN3_VL_EMBEDDING_MARKER = "qwen3-vl-embedding"
ATLAS_UMAP_RANDOM_STATE = 42
ATLAS_UMAP_N_JOBS = 1


def _normalize_model_identity(value: str) -> str:
    return value.lower().replace("_", "-").replace("/", "-")


def is_qwen3_vl_embedding_model(model_path: Path) -> bool:
    """Detect Qwen3-VL embedding models from path and local config metadata."""
    normalized_path = _normalize_model_identity(model_path.as_posix())
    if QWEN3_VL_EMBEDDING_MARKER in normalized_path:
        return True
    try:
        config = json.loads((model_path / "config.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    candidates = [str(config.get("model_type", ""))]
    architectures = config.get("architectures")
    if isinstance(architectures, list):
        candidates.extend(str(item) for item in architectures)
    normalized_config = _normalize_model_identity(" ".join(candidates))
    return "qwen3-vl" in normalized_config and "embedding" in normalized_config


def atlas_embedder_for_modality(modality: AtlasModality, options: AtlasOptions) -> str | None:
    """Return the explicitly configured backend for text or image input."""
    return options.image_embedder if modality == "image" else options.text_embedder


def default_embedder_for_modality(modality: AtlasModality) -> str:
    """Return the Embedding Atlas default backend for a modality."""
    return "sentence-transformers" if modality == "text" else "transformers"


@dataclass(frozen=True, slots=True)
class AtlasEmbeddingBackend:
    """Resolved embedding backend policy for a selected model and modality."""

    name: str
    uses_qwen3_vl_adapter: bool = False

    @classmethod
    def for_model(
        cls,
        *,
        modality: AtlasModality,
        model_path: Path,
        options: AtlasOptions,
    ) -> AtlasEmbeddingBackend:
        """Resolve the backend and model-specific adapter without loading weights."""
        if is_qwen3_vl_embedding_model(model_path):
            return cls(name="sentence-transformers", uses_qwen3_vl_adapter=True)
        configured = atlas_embedder_for_modality(modality, options)
        return cls(name=configured or default_embedder_for_modality(modality))


def effective_embedder_for_modality(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> str:
    """Return the backend name included in the deterministic cache identity."""
    return AtlasEmbeddingBackend.for_model(modality=modality, model_path=model_path, options=options).name


def load_sentence_transformer_model(model_path: Path, options: AtlasOptions) -> Any:
    """Load one local SentenceTransformer, honoring remote-code consent."""
    kwargs = {"trust_remote_code": True} if options.trust_remote_code else {}
    return SentenceTransformer(str(model_path), **kwargs)


def _qwen3_vl_image_input(item: Any) -> Any:
    if isinstance(item, dict) and isinstance(item.get("bytes"), bytes | bytearray):
        return Image.open(BytesIO(bytes(item["bytes"]))).convert("RGB")
    if isinstance(item, dict) and item.get("image") is not None:
        return item["image"]
    return item


def _qwen3_vl_sentence_transformer_input(item: Any, modality: AtlasModality) -> Any:
    return {"image": _qwen3_vl_image_input(item)} if modality == "image" else item


def create_qwen3_vl_sentence_transformer_embedder(
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
) -> Any:
    """Create a Qwen3-VL adapter that owns one reusable model instance."""
    st_model = load_sentence_transformer_model(model_path, options)

    async def embed(batch: list[Any], *, model: str | None, embedder_args: dict) -> np.ndarray:  # noqa: ARG001
        """Encode one batch through the model captured by the adapter."""
        encoded_inputs = [_qwen3_vl_sentence_transformer_input(item, modality) for item in batch]
        embeddings = st_model.encode(
            encoded_inputs,
            show_progress_bar=False,
            batch_size=max(len(encoded_inputs), 1),
        )
        return np.asarray(embeddings, dtype=np.float32)

    return embed


def resolve_embedder_callable(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> tuple[Any, dict[str, bool]]:
    """Create one embedding callable and the arguments required by Atlas."""
    backend = AtlasEmbeddingBackend.for_model(modality=modality, model_path=model_path, options=options)
    embedder_args = {"trust_remote_code": True} if options.trust_remote_code else {}
    if backend.uses_qwen3_vl_adapter:
        return create_qwen3_vl_sentence_transformer_embedder(modality, model_path, options), embedder_args
    embedder = create_embedder(backend.name, modality=modality, model=str(model_path), embedder_args=embedder_args)
    return embedder, embedder_args


def embedding_items(projection_input: Any, input_column: str) -> list[Any]:
    """Materialize one input column for full-projection compatibility.

    Raises:
        ValueError: The frame does not expose the requested column as a sequence.
    """
    try:
        return projection_input[input_column].tolist()
    except Exception as exc:
        raise ValueError(f"failed to read Atlas embedding input: {exc}") from exc


def projection_dtype(options: AtlasOptions) -> Any:
    """Return the NumPy dtype selected for intermediate embeddings."""
    return np.float16 if options.embedding_dtype == "float16" else np.float32


def cast_embeddings(embeddings: np.ndarray, options: AtlasOptions) -> np.ndarray:
    """Return an array in the configured embedding dtype, copying only as needed."""
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


def zero_projection(row_count: int) -> Projection:
    """Create zero coordinates and empty neighbors for zero or one input row."""
    return Projection(
        projection=np.zeros((row_count, 2), dtype=np.float32),
        knn_indices=np.zeros((row_count, 0), dtype=np.int64),
        knn_distances=np.zeros((row_count, 0), dtype=np.float32),
    )


def run_full_projection(embeddings: np.ndarray) -> Projection:
    """Run deterministic single-threaded UMAP over all embeddings."""
    if len(embeddings) <= 1:
        return zero_projection(len(embeddings))
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
    result = run_full_projection(embeddings)
    return AtlasProjectionCoordinates(np.asarray(result.projection, dtype=np.float32))


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
    items = embedding_items(projection_input, input_column)
    row_count = len(items)
    if row_count <= 1:
        return AtlasProjectionCoordinates(np.zeros((row_count, 2), dtype=np.float32))
    selected = anchor_indices(row_count, options.anchor_sample)
    selected_set = set(selected.tolist())
    anchor_embeddings = session.embed([items[index] for index in selected])
    reducer = umap.UMAP(
        metric="cosine",
        n_neighbors=min(15, max(2, len(selected) - 1)),
        random_state=ATLAS_UMAP_RANDOM_STATE,
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
        batch_embeddings = session.embed([items[item_index] for item_index in batch_indices])
        values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)
        batch_indices = []
    if batch_indices:
        batch_embeddings = session.embed([items[item_index] for item_index in batch_indices])
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
    project = compute_anchor_transform_projection if options.projection_mode == "anchor_transform" else compute_full_projection
    return project(
        projection_input,
        input_column=input_column,
        modality=modality,
        model_path=model_path,
        options=options,
        session=session,
    )

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

from .contracts import (
    ATLAS_PROJECTION_NEIGHBORS,
    ATLAS_PROJECTION_X,
    ATLAS_PROJECTION_Y,
    AtlasModality,
    AtlasOptions,
)

QWEN3_VL_EMBEDDING_MARKER = "qwen3-vl-embedding"
ATLAS_UMAP_RANDOM_STATE = 42
ATLAS_UMAP_N_JOBS = 1


def _normalize_model_identity(value: str) -> str:
    return value.lower().replace("_", "-").replace("/", "-")


def is_qwen3_vl_embedding_model(model_path: Path) -> bool:
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
    return options.image_embedder if modality == "image" else options.text_embedder


def default_embedder_for_modality(modality: AtlasModality) -> str:
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
        if is_qwen3_vl_embedding_model(model_path):
            return cls(name="sentence-transformers", uses_qwen3_vl_adapter=True)
        configured = atlas_embedder_for_modality(modality, options)
        return cls(name=configured or default_embedder_for_modality(modality))


def effective_embedder_for_modality(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> str:
    return AtlasEmbeddingBackend.for_model(modality=modality, model_path=model_path, options=options).name


def load_sentence_transformer_model(model_path: Path, options: AtlasOptions) -> Any:
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
    st_model = load_sentence_transformer_model(model_path, options)

    async def embed(batch: list[Any], *, model: str | None, embedder_args: dict) -> np.ndarray:  # noqa: ARG001
        encoded_inputs = [_qwen3_vl_sentence_transformer_input(item, modality) for item in batch]
        embeddings = st_model.encode(
            encoded_inputs,
            show_progress_bar=False,
            batch_size=max(len(encoded_inputs), 1),
        )
        return np.asarray(embeddings, dtype=np.float32)

    return embed


def resolve_embedder_callable(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> tuple[Any, dict[str, bool]]:
    backend = AtlasEmbeddingBackend.for_model(modality=modality, model_path=model_path, options=options)
    embedder_args = {"trust_remote_code": True} if options.trust_remote_code else {}
    if backend.uses_qwen3_vl_adapter:
        return create_qwen3_vl_sentence_transformer_embedder(modality, model_path, options), embedder_args
    embedder = create_embedder(backend.name, modality=modality, model=str(model_path), embedder_args=embedder_args)
    return embedder, embedder_args


def embedding_items(projection_input: Any, input_column: str) -> list[Any]:
    try:
        return projection_input[input_column].tolist()
    except Exception as exc:
        raise ValueError(f"failed to read Atlas embedding input: {exc}") from exc


def projection_dtype(options: AtlasOptions) -> Any:
    return np.float16 if options.embedding_dtype == "float16" else np.float32


def cast_embeddings(embeddings: np.ndarray, options: AtlasOptions) -> np.ndarray:
    return np.asarray(embeddings, dtype=projection_dtype(options))


def embed_items(items: list[Any], *, modality: AtlasModality, model_path: Path, options: AtlasOptions) -> np.ndarray:
    if not items:
        return np.empty((0, 0), dtype=projection_dtype(options))
    if modality == "vector":
        return cast_embeddings(np.asarray(items), options)
    embedder, embedder_args = resolve_embedder_callable(modality, model_path, options)
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
    return cast_embeddings(embeddings, options)


def zero_projection(row_count: int) -> Projection:
    return Projection(
        projection=np.zeros((row_count, 2), dtype=np.float32),
        knn_indices=np.zeros((row_count, 0), dtype=np.int64),
        knn_distances=np.zeros((row_count, 0), dtype=np.float32),
    )


def run_full_projection(embeddings: np.ndarray) -> Projection:
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
) -> Any:
    embeddings = embed_items(
        embedding_items(projection_input, input_column),
        modality=modality,
        model_path=model_path,
        options=options,
    )
    result = run_full_projection(embeddings)
    output = projection_input.copy()
    output[ATLAS_PROJECTION_X] = result.projection[:, 0].tolist()
    output[ATLAS_PROJECTION_Y] = result.projection[:, 1].tolist()
    output[ATLAS_PROJECTION_NEIGHBORS] = [
        {"ids": np.asarray(ids, dtype=np.int64).tolist(), "distances": np.asarray(distances, dtype=float).tolist()}
        for ids, distances in zip(result.knn_indices, result.knn_distances)
    ]
    return output


def anchor_indices(row_count: int, anchor_sample: int | None) -> np.ndarray:
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
) -> Any:
    items = embedding_items(projection_input, input_column)
    row_count = len(items)
    if row_count <= 1:
        output = projection_input.copy()
        output[ATLAS_PROJECTION_X] = [0.0] * row_count
        output[ATLAS_PROJECTION_Y] = [0.0] * row_count
        return output
    selected = anchor_indices(row_count, options.anchor_sample)
    selected_set = set(selected.tolist())
    anchor_embeddings = embed_items(
        [items[index] for index in selected],
        modality=modality,
        model_path=model_path,
        options=options,
    )
    reducer = umap.UMAP(
        metric="cosine",
        n_neighbors=min(15, max(2, len(selected) - 1)),
        random_state=ATLAS_UMAP_RANDOM_STATE,
        n_jobs=ATLAS_UMAP_N_JOBS,
    )
    values = np.empty((row_count, 2), dtype=np.float32)
    values[selected] = np.asarray(reducer.fit_transform(anchor_embeddings), dtype=np.float32)
    transform_batch_size = max(options.batch_size or 256, 1)
    remaining = [index for index in range(row_count) if index not in selected_set]
    for start in range(0, len(remaining), transform_batch_size):
        batch_indices = remaining[start : start + transform_batch_size]
        batch_embeddings = embed_items(
            [items[index] for index in batch_indices],
            modality=modality,
            model_path=model_path,
            options=options,
        )
        values[batch_indices] = np.asarray(reducer.transform(batch_embeddings), dtype=np.float32)
    output = projection_input.copy()
    output[ATLAS_PROJECTION_X] = values[:, 0].tolist()
    output[ATLAS_PROJECTION_Y] = values[:, 1].tolist()
    return output


def project_atlas_frame(
    projection_input: Any,
    *,
    input_column: str,
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
) -> Any:
    project = compute_anchor_transform_projection if options.projection_mode == "anchor_transform" else compute_full_projection
    return project(
        projection_input,
        input_column=input_column,
        modality=modality,
        model_path=model_path,
        options=options,
    )

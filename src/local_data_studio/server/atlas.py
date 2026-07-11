"""Compatibility facade for Embedding Atlas runtime components."""

from .atlas_components.contracts import (
    ATLAS_EMBED_INPUT_COLUMN,
    ATLAS_PROJECTION_NEIGHBORS,
    ATLAS_PROJECTION_X,
    ATLAS_PROJECTION_Y,
    AtlasModality,
    AtlasOptions,
    AtlasPreparedDataset,
)
from .atlas_components.dataset import atlas_dataset_cache_path, prepare_atlas_dataset
from .atlas_components.embedding_backends import (
    AtlasEmbeddingBackend,
)
from .atlas_components.embedding_backends import (
    effective_embedder_for_modality as _effective_embedder_for_modality,
)
from .atlas_components.embedding_backends import (
    load_sentence_transformer_model as _load_sentence_transformer_model,
)
from .atlas_components.embedding_backends import (
    resolve_embedder_callable as _resolve_embedder_callable,
)
from .atlas_components.images import ATLAS_TRUNCATION_SUFFIX
from .atlas_components.images import (
    image_value_to_bytes as _image_value_to_bytes,
)
from .atlas_components.images import (
    is_image_like_value as _is_image_like_value,
)
from .atlas_components.images import (
    normalize_image_display_value as _normalize_image_display_value,
)
from .atlas_components.images import (
    prepare_image_projection_input as _prepare_image_projection_input,
)
from .atlas_components.images import (
    read_url_bytes as _read_url_bytes,
)
from .atlas_components.process import build_atlas_command, launch_embedding_atlas
from .atlas_components.process import (
    embedding_atlas_env as _embedding_atlas_env,
)
from .atlas_components.process import (
    normalize_atlas_url as _normalize_atlas_url,
)
from .atlas_components.process import (
    spawn_embedding_atlas as _spawn_embedding_atlas,
)
from .atlas_components.projection import (
    compute_anchor_transform_projection as _compute_anchor_transform_projection,
)
from .atlas_components.projection import (
    compute_full_projection as _compute_full_projection,
)
from .atlas_components.projection import (
    embed_items as _embed_items,
)
from .atlas_components.projection import (
    project_atlas_frame,
)
from .atlas_components.projection import (
    run_full_projection as _run_full_projection,
)
from .atlas_components.service import (
    discover_embedder_models,
    infer_atlas_modality,
    reserve_atlas_start_port,
    resolve_embedder_model,
    run_atlas_visualization,
)

__all__ = [
    "ATLAS_EMBED_INPUT_COLUMN",
    "ATLAS_PROJECTION_NEIGHBORS",
    "ATLAS_PROJECTION_X",
    "ATLAS_PROJECTION_Y",
    "ATLAS_TRUNCATION_SUFFIX",
    "AtlasEmbeddingBackend",
    "AtlasModality",
    "AtlasOptions",
    "AtlasPreparedDataset",
    "atlas_dataset_cache_path",
    "build_atlas_command",
    "discover_embedder_models",
    "infer_atlas_modality",
    "launch_embedding_atlas",
    "prepare_atlas_dataset",
    "project_atlas_frame",
    "reserve_atlas_start_port",
    "resolve_embedder_model",
    "run_atlas_visualization",
]

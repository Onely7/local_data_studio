"""Compatibility facade for Atlas image and display-value helpers."""

from __future__ import annotations

from .image_values import (
    ATLAS_IMAGE_FETCH_RETRIES,
    ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS,
    ATLAS_IMAGE_MAX_BYTES,
    ATLAS_TRUNCATION_SUFFIX,
    IMAGE_BASE64_PREFIXES,
    IMAGE_COLUMN_HINTS,
    IMAGE_HEX_PREFIXES,
    IMAGE_REFERENCE_PATTERN,
    decode_data_image,
    decode_image_bytes_string,
    image_value_to_bytes,
    is_image_bytes,
    is_image_bytes_string,
    is_image_like_value,
    is_image_reference,
    read_url_bytes,
)
from .output_frames import (
    attach_projection_columns,
    build_atlas_output_frame,
    drop_atlas_embed_input,
    image_like_columns,
    normalize_image_display_columns,
    normalize_image_display_value,
    sanitize_atlas_cell,
    sanitize_atlas_output_frame,
)
from .projection_inputs import prepare_image_projection_input, prepare_projection_input, text_for_embedding

__all__ = [
    "ATLAS_IMAGE_FETCH_RETRIES",
    "ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS",
    "ATLAS_IMAGE_MAX_BYTES",
    "ATLAS_TRUNCATION_SUFFIX",
    "IMAGE_BASE64_PREFIXES",
    "IMAGE_COLUMN_HINTS",
    "IMAGE_HEX_PREFIXES",
    "IMAGE_REFERENCE_PATTERN",
    "attach_projection_columns",
    "build_atlas_output_frame",
    "decode_data_image",
    "decode_image_bytes_string",
    "drop_atlas_embed_input",
    "image_like_columns",
    "image_value_to_bytes",
    "is_image_bytes",
    "is_image_bytes_string",
    "is_image_like_value",
    "is_image_reference",
    "normalize_image_display_columns",
    "normalize_image_display_value",
    "prepare_image_projection_input",
    "prepare_projection_input",
    "read_url_bytes",
    "sanitize_atlas_cell",
    "sanitize_atlas_output_frame",
    "text_for_embedding",
]

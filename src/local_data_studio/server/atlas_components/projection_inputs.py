"""Encoder-only Atlas projection input construction."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ..config import ATLAS_TEXT_MAX_CHARS
from .contracts import ATLAS_EMBED_INPUT_COLUMN, AtlasModality
from .image_values import image_value_to_bytes
from .prompts import CompiledPromptTemplate, PromptedEmbeddingValue


def text_for_embedding(value: Any) -> str:
    """Convert a text value to a bounded deterministic encoder string."""
    text = "null" if value is None else str(value)
    return text[:ATLAS_TEXT_MAX_CHARS] if ATLAS_TEXT_MAX_CHARS and len(text) > ATLAS_TEXT_MAX_CHARS else text


def prepare_image_projection_input(
    data_frame: Any,
    *,
    column: str,
    dataset_path: Path,
    prompt_template: CompiledPromptTemplate | None = None,
) -> tuple[Any, Any]:
    """Build encoder-only image bytes while preserving original display values.

    Rows that cannot be decoded are excluded from both returned frames so
    projection coordinates remain aligned with displayed rows.
    """
    try:
        values = data_frame[column]
    except Exception as exc:
        raise ValueError(f"failed to read image column {column}: {exc}") from exc
    kept_indices: list[int] = []
    embedding_items: list[Any] = []
    first_error: str | None = None
    for index, value in enumerate(values):
        try:
            image_bytes = image_value_to_bytes(value, dataset_path)
        except ValueError as exc:
            if first_error is None:
                first_error = f"row {index + 1}: {exc}"
            continue
        kept_indices.append(index)
        embedding_value: Any = {"bytes": image_bytes}
        if prompt_template is not None:
            prompt = prompt_template.render(data_frame.iloc[index].to_dict())
            embedding_value = PromptedEmbeddingValue(embedding_value, prompt)
        embedding_items.append(embedding_value)
    if not kept_indices:
        raise ValueError(f"no readable images in column {column}; first error: {first_error or 'no image values found'}")
    output_frame = data_frame.iloc[kept_indices].copy().reset_index(drop=True)
    return pd.DataFrame({ATLAS_EMBED_INPUT_COLUMN: embedding_items}), output_frame


def prepare_projection_input(
    data_frame: Any,
    *,
    column: str,
    modality: AtlasModality,
    dataset_path: Path,
    prompt_template: CompiledPromptTemplate | None = None,
) -> tuple[Any, str, Any]:
    """Separate encoder input from an Atlas display frame with aligned rows."""
    if modality == "text":
        if prompt_template is None:
            values = data_frame[column].map(text_for_embedding)
        elif prompt_template.has_placeholders:
            values = data_frame.apply(
                lambda row: PromptedEmbeddingValue(prompt_template.render(row.to_dict()), ""),
                axis=1,
            )
        else:
            prompt = prompt_template.render({})
            values = data_frame[column].map(lambda value: PromptedEmbeddingValue(text_for_embedding(value), prompt))
        frame = pd.DataFrame({ATLAS_EMBED_INPUT_COLUMN: values})
        return frame, ATLAS_EMBED_INPUT_COLUMN, data_frame
    if modality != "image":
        return data_frame, column, data_frame
    frame, output = prepare_image_projection_input(
        data_frame,
        column=column,
        dataset_path=dataset_path,
        prompt_template=prompt_template,
    )
    return frame, ATLAS_EMBED_INPUT_COLUMN, output

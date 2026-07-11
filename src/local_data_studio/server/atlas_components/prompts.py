"""Safe row-aware prompt templates for Sentence Transformers embedding."""

from __future__ import annotations

import json
from dataclasses import dataclass
from string import Formatter
from typing import Any


class PromptTemplateError(ValueError):
    """Raised before model loading when a prompt template is invalid."""


@dataclass(frozen=True, slots=True)
class PromptSegment:
    """One literal segment followed by an optional exact column reference."""

    literal: str
    column: str | None


@dataclass(frozen=True, slots=True)
class CompiledPromptTemplate:
    """Validated prompt segments and referenced source columns."""

    segments: tuple[PromptSegment, ...]
    columns: tuple[str, ...]
    max_chars: int

    @property
    def has_placeholders(self) -> bool:
        """Return whether at least one row column is referenced."""
        return bool(self.columns)

    def render(self, row: dict[str, Any]) -> str:
        """Render one row without attribute access or format evaluation."""
        parts: list[str] = []
        for segment in self.segments:
            parts.append(segment.literal)
            if segment.column is not None:
                parts.append(_prompt_value(row.get(segment.column)))
        rendered = "".join(parts)
        return rendered[: self.max_chars] if self.max_chars > 0 else rendered


@dataclass(frozen=True, slots=True)
class PromptedEmbeddingValue:
    """An encoder value paired with an explicit per-row prompt override."""

    value: Any
    prompt: str | None


def _prompt_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    if isinstance(value, bytes | bytearray):
        return f"<binary {len(value)} bytes>"
    if isinstance(value, dict | list | tuple):
        try:
            return json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def compile_prompt_template(template: str, available_columns: list[str], max_chars: int) -> CompiledPromptTemplate:
    """Validate braces and exact column placeholders without evaluating fields.

    Raises:
        PromptTemplateError: Braces are malformed, a referenced column is absent,
            or conversion and format syntax is requested.
    """
    available = set(available_columns)
    segments: list[PromptSegment] = []
    referenced: list[str] = []
    try:
        parsed = list(Formatter().parse(template))
    except ValueError as exc:
        raise PromptTemplateError(f"invalid prompt braces: {exc}") from exc
    for literal, field_name, format_spec, conversion in parsed:
        if conversion is not None or format_spec:
            raise PromptTemplateError("prompt placeholders do not support conversions or format specifiers")
        if field_name is not None:
            if field_name not in available:
                columns = ", ".join(available_columns) or "none"
                raise PromptTemplateError(f"unknown prompt column {{{field_name}}}; available columns: {columns}")
            referenced.append(field_name)
        segments.append(PromptSegment(literal, field_name))
    return CompiledPromptTemplate(tuple(segments), tuple(dict.fromkeys(referenced)), max_chars)

"""Compatibility facade for provider-neutral SQL generation."""

from __future__ import annotations

from typing import Any

from .llm_profiles import LLM_PROFILES
from .llm_prompt import clean_generated_sql as _clean_sql
from .llm_service import SqlGenerationResult, generate_sql_request


def generate_sql_from_prompt(
    prompt: str,
    schema: list[dict[str, str]],
    sample: dict[str, Any] | None,
    model_id: str | None = None,
) -> str:
    """Generate validated SQL using a configured profile.

    This facade preserves the original string return value for Python callers.
    New API code should use ``generate_sql_request`` to retain model metadata.
    """
    return generate_sql_request(prompt, schema, sample, model_id).sql


def public_llm_models() -> dict[str, Any]:
    """Return browser-safe model selector data."""
    return {
        "models": LLM_PROFILES.public_models(),
        "default_model": LLM_PROFILES.effective_default_model,
    }


__all__ = [
    "SqlGenerationResult",
    "_clean_sql",
    "generate_sql_from_prompt",
    "generate_sql_request",
    "public_llm_models",
]

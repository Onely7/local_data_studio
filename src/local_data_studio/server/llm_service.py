"""SQL generation orchestration across configured LiteLLM providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .llm_client import complete_text
from .llm_profiles import LLM_PROFILES, LlmProfileRegistry
from .llm_prompt import build_sql_generation_messages, clean_generated_sql


@dataclass(frozen=True, slots=True)
class SqlGenerationResult:
    """Validated SQL and the server-side profile that generated it."""

    sql: str
    model_id: str
    model_label: str


def generate_sql_request(
    prompt: str,
    schema: list[dict[str, str]],
    sample: dict[str, Any] | None,
    model_id: str | None = None,
    *,
    registry: LlmProfileRegistry | None = None,
) -> SqlGenerationResult:
    """Resolve a profile, call LiteLLM, and validate its SQL text output."""
    selected_registry = registry or LLM_PROFILES
    selection = selected_registry.selection(model_id)
    messages = build_sql_generation_messages(prompt, schema, sample)
    text = complete_text(selection, messages, default_timeout_seconds=selected_registry.timeout_seconds)
    return SqlGenerationResult(
        sql=clean_generated_sql(text),
        model_id=selection.id,
        model_label=selection.label,
    )

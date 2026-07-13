"""Lazy LiteLLM adapter for non-streaming text operations."""

from __future__ import annotations

import copy
import importlib
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from .llm_profiles import LlmModelSelection


def _load_litellm() -> Any:
    """Import LiteLLM only when SQL generation is requested."""
    return importlib.import_module("litellm")


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, Mapping) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif isinstance(getattr(item, "text", None), str):
                parts.append(item.text)
        return "".join(parts)
    return ""


def extract_completion_text(response: Any) -> str:
    """Extract assistant text from LiteLLM object or dictionary responses."""
    if isinstance(response, Mapping):
        choices = response.get("choices")
        if isinstance(choices, list) and choices:
            message = choices[0].get("message") if isinstance(choices[0], Mapping) else None
            if isinstance(message, Mapping):
                return _content_text(message.get("content"))
        return ""
    choices = getattr(response, "choices", None)
    if not choices:
        return ""
    message = getattr(choices[0], "message", None)
    return _content_text(getattr(message, "content", None))


def _provider_failure(exc: Exception, litellm: Any, selection: LlmModelSelection, operation: str) -> HTTPException:
    timeout_type = getattr(litellm, "Timeout", ())
    if timeout_type and isinstance(exc, timeout_type):
        return HTTPException(status_code=504, detail=f"{operation} timed out for {selection.label}")
    return HTTPException(status_code=502, detail=f"{operation} provider failed for {selection.label}")


def complete_text(
    selection: LlmModelSelection,
    messages: list[dict[str, str]],
    *,
    default_timeout_seconds: float,
    operation: str = "SQL generation",
) -> str:
    """Run one non-streaming text completion through a validated model selection.

    Raises:
        HTTPException: LiteLLM times out, the provider fails, or no assistant
            text is returned. Provider exception details are intentionally hidden.
    """
    litellm = _load_litellm()
    profile = selection.profile
    request: dict[str, Any] = copy.deepcopy(profile.provider_options)
    request.update(
        {
            "model": selection.model,
            "messages": messages,
            "timeout": profile.timeout_seconds or default_timeout_seconds,
        }
    )
    api_key = profile.api_key()
    if api_key is not None:
        request["api_key"] = api_key
    if profile.base_url is not None:
        request["base_url"] = profile.base_url
    try:
        response = litellm.completion(**request)
    except Exception as exc:
        raise _provider_failure(exc, litellm, selection, operation) from exc
    text = extract_completion_text(response).strip()
    if not text:
        raise HTTPException(status_code=502, detail=f"{operation} provider returned no text for {selection.label}")
    return text

"""Validated server-side model profiles for SQL generation."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from dotenv import dotenv_values
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from ..runtime_config import config_section, read_runtime_config
from .config import ENV_FILE

PROFILE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FORBIDDEN_PROVIDER_OPTIONS = {
    "api_base",
    "api_key",
    "audio",
    "base_url",
    "callbacks",
    "failure_callback",
    "function_call",
    "functions",
    "logger_fn",
    "messages",
    "modalities",
    "model",
    "prediction",
    "response_format",
    "stream",
    "stream_options",
    "structured_outputs",
    "success_callback",
    "timeout",
    "tool_choice",
    "tools",
}


def _forbidden_provider_option(value: JsonValue, path: tuple[str, ...] = ()) -> str | None:
    if isinstance(value, dict):
        for key, nested in value.items():
            current = (*path, key)
            if key in FORBIDDEN_PROVIDER_OPTIONS:
                return ".".join(current)
            found = _forbidden_provider_option(nested, current)
            if found:
                return found
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            found = _forbidden_provider_option(nested, (*path, str(index)))
            if found:
                return found
    return None


class LlmModelProfile(BaseModel):
    """One selectable LiteLLM model without resolved credential values."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    label: str
    model: str
    base_url: str | None = None
    api_key_env: str | None = None
    timeout_seconds: float | None = Field(default=None, gt=0)
    provider_options: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, value: Any) -> str:
        """Require a short stable identifier suitable for API requests."""
        normalized = str(value or "").strip()
        if not PROFILE_ID_PATTERN.fullmatch(normalized):
            raise ValueError("id must contain only letters, numbers, dots, underscores, or hyphens")
        return normalized

    @field_validator("label", mode="before")
    @classmethod
    def validate_label(cls, value: Any) -> str:
        """Require a non-empty UI label."""
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("label must not be empty")
        return normalized

    @field_validator("model", mode="before")
    @classmethod
    def validate_model(cls, value: Any) -> str:
        """Require an explicit LiteLLM provider prefix and current vLLM name."""
        normalized = str(value or "").strip()
        if "/" not in normalized or normalized.startswith("/") or normalized.endswith("/"):
            raise ValueError("model must include an explicit LiteLLM provider prefix")
        if normalized.startswith("vllm/"):
            raise ValueError("vllm/ is not supported; use hosted_vllm/")
        return normalized

    @field_validator("base_url", mode="before")
    @classmethod
    def validate_base_url(cls, value: Any) -> str | None:
        """Accept an optional HTTP(S) provider endpoint."""
        if value is None or not str(value).strip():
            return None
        normalized = str(value).strip().rstrip("/")
        parsed = urlsplit(normalized)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be an absolute HTTP or HTTPS URL")
        return normalized

    @field_validator("api_key_env", mode="before")
    @classmethod
    def validate_api_key_env(cls, value: Any) -> str | None:
        """Accept an optional environment-variable name, never a credential."""
        if value is None or not str(value).strip():
            return None
        normalized = str(value).strip()
        if not ENV_NAME_PATTERN.fullmatch(normalized):
            raise ValueError("api_key_env must be a valid environment-variable name")
        return normalized

    @field_validator("provider_options")
    @classmethod
    def validate_provider_options(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Reject options that can replace the common text-completion contract."""
        forbidden = _forbidden_provider_option(value)
        if forbidden:
            raise ValueError(f"provider_options must not set {forbidden}")
        return value

    @property
    def provider(self) -> str:
        """Return the explicit LiteLLM provider prefix."""
        return self.model.split("/", 1)[0]

    def api_key(self) -> str | None:
        """Resolve the configured credential from the OS environment or .env."""
        if not self.api_key_env:
            return None
        value = os.environ.get(self.api_key_env)
        if value is None and ENV_FILE.is_file():
            value = dotenv_values(ENV_FILE).get(self.api_key_env)
        return value.strip() if value and value.strip() else None


class LlmProfileRegistry(BaseModel):
    """Validated collection of selectable SQL-generation models."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    default_model: str | None = None
    timeout_seconds: float = Field(default=60.0, gt=0)
    models: tuple[LlmModelProfile, ...] = ()

    @model_validator(mode="after")
    def validate_registry(self) -> LlmProfileRegistry:
        """Require unique IDs and a default that references an existing profile."""
        ids = [profile.id for profile in self.models]
        if len(ids) != len(set(ids)):
            raise ValueError("llm model profile ids must be unique")
        if self.default_model and self.default_model not in ids:
            raise ValueError("llm.default_model must reference a configured model profile")
        return self

    @property
    def effective_default_model(self) -> str | None:
        """Return the explicit default or the first configured profile ID."""
        if self.default_model:
            return self.default_model
        return self.models[0].id if self.models else None

    def profile(self, profile_id: str | None) -> LlmModelProfile:
        """Resolve a requested profile and enforce configured credential presence.

        Raises:
            HTTPException: No model exists, the ID is unknown, or its explicit
                credential environment variable is unset.
        """
        selected = profile_id or self.effective_default_model
        if selected is None:
            raise HTTPException(status_code=503, detail="no SQL generation model is configured")
        profile = next((item for item in self.models if item.id == selected), None)
        if profile is None:
            raise HTTPException(status_code=400, detail="unknown SQL generation model")
        if profile.api_key_env and profile.api_key() is None:
            raise HTTPException(status_code=503, detail=f"credential environment variable for {profile.label} is not set")
        return profile

    def public_models(self) -> list[dict[str, Any]]:
        """Return browser-safe model metadata without endpoints or options."""
        default = self.effective_default_model
        return [
            {
                "id": profile.id,
                "label": profile.label,
                "provider": profile.provider,
                "available": not profile.api_key_env or profile.api_key() is not None,
                "reason": "" if not profile.api_key_env or profile.api_key() is not None else "credential is not configured",
                "default": profile.id == default,
            }
            for profile in self.models
        ]


def load_llm_profile_registry(path: str | None = None) -> LlmProfileRegistry:
    """Load the optional ``[llm]`` section from runtime TOML configuration."""
    config, _ = read_runtime_config(path)
    section: Mapping[str, Any] = config_section(config, "llm")
    return LlmProfileRegistry.model_validate(dict(section)) if section else LlmProfileRegistry()


LLM_PROFILES = load_llm_profile_registry()

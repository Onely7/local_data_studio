"""Validated speech-recognition models and limits for audio transcription."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..runtime_config import config_section, read_runtime_config
from .config import BASE_DIR

MODEL_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
DEVICE_PATTERN = re.compile(r"^(?:auto|cpu|cuda(?::\d+)?)$")


class TranscriptionModelProfile(BaseModel):
    """One speech-recognition model the browser may select."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    label: str | None = None
    model: str = Field(min_length=1)
    device: str = "auto"
    language: str | None = None
    max_new_tokens: int | None = Field(default=None, ge=1)

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        """Keep model IDs short and URL-safe."""
        if not MODEL_ID_PATTERN.fullmatch(value):
            raise ValueError("id must be 1-64 letters, digits, '.', '_' or '-'")
        return value

    @field_validator("device")
    @classmethod
    def validate_device(cls, value: str) -> str:
        """Accept auto, cpu, cuda, or cuda:N."""
        device = value.strip().lower()
        if not DEVICE_PATTERN.fullmatch(device):
            raise ValueError('device must be "auto", "cpu", "cuda", or "cuda:N"')
        return device

    @field_validator("language")
    @classmethod
    def normalize_language(cls, value: str | None) -> str | None:
        """Treat blank and "auto" as automatic language detection."""
        text = (value or "").strip()
        return None if not text or text.lower() == "auto" else text

    @property
    def display_label(self) -> str:
        """Return the label shown in the browser."""
        return self.label or self.id

    def model_location(self, base_dir: Path = BASE_DIR) -> Path | str:
        """Return the local model directory, or the Hugging Face model ID.

        Relative paths resolve from the workspace directory. A value that names
        no local directory is treated as a Hugging Face model ID.
        """
        candidate = Path(self.model).expanduser()
        if not candidate.is_absolute():
            candidate = base_dir / candidate
        return candidate.resolve() if candidate.is_dir() else self.model

    def availability(self, base_dir: Path = BASE_DIR) -> tuple[bool, str]:
        """Return whether the model can be loaded without guessing, and why not."""
        location = self.model_location(base_dir)
        if isinstance(location, Path):
            return True, ""
        if Path(self.model).is_absolute() or self.model.startswith((".", "~")) or self.model.count("/") != 1:
            return False, f"model directory not found: {self.model}"
        return True, "downloads from Hugging Face on first use"


class TranscriptionSettings(BaseModel):
    """Server-owned transcription models and limits."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    default_model: str | None = None
    max_audio_seconds: float = Field(default=1200, gt=0)
    max_batch_items: int = Field(default=100, ge=1)
    chunk_seconds: float = Field(default=180, ge=10)
    idle_unload_seconds: float = Field(default=600, ge=0)
    models: tuple[TranscriptionModelProfile, ...] = ()

    @model_validator(mode="after")
    def validate_models(self) -> Self:
        """Require unique model IDs and a configured default."""
        ids = [profile.id for profile in self.models]
        if len(ids) != len(set(ids)):
            raise ValueError("transcription model ids must be unique")
        if self.default_model is not None and self.default_model not in ids:
            raise ValueError("default_model must name a configured transcription model")
        return self

    def profile(self, model_id: str | None) -> TranscriptionModelProfile:
        """Return the requested profile or the default one.

        Raises:
            KeyError: No configured profile matches.
        """
        selected = model_id or self.default_model or (self.models[0].id if self.models else None)
        for profile in self.models:
            if profile.id == selected:
                return profile
        raise KeyError(selected or "")


def load_transcription_settings(path: str | None = None) -> TranscriptionSettings:
    """Load optional transcription models and limits from the runtime TOML file."""
    config, _ = read_runtime_config(path)
    section: Mapping[str, Any] = config_section(config, "transcription")
    return TranscriptionSettings.model_validate(dict(section)) if section else TranscriptionSettings()


TRANSCRIPTION_SETTINGS = load_transcription_settings()


def public_transcription_config(settings: TranscriptionSettings = TRANSCRIPTION_SETTINGS) -> dict[str, Any]:
    """Return browser-safe model choices and limits without local paths."""
    models = []
    for profile in settings.models:
        available, reason = profile.availability()
        models.append({"id": profile.id, "label": profile.display_label, "available": available, "reason": reason})
    default = settings.default_model or next((model["id"] for model in models if model["available"]), None)
    return {
        "models": models,
        "default_model": default,
        "limits": {"max_audio_seconds": settings.max_audio_seconds, "max_batch_items": settings.max_batch_items},
    }

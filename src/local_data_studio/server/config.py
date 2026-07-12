"""Shared configuration helpers and environment-backed settings."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from ..runtime_config import apply_settings_environment, read_runtime_config

PACKAGE_DIR: Path = Path(__file__).resolve().parents[1]
BASE_DIR: Path = Path(os.environ.get("LOCAL_DATA_STUDIO_WORKSPACE_DIR") or Path.cwd()).expanduser().resolve()
ENV_FILE: Path = Path(os.environ.get("LOCAL_DATA_STUDIO_ENV_FILE") or BASE_DIR / ".env").expanduser().resolve()


class Settings(BaseSettings):
    """Typed application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=str(ENV_FILE), case_sensitive=False, extra="ignore")

    data_file: str | None = Field(default=None, validation_alias="DATA_FILE")
    data_dir: str = Field(default=str(BASE_DIR / "data"), validation_alias="DATA_DIR")
    cache_dir: str = Field(default=str(BASE_DIR / "cache"), validation_alias="CACHE_DIR")
    embedder_models_dir: str = Field(default=str(BASE_DIR / "models" / "embedder"), validation_alias="EMBEDDER_MODELS_DIR")
    default_eda_sample: int = Field(default=5000, validation_alias="EDA_ROW_LIMIT")
    allow_delete_data: bool = Field(default=True, validation_alias="ALLOW_DELETE_DATA")
    eda_cell_max_chars: int = Field(default=5000, validation_alias="EDA_CELL_MAX_CHARS")
    eda_nested_policy: str = Field(default="stringify", validation_alias="EDA_NESTED_POLICY")
    eda_cache_max_bytes: int = Field(default=1024 * 1024 * 1024, validation_alias="EDA_CACHE_MAX_BYTES")
    atlas_host: str = Field(default="127.0.0.1", validation_alias="ATLAS_HOST")
    atlas_port: int = Field(default=5055, validation_alias="ATLAS_PORT")
    atlas_max_instances: int = Field(default=4, validation_alias="ATLAS_MAX_INSTANCES")
    atlas_sample: int = Field(default=0, validation_alias="ATLAS_SAMPLE")
    atlas_batch_size: int = Field(default=0, validation_alias="ATLAS_BATCH_SIZE")
    atlas_cache_max_bytes: int = Field(default=10 * 1024 * 1024 * 1024, validation_alias="ATLAS_CACHE_MAX_BYTES")
    atlas_text_max_chars: int = Field(default=4096, validation_alias="ATLAS_TEXT_MAX_CHARS")
    atlas_embedding_dtype: str = Field(default="float32", validation_alias="ATLAS_EMBEDDING_DTYPE")
    atlas_umap_projection_mode: str = Field(default="full", validation_alias="ATLAS_UMAP_PROJECTION_MODE")
    atlas_umap_anchor_sample: int = Field(default=10000, validation_alias="ATLAS_UMAP_ANCHOR_SAMPLE")
    legacy_atlas_projection_mode: str | None = Field(default=None, validation_alias="ATLAS_PROJECTION_MODE", exclude=True)
    legacy_atlas_anchor_sample: int | None = Field(default=None, validation_alias="ATLAS_ANCHOR_SAMPLE", exclude=True)
    atlas_trust_remote_code: bool = Field(default=False, validation_alias="ATLAS_TRUST_REMOTE_CODE")
    file_serve_roots: str | None = Field(
        default=None,
        validation_alias="FILE_SERVE_ROOTS",
        description="Comma-separated list of absolute directories from which /api/raw may serve files.",
    )
    vis_exclude_dirs: str | None = Field(
        default=None,
        validation_alias="VIS_EXCLUDE_DIRS",
        description="Comma-separated list of directories to exclude from dataset discovery under DATA_DIR.",
    )
    vis_exclude_files: str | None = Field(
        default=None,
        validation_alias="VIS_EXCLUDE_FILES",
        description="Comma-separated list of files to exclude from dataset discovery under DATA_DIR.",
    )

    @field_validator("data_file", mode="before")
    @classmethod
    def normalize_data_file(cls, value: str | None) -> str | None:
        """Treat a blank single-file setting as unset."""
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("default_eda_sample")
    @classmethod
    def validate_eda_row_limit(cls, value: int) -> int:
        """Accept an unlimited marker or any positive EDA row limit."""
        if value == -1 or value >= 1:
            return value
        raise ValueError("EDA_ROW_LIMIT must be -1 or an integer greater than or equal to 1")

    @field_validator("eda_nested_policy", mode="before")
    @classmethod
    def normalize_nested_policy(cls, value: str | None) -> str:
        """Normalize the nested-value policy to a lowercase non-empty value."""
        return (value or "stringify").strip().lower()

    @field_validator("atlas_embedding_dtype", mode="before")
    @classmethod
    def normalize_atlas_embedding_dtype(cls, value: str | None) -> str:
        """Accept only float32 or float16 Atlas embedding storage."""
        normalized = (value or "float32").strip().lower()
        if normalized not in {"float32", "float16"}:
            raise ValueError("ATLAS_EMBEDDING_DTYPE must be float32 or float16")
        return normalized

    @field_validator("atlas_sample")
    @classmethod
    def validate_atlas_sample(cls, value: int) -> int:
        """Accept zero for all rows or a positive projection row limit."""
        if value >= 0:
            return value
        raise ValueError("ATLAS_SAMPLE must be greater than or equal to 0")

    @field_validator("atlas_host", mode="before")
    @classmethod
    def normalize_atlas_host(cls, value: str | None) -> str:
        """Restrict child Atlas servers to the supported IPv4 loopback host."""
        normalized = (value or "127.0.0.1").strip().lower()
        if normalized in {"localhost", "127.0.0.1"}:
            return "127.0.0.1"
        raise ValueError("ATLAS_HOST must be localhost or 127.0.0.1")

    @field_validator("atlas_port")
    @classmethod
    def validate_atlas_port(cls, value: int) -> int:
        """Require a valid TCP port as the Atlas allocation starting point."""
        if 1 <= value <= 65535:
            return value
        raise ValueError("ATLAS_PORT must be between 1 and 65535")

    @field_validator("atlas_max_instances")
    @classmethod
    def validate_atlas_max_instances(cls, value: int) -> int:
        """Require a positive bound for concurrent Atlas child processes."""
        if value >= 1:
            return value
        raise ValueError("ATLAS_MAX_INSTANCES must be greater than or equal to 1")

    @field_validator("atlas_umap_projection_mode", mode="before")
    @classmethod
    def normalize_atlas_umap_projection_mode(cls, value: str | None) -> str:
        """Accept full UMAP or shared-space anchor transform projection."""
        normalized = (value or "full").strip().lower().replace("-", "_")
        if normalized not in {"full", "anchor_transform"}:
            raise ValueError("ATLAS_UMAP_PROJECTION_MODE must be full or anchor_transform")
        return normalized

    @field_validator("atlas_umap_anchor_sample")
    @classmethod
    def validate_atlas_umap_anchor_sample(cls, value: int) -> int:
        """Require a non-negative UMAP anchor count."""
        if value >= 0:
            return value
        raise ValueError("ATLAS_UMAP_ANCHOR_SAMPLE must be greater than or equal to 0")

    @model_validator(mode="after")
    def reject_legacy_umap_settings(self) -> Settings:
        """Reject removed UMAP setting names with explicit migration guidance."""
        if self.legacy_atlas_projection_mode is not None:
            raise ValueError("ATLAS_PROJECTION_MODE was removed; use ATLAS_UMAP_PROJECTION_MODE")
        if self.legacy_atlas_anchor_sample is not None:
            raise ValueError("ATLAS_ANCHOR_SAMPLE was removed; use ATLAS_UMAP_ANCHOR_SAMPLE")
        return self


# This also covers direct Uvicorn/ASGI startup with LOCAL_DATA_STUDIO_CONFIG_FILE.
# CLI startup applies the same settings before importing this module.
RUNTIME_CONFIG, _ = read_runtime_config(None)
apply_settings_environment(RUNTIME_CONFIG)
SETTINGS = Settings()

DATA_FILE_ENV: str | None = SETTINGS.data_file
DATA_DIR_ENV: str = SETTINGS.data_dir
SINGLE_FILE: Path | None = Path(DATA_FILE_ENV).resolve() if DATA_FILE_ENV else None
DATA_ROOT: Path = SINGLE_FILE.parent if SINGLE_FILE else Path(DATA_DIR_ENV).resolve()
DATA_SERVE_ROOT: Path = DATA_ROOT
EMBEDDER_MODELS_DIR: Path = Path(SETTINGS.embedder_models_dir).expanduser().resolve()
EMBEDDER_MODELS_DIR.mkdir(parents=True, exist_ok=True)


def _split_comma_separated_setting(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_path_best_effort(path: Path) -> Path:
    try:
        expanded = path.expanduser()
    except RuntimeError:
        expanded = path
    try:
        return expanded.resolve()
    except OSError:
        return expanded.absolute()


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for p in paths:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        unique.append(p)
    return unique


def _resolve_file_serve_roots(data_serve_root: Path, configured_roots: str | None) -> list[Path]:
    roots = [data_serve_root, BASE_DIR]
    for entry in _split_comma_separated_setting(configured_roots):
        roots.append(_resolve_path_best_effort(Path(entry)))
    return _dedupe_paths(roots)


def _resolve_excluded_dataset_dirs(data_root: Path, entries: list[str]) -> list[Path]:
    resolved: list[Path] = []
    for entry in entries:
        try:
            candidate = Path(entry).expanduser()
        except RuntimeError:
            candidate = Path(entry)
        if not candidate.is_absolute():
            candidate = data_root / candidate
        resolved.append(_resolve_path_best_effort(candidate))
    return _dedupe_paths(resolved)


FILE_SERVE_ROOTS: list[Path] = _resolve_file_serve_roots(DATA_SERVE_ROOT, SETTINGS.file_serve_roots)
VIS_EXCLUDE_DIRS: list[str] = _split_comma_separated_setting(SETTINGS.vis_exclude_dirs)
VIS_EXCLUDE_PATHS: list[Path] = _resolve_excluded_dataset_dirs(DATA_ROOT, VIS_EXCLUDE_DIRS)
VIS_EXCLUDE_FILES: list[str] = _split_comma_separated_setting(SETTINGS.vis_exclude_files)
VIS_EXCLUDE_FILE_PATHS: list[Path] = _resolve_excluded_dataset_dirs(DATA_ROOT, VIS_EXCLUDE_FILES)
CACHE_DIR: Path = Path(SETTINGS.cache_dir).resolve()
CACHE_DIR.mkdir(parents=True, exist_ok=True)
ALLOWED_EXTENSIONS: set[str] = {".jsonl", ".json", ".csv", ".tsv", ".parquet"}
UPLOAD_EXTENSIONS: set[str] = {".jsonl", ".csv", ".tsv", ".parquet"}
DEFAULT_LIMIT: int = 100
MAX_LIMIT: int = 1000
MAX_COLUMNS: int = 500
MAX_CELL_CHARS: int = 1200
MAX_SEQ_ITEMS: int = 30
DEFAULT_SAMPLE: int = 500
MAX_SAMPLE: int = 2000
MAX_OFFSET_FALLBACK: int = 10_000
MAX_JSON_PREVIEW_BYTES: int = 16 * 1024 * 1024
SYNC_OPERATION_MAX_BYTES: int = 256 * 1024 * 1024
HARD_DELETE_MAX_BYTES: int = 256 * 1024 * 1024
PARQUET_PREVIEW_BATCH_SIZE: int = 2048
DUCKDB_QUERY_MEMORY_LIMIT: str = "1GB"
DUCKDB_QUERY_TIMEOUT_SECONDS: float = 30.0
DUCKDB_QUERY_POLL_SECONDS: float = 0.1
COLUMN_LIMIT_WARNING: str = f"Only the first {MAX_COLUMNS} columns are returned to keep API responses bounded."

DEFAULT_EDA_SAMPLE: int = SETTINGS.default_eda_sample
ALLOW_DELETE_DATA: bool = SETTINGS.allow_delete_data

EDA_CELL_MAX_CHARS: int = SETTINGS.eda_cell_max_chars
EDA_NESTED_POLICY: str = SETTINGS.eda_nested_policy
EDA_CACHE_DIR: Path = CACHE_DIR / "eda"
EDA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
EDA_CACHE_MAX_BYTES: int = max(0, SETTINGS.eda_cache_max_bytes)

ATLAS_HOST: str = SETTINGS.atlas_host
ATLAS_PORT: int = SETTINGS.atlas_port
ATLAS_MAX_INSTANCES: int = SETTINGS.atlas_max_instances
ATLAS_SAMPLE: int = SETTINGS.atlas_sample
ATLAS_BATCH_SIZE: int = SETTINGS.atlas_batch_size
ATLAS_CACHE_ROOT: Path = CACHE_DIR / "atlas"
ATLAS_DATA_CACHE_DIR: Path = ATLAS_CACHE_ROOT / "datasets"
ATLAS_CACHE_DIR: Path = ATLAS_CACHE_ROOT / "projection"
ATLAS_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
ATLAS_DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
ATLAS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
ATLAS_CACHE_MAX_BYTES: int = max(0, SETTINGS.atlas_cache_max_bytes)
ATLAS_TEXT_MAX_CHARS: int = max(0, SETTINGS.atlas_text_max_chars)
ATLAS_EMBEDDING_DTYPE: str = SETTINGS.atlas_embedding_dtype
ATLAS_UMAP_PROJECTION_MODE: str = SETTINGS.atlas_umap_projection_mode
ATLAS_UMAP_ANCHOR_SAMPLE: int = SETTINGS.atlas_umap_anchor_sample
ATLAS_TRUST_REMOTE_CODE: bool = SETTINGS.atlas_trust_remote_code

"""Shared configuration helpers and environment-backed settings."""

from pathlib import Path

from pydantic import BaseSettings, Field, validator

BASE_DIR: Path = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """Typed application settings loaded from environment variables."""

    data_file: str | None = Field(default=None, env="DATA_FILE")
    data_dir: str = Field(default=str(BASE_DIR / "data"), env="DATA_DIR")
    cache_dir: str = Field(default=str(BASE_DIR / "cache"), env="CACHE_DIR")
    openai_api_key: str | None = Field(default=None, env="OPENAI_API_KEY")
    openai_base_url: str = Field(default="https://api.openai.com/v1", env="OPENAI_BASE_URL")
    openai_model: str = Field(default="gpt-5.2", env="OPENAI_MODEL")
    eda_font_family: str | None = Field(default=None, env="EDA_FONT_FAMILY")
    eda_font_path: str | None = Field(default=None, env="EDA_FONT_PATH")
    default_eda_sample: int = Field(default=5000, env="EDA_ROW_LIMIT")
    allow_delete_data: bool = Field(default=True, env="ALLOW_DELETE_DATA")
    default_eda_mode: str = Field(default="minimal", env="EDA_PROFILE_MODE")
    eda_cell_max_chars: int = Field(default=5000, env="EDA_CELL_MAX_CHARS")
    eda_nested_policy: str = Field(default="stringify", env="EDA_NESTED_POLICY")
    file_serve_roots: str | None = Field(
        default=None,
        env="FILE_SERVE_ROOTS",
        description="Comma-separated list of absolute directories from which /api/raw may serve files.",
    )
    vis_exclude_dirs: str | None = Field(
        default=None,
        env="VIS_EXCLUDE_DIRS",
        description="Comma-separated list of directories to exclude from dataset discovery under DATA_DIR.",
    )

    @validator("data_file", pre=True)
    def normalize_data_file(cls, value: str | None) -> str | None:  # noqa: N805
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @validator("default_eda_mode", pre=True)
    def normalize_eda_mode(cls, value: str | None) -> str:  # noqa: N805
        return (value or "minimal").strip().lower()

    @validator("eda_nested_policy", pre=True)
    def normalize_nested_policy(cls, value: str | None) -> str:  # noqa: N805
        return (value or "stringify").strip().lower()

    class Config(BaseSettings.Config):
        env_file = str(BASE_DIR / ".env")
        case_sensitive = False


SETTINGS = Settings()

DATA_FILE_ENV: str | None = SETTINGS.data_file
DATA_DIR_ENV: str = SETTINGS.data_dir
SINGLE_FILE: Path | None = Path(DATA_FILE_ENV).resolve() if DATA_FILE_ENV else None
DATA_ROOT: Path = SINGLE_FILE.parent if SINGLE_FILE else Path(DATA_DIR_ENV).resolve()
DATA_SERVE_ROOT: Path = DATA_ROOT


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
CACHE_DIR: Path = Path(SETTINGS.cache_dir).resolve()
CACHE_DIR.mkdir(parents=True, exist_ok=True)
OPENAI_API_KEY: str | None = SETTINGS.openai_api_key
OPENAI_BASE_URL: str = SETTINGS.openai_base_url
OPENAI_MODEL: str = SETTINGS.openai_model
EDA_FONT_FAMILY: str | None = SETTINGS.eda_font_family
EDA_FONT_PATH: str | None = SETTINGS.eda_font_path

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
MAX_EDA_SAMPLE: int = 50000
ALLOW_DELETE_DATA: bool = SETTINGS.allow_delete_data

# EDA profile mode: "minimal" or "maximal"
DEFAULT_EDA_MODE: str = SETTINGS.default_eda_mode
EDA_CELL_MAX_CHARS: int = SETTINGS.eda_cell_max_chars
EDA_NESTED_POLICY: str = SETTINGS.eda_nested_policy

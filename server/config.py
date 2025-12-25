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
MAX_CELL_CHARS: int = 1200
MAX_SEQ_ITEMS: int = 30
DEFAULT_SAMPLE: int = 500
MAX_SAMPLE: int = 2000

DEFAULT_EDA_SAMPLE: int = SETTINGS.default_eda_sample
MAX_EDA_SAMPLE: int = 50000
ALLOW_DELETE_DATA: bool = SETTINGS.allow_delete_data

# EDA profile mode: "minimal" or "maximal"
DEFAULT_EDA_MODE: str = SETTINGS.default_eda_mode
EDA_CELL_MAX_CHARS: int = SETTINGS.eda_cell_max_chars
EDA_NESTED_POLICY: str = SETTINGS.eda_nested_policy

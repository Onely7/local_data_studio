"""Shared loading helpers for Local Data Studio TOML configuration."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

CONFIG_FILE_ENV = "LOCAL_DATA_STUDIO_CONFIG_FILE"

# These TOML keys mirror environment-backed Settings fields. Paths remain in
# [paths], while [settings] owns the operational EDA, Atlas, and deletion
# controls so a checked local TOML can replace a large .env file.
SETTINGS_ENV_KEYS: dict[str, str] = {
    "eda_row_limit": "EDA_ROW_LIMIT",
    "allow_delete_data": "ALLOW_DELETE_DATA",
    "eda_cell_max_chars": "EDA_CELL_MAX_CHARS",
    "eda_nested_policy": "EDA_NESTED_POLICY",
    "eda_cache_max_bytes": "EDA_CACHE_MAX_BYTES",
    "atlas_host": "ATLAS_HOST",
    "atlas_port": "ATLAS_PORT",
    "atlas_sample": "ATLAS_SAMPLE",
    "atlas_batch_size": "ATLAS_BATCH_SIZE",
    "atlas_cache_max_bytes": "ATLAS_CACHE_MAX_BYTES",
    "atlas_text_max_chars": "ATLAS_TEXT_MAX_CHARS",
    "atlas_embedding_dtype": "ATLAS_EMBEDDING_DTYPE",
    "atlas_umap_projection_mode": "ATLAS_UMAP_PROJECTION_MODE",
    "atlas_umap_anchor_sample": "ATLAS_UMAP_ANCHOR_SAMPLE",
    "atlas_trust_remote_code": "ATLAS_TRUST_REMOTE_CODE",
}


def read_runtime_config(path: str | None) -> tuple[dict[str, Any], Path | None]:
    """Read an optional TOML configuration file.

    Args:
        path: Explicit path, or ``None`` to use ``LOCAL_DATA_STUDIO_CONFIG_FILE``.

    Returns:
        The parsed root table and resolved source path. Both are empty when no
        configuration file is selected.

    Raises:
        OSError: The selected file cannot be read.
        tomllib.TOMLDecodeError: The selected file is not valid TOML.
    """
    selected = path or os.environ.get(CONFIG_FILE_ENV)
    if not selected:
        return {}, None
    config_path = Path(selected).expanduser().resolve()
    with config_path.open("rb") as handle:
        loaded = tomllib.load(handle)
    return loaded, config_path


def config_section(config: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    """Return a mapping-valued root section, or an empty mapping."""
    value = config.get(name)
    return value if isinstance(value, Mapping) else {}


def apply_settings_environment(config: Mapping[str, Any]) -> None:
    """Expose `[settings]` values through the existing Settings environment API.

    Existing OS environment variables are never replaced. This preserves the
    documented precedence over TOML while allowing Pydantic Settings to keep
    its existing validation and `.env` fallback behavior.
    """
    settings = config_section(config, "settings")
    for key, environment_name in SETTINGS_ENV_KEYS.items():
        value = settings.get(key)
        if value is None or environment_name in os.environ:
            continue
        os.environ[environment_name] = str(value).lower() if isinstance(value, bool) else str(value)

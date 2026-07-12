"""Shared loading helpers for Local Data Studio TOML configuration."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

CONFIG_FILE_ENV = "LOCAL_DATA_STUDIO_CONFIG_FILE"


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

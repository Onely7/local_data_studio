"""Cache helpers for Embedding Atlas projection artifacts."""

from __future__ import annotations

import os
from pathlib import Path

from .cache import prune_cache_dir


def prune_cache_dir_from_env() -> None:
    """Prune the Atlas projection cache using environment variables."""
    cache_dir = os.environ.get("LOCAL_DATA_STUDIO_ATLAS_CACHE_PRUNE_DIR") or os.environ.get("LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR")
    if not cache_dir:
        return
    try:
        max_bytes = int(os.environ.get("LOCAL_DATA_STUDIO_ATLAS_CACHE_MAX_BYTES", "0"))
    except ValueError:
        max_bytes = 0
    prune_cache_dir(Path(cache_dir), max_bytes)

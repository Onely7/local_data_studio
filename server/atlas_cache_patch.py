"""Runtime patch loaded by embedding-atlas to use Local Data Studio cache."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

import embedding_atlas.projection as atlas_projection

from .atlas_cache import prune_cache_dir_from_env


def _cache_root() -> Path | None:
    cache_dir = os.environ.get("LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR")
    return Path(cache_dir) if cache_dir else None


if not getattr(atlas_projection, "_local_data_studio_cache_patched", False):
    _original_compute_projection = atlas_projection.compute_projection
    _original_async_compute_projection = atlas_projection.async_compute_projection

    def compute_projection(*args: Any, cache_root: str | Path | None = None, **kwargs: Any) -> Any:
        prune_cache_dir_from_env()
        result = _original_compute_projection(*args, cache_root=cache_root or _cache_root(), **kwargs)
        prune_cache_dir_from_env()
        return result

    async def async_compute_projection(*args: Any, cache_root: str | Path | None = None, **kwargs: Any) -> Any:
        prune_cache_dir_from_env()
        result = await _original_async_compute_projection(*args, cache_root=cache_root or _cache_root(), **kwargs)
        prune_cache_dir_from_env()
        return result

    patched_projection = cast(Any, atlas_projection)
    patched_projection.compute_projection = compute_projection
    patched_projection.async_compute_projection = async_compute_projection
    patched_projection._local_data_studio_cache_patched = True

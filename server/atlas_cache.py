"""Cache helpers for Embedding Atlas projection artifacts."""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _file_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _protected_cache_paths(paths: Iterable[Path] | None) -> set[Path]:
    if paths is None:
        return set()
    return {path.resolve() for path in paths}


def prune_cache_dir(cache_dir: Path, max_bytes: int, *, preserve: Iterable[Path] | None = None) -> int:
    """Prune cache files oldest-first while preserving active artifacts."""
    protected_paths = _protected_cache_paths(preserve)
    if max_bytes <= 0:
        if cache_dir.exists():
            for path in sorted(cache_dir.rglob("*"), key=lambda item: len(item.parts), reverse=True):
                if path.resolve() in protected_paths:
                    continue
                if path.is_file():
                    path.unlink(missing_ok=True)
                elif path.is_dir():
                    try:
                        path.rmdir()
                    except OSError:
                        pass
        cache_dir.mkdir(parents=True, exist_ok=True)
        return sum(_file_size(path) for path in cache_dir.rglob("*") if path.is_file())

    cache_dir.mkdir(parents=True, exist_ok=True)
    files = [path for path in cache_dir.rglob("*") if path.is_file()]
    total = sum(_file_size(path) for path in files)
    if total <= max_bytes:
        return total

    for path in sorted(files, key=lambda item: (_file_mtime(item), item.as_posix())):
        if path.resolve() in protected_paths:
            continue
        try:
            size = path.stat().st_size
            path.unlink()
            total -= size
        except OSError:
            continue
        if total <= max_bytes:
            break

    for directory in sorted((path for path in cache_dir.rglob("*") if path.is_dir()), key=lambda item: len(item.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            pass
    return max(total, 0)


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

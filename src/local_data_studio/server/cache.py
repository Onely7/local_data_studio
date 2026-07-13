"""Dataset fingerprinting and cache path helpers."""

from __future__ import annotations

import hashlib
import json
import stat as stat_module
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .config import CACHE_DIR

COUNT_CACHE_DIR = CACHE_DIR / "count"
METADATA_CACHE_DIR = CACHE_DIR / "metadata"
INDEX_CACHE_DIR = CACHE_DIR / "index"
SEARCH_CACHE_DIR = CACHE_DIR / "search"
STATS_CACHE_DIR = CACHE_DIR / "stats"

for cache_dir in (COUNT_CACHE_DIR, METADATA_CACHE_DIR, INDEX_CACHE_DIR, SEARCH_CACHE_DIR, STATS_CACHE_DIR):
    cache_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True, slots=True)
class _CacheFile:
    path: Path
    size: int
    modified_ns: int


_CACHE_LOCKS_GUARD = threading.Lock()
_CACHE_LOCKS: dict[Path, threading.Lock] = {}


def _cache_lock(cache_dir: Path) -> threading.Lock:
    resolved = cache_dir.resolve()
    with _CACHE_LOCKS_GUARD:
        return _CACHE_LOCKS.setdefault(resolved, threading.Lock())


def _scan_cache_files(cache_dir: Path) -> list[_CacheFile]:
    files: list[_CacheFile] = []
    for path in cache_dir.rglob("*"):
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat_module.S_ISREG(stat.st_mode):
            files.append(_CacheFile(path=path, size=stat.st_size, modified_ns=stat.st_mtime_ns))
    return files


def _remove_empty_cache_dirs(cache_dir: Path) -> None:
    for directory in sorted((path for path in cache_dir.rglob("*") if path.is_dir()), key=lambda item: len(item.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            pass


def prune_cache_dir(cache_dir: Path, max_bytes: int, *, preserve: Iterable[Path] | None = None) -> int:
    """Prune cache files oldest-first while retaining active artifacts.

    A file listed in ``preserve`` is never removed during this call, so a report
    returned to the caller remains available even when it alone exceeds the
    configured capacity.
    """
    protected_paths = {path.resolve() for path in preserve or ()}
    with _cache_lock(cache_dir):
        cache_dir.mkdir(parents=True, exist_ok=True)
        files = _scan_cache_files(cache_dir)
        total = sum(entry.size for entry in files)
        target_bytes = max(max_bytes, 0)
        if total > target_bytes:
            for entry in sorted(files, key=lambda item: (item.modified_ns, item.path.as_posix())):
                if entry.path.resolve() in protected_paths:
                    continue
                try:
                    entry.path.unlink()
                except OSError:
                    continue
                total -= entry.size
                if total <= target_bytes:
                    break
        _remove_empty_cache_dirs(cache_dir)
        return max(total, 0)


@dataclass(frozen=True, slots=True)
class DatasetFingerprint:
    """Stable identifier for cache invalidation of a local dataset file."""

    key: str
    path: Path
    size: int
    modified_ns: int

    @classmethod
    def from_path(cls, path: Path) -> DatasetFingerprint:
        """Fingerprint the resolved path, byte size, and nanosecond modification time.

        Raises:
            OSError: The dataset cannot be resolved or inspected.
        """
        resolved = path.resolve()
        stat = resolved.stat()
        payload = f"{resolved}|{stat.st_size}|{stat.st_mtime_ns}"
        key = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return cls(key=key, path=resolved, size=stat.st_size, modified_ns=stat.st_mtime_ns)


def metadata_cache_path(path: Path) -> Path:
    """Return the metadata cache file for a dataset."""
    return METADATA_CACHE_DIR / f"{DatasetFingerprint.from_path(path).key}.json"


def _operation_cache_key(path: Path, payload: dict[str, object]) -> str:
    fingerprint = DatasetFingerprint.from_path(path).key
    encoded_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{fingerprint}|{encoded_payload}".encode()).hexdigest()


def count_cache_path(path: Path, *, deleted_ids: list[int]) -> Path:
    """Return the cached count result path for a dataset and soft-delete state."""
    key = _operation_cache_key(path, {"deleted_ids": sorted(deleted_ids)})
    return COUNT_CACHE_DIR / f"{key}.json"


def index_cache_path(path: Path) -> Path:
    """Return the sidecar SQLite index file for a dataset."""
    return INDEX_CACHE_DIR / f"{DatasetFingerprint.from_path(path).key}.sqlite"


def search_cache_path(path: Path, *, query: str, limit: int, deleted_ids: list[int]) -> Path:
    """Return the cached search result path for a dataset and search parameters."""
    key = _operation_cache_key(path, {"query": query, "limit": limit, "deleted_ids": sorted(deleted_ids)})
    return SEARCH_CACHE_DIR / f"{key}.json"


def stats_cache_path(path: Path) -> Path:
    """Return the sampled statistics cache file for a dataset."""
    return STATS_CACHE_DIR / f"{DatasetFingerprint.from_path(path).key}.json"

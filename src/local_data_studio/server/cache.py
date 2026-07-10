"""Dataset fingerprinting and cache path helpers."""

from __future__ import annotations

import hashlib
import json
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
class DatasetFingerprint:
    """Stable identifier for cache invalidation of a local dataset file."""

    key: str
    path: Path
    size: int
    modified_ns: int

    @classmethod
    def from_path(cls, path: Path) -> DatasetFingerprint:
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

"""Helpers for discovering, resolving, and naming local files."""

import os
import threading
from collections.abc import Sequence
from pathlib import Path

from fastapi import HTTPException

from .config import ALLOWED_EXTENSIONS, DATA_ROOT, SINGLE_FILE, VIS_EXCLUDE_PATHS

IMAGE_PREVIEW_EXTENSIONS: set[str] = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}

_DATASET_CATALOG_LOCK = threading.Lock()
_DATASET_CATALOG: dict[str, Path] = {}
_DATASET_CATALOG_READY = threading.Event()


def _resolve_path_best_effort(path: Path) -> Path:
    try:
        expanded = path.expanduser()
    except RuntimeError:
        expanded = path
    try:
        return expanded.resolve()
    except OSError:
        return expanded.absolute()


def _is_path_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _is_excluded_directory(path: Path, excluded_dirs: Sequence[Path]) -> bool:
    return any(_is_path_within(path, excluded_dir) for excluded_dir in excluded_dirs)


def _is_supported_dataset_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in ALLOWED_EXTENSIONS


def discover_dataset_files(
    data_root: Path,
    single_file: Path | None,
    excluded_dirs: Sequence[Path],
) -> list[Path]:
    """Return supported dataset files while pruning configured excluded directories."""
    if single_file is not None:
        return [single_file] if _is_supported_dataset_file(single_file) else []

    if not data_root.exists():
        return []

    resolved_excluded_dirs = [_resolve_path_best_effort(path) for path in excluded_dirs]
    discovered: list[Path] = []

    for dirpath, dirnames, filenames in os.walk(data_root):
        current_dir = _resolve_path_best_effort(Path(dirpath))
        if _is_excluded_directory(current_dir, resolved_excluded_dirs):
            dirnames[:] = []
            continue

        dirnames[:] = [
            dirname
            for dirname in dirnames
            if not _is_excluded_directory(
                _resolve_path_best_effort(current_dir / dirname),
                resolved_excluded_dirs,
            )
        ]

        for filename in filenames:
            candidate = current_dir / filename
            if _is_supported_dataset_file(candidate):
                discovered.append(candidate)

    return sorted(discovered)


def refresh_dataset_file_catalog() -> dict[str, Path]:
    """Discover datasets and cache an allowlist keyed by their displayed names."""
    files = discover_dataset_files(DATA_ROOT, SINGLE_FILE, VIS_EXCLUDE_PATHS)
    catalog: dict[str, Path] = {}
    for path in files:
        resolved = _resolve_path_best_effort(path)
        try:
            name = str(resolved.relative_to(DATA_ROOT))
        except ValueError:
            continue
        catalog[name] = resolved

    with _DATASET_CATALOG_LOCK:
        _DATASET_CATALOG.clear()
        _DATASET_CATALOG.update(catalog)
        _DATASET_CATALOG_READY.set()
    return dict(catalog)


def _dataset_file_catalog() -> dict[str, Path]:
    with _DATASET_CATALOG_LOCK:
        if _DATASET_CATALOG_READY.is_set():
            return dict(_DATASET_CATALOG)
    return refresh_dataset_file_catalog()


def resolve_data_file(file_name: str | None) -> Path:
    """Resolve an API dataset name through the server-owned dataset allowlist."""
    if not file_name:
        raise HTTPException(status_code=400, detail="file is required")

    candidate = _dataset_file_catalog().get(file_name)
    if candidate is None:
        raise HTTPException(status_code=404, detail="file not found")
    return candidate


def unique_path(directory: Path, filename: str) -> Path:
    """Return a non-colliding path inside a directory for an upload."""
    base = Path(filename).name
    stem = Path(base).stem or "dataset"
    suffix = Path(base).suffix
    candidate = directory / base
    if not candidate.exists():
        return candidate
    index = 1
    while True:
        candidate = directory / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _strip_file_url_scheme(path: str) -> str:
    if path.startswith("file://"):
        return path[len("file://") :]
    return path


def is_path_within_roots(path: Path, roots: Sequence[Path]) -> bool:
    """Return True when path is equal to or contained by one of the roots."""
    resolved_roots = [_resolve_path_best_effort(root) for root in roots]
    return any(_is_path_within(path, root) for root in resolved_roots)


def resolve_raw_image_file(raw_path: str | None, allowed_roots: Sequence[Path]) -> Path:
    """Resolve a local image path that may be safely served by /api/raw."""
    requested_path = _strip_file_url_scheme((raw_path or "").strip())
    if not requested_path:
        raise HTTPException(status_code=400, detail="path is required")

    resolved = _resolve_path_best_effort(Path(requested_path))

    if not resolved.exists() or not resolved.is_file():
        raise HTTPException(status_code=404, detail="file not found")

    if resolved.suffix.lower() not in IMAGE_PREVIEW_EXTENSIONS:
        raise HTTPException(status_code=400, detail="unsupported file type")

    if not is_path_within_roots(resolved, allowed_roots):
        raise HTTPException(status_code=403, detail="path is outside allowed roots")

    return resolved

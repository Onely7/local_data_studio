"""Helpers for discovering, resolving, and naming local files."""

import os
from collections.abc import Sequence
from pathlib import Path

from fastapi import HTTPException

from .config import ALLOWED_EXTENSIONS, DATA_ROOT, SINGLE_FILE

IMAGE_PREVIEW_EXTENSIONS: set[str] = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}


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


def resolve_data_file(file_name: str | None) -> Path:
    """Resolve a dataset path within the configured data root."""
    if SINGLE_FILE:
        candidate = SINGLE_FILE if not file_name else (DATA_ROOT / file_name).resolve()
        if candidate != SINGLE_FILE:
            raise HTTPException(status_code=400, detail="file does not match DATA_FILE target")
        if candidate.suffix.lower() not in ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=400, detail="unsupported file extension")
        if not candidate.exists() or not candidate.is_file():
            raise HTTPException(status_code=404, detail="file not found")
        return candidate

    if not file_name:
        raise HTTPException(status_code=400, detail="file is required")

    candidate = (DATA_ROOT / file_name).resolve()
    if DATA_ROOT != candidate and DATA_ROOT not in candidate.parents:
        raise HTTPException(status_code=400, detail="file path is outside data directory")
    if candidate.suffix.lower() not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="unsupported file extension")
    if not candidate.exists() or not candidate.is_file():
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

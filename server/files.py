"""Helpers for resolving and naming dataset files."""

from pathlib import Path

from fastapi import HTTPException

from .config import ALLOWED_EXTENSIONS, DATA_ROOT, SINGLE_FILE


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

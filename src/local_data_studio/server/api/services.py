"""Small shared services used by API routers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..cache import stats_cache_path
from ..config import DEFAULT_SAMPLE
from ..sql import is_large_dataset
from ..stats import compute_column_stats


def atlas_service() -> Any:
    """Import the optional ML stack only when an Atlas job is requested."""
    from .. import atlas  # noqa: PLC0415

    return atlas


def eda_reports_service() -> Any:
    """Import profiling dependencies only when an EDA operation is requested."""
    try:
        from .. import eda_reports  # noqa: PLC0415
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"EDA dependencies could not be loaded: {exc}") from exc
    return eda_reports


def reject_large_sync_operation(path: Path, operation: str) -> None:
    """Reject potentially blocking full scans above the large-dataset threshold."""
    if is_large_dataset(path):
        raise HTTPException(
            status_code=400,
            detail=f"{operation} can scan the full dataset; use the background job endpoint for large files",
        )


def load_cached_result(cache_path: Path) -> dict[str, Any] | None:
    """Load an object-shaped JSON cache, treating missing or invalid data as a miss."""
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_cached_result(cache_path: Path, result: dict[str, Any]) -> None:
    """Replace a JSON cache file with the supplied result.

    The caller retains ownership of ``result``; this function does not mutate it.
    """
    cache_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_cached_stats(path: Path, sample: int) -> dict[str, Any] | None:
    payload = load_cached_result(stats_cache_path(path))
    if payload is None or payload.get("sample_request") != sample:
        return None
    result = payload.get("result")
    return result if isinstance(result, dict) else None


def compute_cached_column_stats(file_name: str, path: Path, sample: int | None, force: bool) -> dict[str, Any]:
    """Compute or reuse statistics keyed by dataset fingerprint and sample size."""
    sample_value = sample if sample is not None else DEFAULT_SAMPLE
    if not force:
        cached = _load_cached_stats(path, sample_value)
        if cached is not None:
            return {**cached, "cached": True}
    result = compute_column_stats(file_name, path, sample_value)
    write_cached_result(stats_cache_path(path), {"sample_request": sample_value, "result": result})
    return {**result, "cached": False}

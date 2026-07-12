"""High-level EDA report generation workflows."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from .cache import prune_cache_dir
from .config import CACHE_DIR, DEFAULT_EDA_MODE, DEFAULT_EDA_SAMPLE, EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES
from .deleted_rows import deleted_row_ids_for
from .eda import build_eda_report, eda_cache_path, load_eda_dataframe, sanitize_eda_dataframe
from .jobs import JobContext
from .sql import guard_select_sql_for_dataset, load_query_dataframe_guarded

QUERY_EDA_CACHE_VERSION = "query-v4"
QUERY_EDA_HELPER_COLUMNS = frozenset({"__rowid", "rn"})


@dataclass(frozen=True, slots=True)
class EdaReportOptions:
    """Validated EDA report options shared by full-dataset and query-result reports."""

    sample: int
    mode: str
    force: bool

    @property
    def minimal(self) -> bool:
        """Return whether report generation uses the reduced-cost profile."""
        return self.mode != "maximal"

    @classmethod
    def from_request(cls, *, sample: int | None, mode: str | None, force: bool | None) -> EdaReportOptions:
        """Normalize nullable request values over configured EDA defaults."""
        requested_sample = DEFAULT_EDA_SAMPLE if sample is None else sample
        if requested_sample != -1 and requested_sample < 1:
            raise HTTPException(status_code=400, detail="EDA row limit must be -1 or an integer greater than or equal to 1")
        normalized_mode = (mode or DEFAULT_EDA_MODE or "minimal").strip().lower()
        if normalized_mode not in {"minimal", "maximal"}:
            raise HTTPException(status_code=400, detail="EDA profile mode must be minimal or maximal")
        return cls(sample=requested_sample, mode=normalized_mode, force=bool(force))


def _query_eda_cache_path(path: Path, sql: str, options: EdaReportOptions) -> Path:
    stat = path.stat()
    payload = f"{path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}|{QUERY_EDA_CACHE_VERSION}|{sql}|{options.sample}|{options.mode}"
    key = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
    stem = path.stem or "data"
    safe_stem = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)
    return EDA_CACHE_DIR / f"{safe_stem}-query-{options.mode}-{key}.html"


def _report_response(*, file_name: str, cache_path: Path, cached: bool, options: EdaReportOptions, source: str | None = None) -> dict[str, Any]:
    response: dict[str, Any] = {
        "file": file_name,
        "url": f"/cache/{cache_path.relative_to(CACHE_DIR).as_posix()}",
        "cached": cached,
        "sample": options.sample,
        "mode": options.mode,
    }
    if source is not None:
        response["source"] = source
    return response


def _raise_if_empty_dataframe(df: Any, *, message: str) -> None:
    try:
        if df.is_empty():
            raise HTTPException(status_code=400, detail=message)
    except AttributeError:
        if getattr(df, "empty", False):
            raise HTTPException(status_code=400, detail=message) from None


def _drop_query_helper_columns(df: Any) -> Any:
    columns = getattr(df, "columns", [])
    drop_columns = [name for name in columns if str(name).lower() in QUERY_EDA_HELPER_COLUMNS]
    if not drop_columns:
        return df
    drop = getattr(df, "drop", None)
    if not callable(drop):
        return df
    try:
        return drop(columns=drop_columns)
    except TypeError:
        # Polars accepts positional column names while pandas requires the
        # explicit ``columns=`` keyword to avoid interpreting them as rows.
        return drop(drop_columns)


def _write_profile_report(df: Any, *, title: str, cache_path: Path, options: EdaReportOptions, context: JobContext | None) -> None:
    try:
        if context is not None:
            context.check_cancelled()
            context.update(progress=0.7, message="Building EDA report")
        report = build_eda_report(df, title=title, minimal=options.minimal)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_file(str(cache_path))
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="ydata-profiling is not installed") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"EDA generation failed: {exc}") from exc


def generate_dataset_eda_report(
    *,
    file_name: str,
    path: Path,
    sample: int | None,
    mode: str | None,
    force: bool | None,
    context: JobContext | None = None,
) -> dict[str, Any]:
    """Generate or reuse an EDA report for a bounded sample of the whole dataset."""
    options = EdaReportOptions.from_request(sample=sample, mode=mode, force=force)
    cache_path = eda_cache_path(path, options.sample, options.mode)

    if cache_path.exists() and not options.force:
        prune_cache_dir(EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES, preserve=(cache_path,))
        return _report_response(file_name=file_name, cache_path=cache_path, cached=True, options=options)

    if context is not None:
        context.update(progress=0.1, message="Loading sampled rows")
    deleted_ids = deleted_row_ids_for(path)
    df = load_eda_dataframe(path, options.sample, deleted_ids)
    if df is None:
        raise HTTPException(status_code=400, detail="failed to load dataset")

    if context is not None:
        context.check_cancelled()
        context.update(progress=0.45, message="Preparing sampled data")
    df = sanitize_eda_dataframe(df)
    _raise_if_empty_dataframe(df, message="dataset is empty")

    prune_cache_dir(EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES)
    _write_profile_report(df, title=f"EDA Report: {path.name}", cache_path=cache_path, options=options, context=context)
    prune_cache_dir(EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES, preserve=(cache_path,))
    return _report_response(file_name=file_name, cache_path=cache_path, cached=False, options=options)


def generate_query_eda_report(
    *,
    file_name: str,
    path: Path,
    sql: str,
    sample: int | None,
    mode: str | None,
    force: bool | None,
    context: JobContext | None = None,
) -> dict[str, Any]:
    """Generate or reuse an EDA report for bounded SQL Console query results."""
    options = EdaReportOptions.from_request(sample=sample, mode=mode, force=force)
    guarded_sql = guard_select_sql_for_dataset(path, sql)
    cache_path = _query_eda_cache_path(path, guarded_sql, options)

    if cache_path.exists() and not options.force:
        prune_cache_dir(EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES, preserve=(cache_path,))
        return _report_response(file_name=file_name, cache_path=cache_path, cached=True, options=options, source="query")

    if context is not None:
        context.update(progress=0.1, message="Running SQL query")
    df = load_query_dataframe_guarded(path=path, sql=guarded_sql, sample=options.sample, context=context)

    if context is not None:
        context.check_cancelled()
        context.update(progress=0.45, message="Preparing query results")
    df = _drop_query_helper_columns(df)
    df = sanitize_eda_dataframe(df)
    _raise_if_empty_dataframe(df, message="query returned no rows")

    prune_cache_dir(EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES)
    _write_profile_report(df, title=f"EDA Report: {path.name} query results", cache_path=cache_path, options=options, context=context)
    prune_cache_dir(EDA_CACHE_DIR, EDA_CACHE_MAX_BYTES, preserve=(cache_path,))
    return _report_response(file_name=file_name, cache_path=cache_path, cached=False, options=options, source="query")

"""EDA report generation utilities backed by YData Profiling."""

import datetime
import decimal
import hashlib
import json
from pathlib import Path
from typing import Any

from ydata_profiling import ProfileReport

from .config import (
    CACHE_DIR,
    EDA_CELL_MAX_CHARS,
    EDA_NESTED_POLICY,
)
from .db import open_connection, relation_with_rowid_sql

EDA_CACHE_VERSION = "ydata-v1"


def eda_cache_key(path: Path, sample_rows: int, mode: str) -> str:
    """Build a stable cache key based on file metadata and options."""
    stat = path.stat()
    payload = f"{EDA_CACHE_VERSION}|{path.resolve()}|{stat.st_size}|{stat.st_mtime}|{sample_rows}|{mode}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def eda_cache_path(path: Path, sample_rows: int, mode: str) -> Path:
    """Return the cache path for an EDA report."""
    key = eda_cache_key(path, sample_rows, mode)[:12]
    stem = path.stem or "data"
    safe_stem = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)
    return CACHE_DIR / f"{safe_stem}-{mode}-{key}.html"


def load_eda_dataframe(path: Path, sample_rows: int, deleted_ids: list[int]) -> Any:
    """Load a bounded pandas sample suitable for YData Profiling."""
    with open_connection() as con:
        rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
        query = f"SELECT * EXCLUDE(__rowid) FROM ({rel_sql}) LIMIT {sample_rows}"
        return con.execute(query, params).df()


def sanitize_eda_dataframe(df: Any) -> Any:
    """Convert unsupported nested and binary values to bounded strings.

    YData Profiling consumes pandas DataFrames.  Scalar columns remain typed so
    their statistics stay useful; only cells that cannot be profiled reliably
    (lists, objects, binary data, and decimals) are normalised.
    """
    import pandas as pd  # noqa: PLC0415

    max_chars = EDA_CELL_MAX_CHARS
    nested_policy = EDA_NESTED_POLICY
    # nested_policy: "stringify" or "drop"

    def _stringify(v: Any) -> str | None:
        if v is None:
            return None
        if isinstance(v, bytes):
            s = v.hex()
        elif isinstance(v, (datetime.date, datetime.datetime, decimal.Decimal)):
            s = str(v)
        elif isinstance(v, (list, tuple, dict)):
            try:
                s = json.dumps(v, ensure_ascii=False)
            except TypeError:
                s = str(v)
        else:
            s = str(v)

        if max_chars > 0 and len(s) > max_chars:
            s = s[:max_chars] + "... (truncated)"
        return s

    if not isinstance(df, pd.DataFrame):
        return df

    out = df.copy()
    drop_cols: list[str] = []

    unsupported_types = (bytes, bytearray, memoryview, list, tuple, dict, decimal.Decimal)
    for name in out.columns:
        values = out[name]
        needs_normalization = values.map(lambda value: isinstance(value, unsupported_types)).any()
        if not needs_normalization:
            continue
        if nested_policy == "drop":
            drop_cols.append(str(name))
            continue
        out[name] = values.map(_stringify)

    if drop_cols:
        out = out.drop(columns=drop_cols)

    return out


def build_eda_report(df: Any, title: str, minimal: bool) -> Any:
    """Create an HTML report without mutating Pydantic's module state."""
    return ProfileReport(df, title=title, minimal=minimal)

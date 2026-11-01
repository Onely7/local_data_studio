"""Guarded DuckDB SQL execution for user-authored SELECT queries."""

from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from .cache import DUCKDB_SPILL_DIR, QUERY_CACHE_DIR, prune_cache_dir, query_result_cache_path
from .config import (
    DUCKDB_QUERY_MEMORY_LIMIT,
    DUCKDB_QUERY_POLL_SECONDS,
    DUCKDB_QUERY_TIMEOUT_SECONDS,
    QUERY_CACHE_MAX_BYTES,
    QUERY_RESULT_MAX_ROWS,
    SQL_JOB_TIMEOUT_SECONDS,
    SYNC_OPERATION_MAX_BYTES,
)
from .db import (
    build_table_response,
    fetch_rows,
    normalize_pagination,
    open_connection,
    quote_literal,
    relation_sql_literal,
    relation_with_rowid_literal,
)
from .deleted_rows import deleted_row_ids_for
from .jobs import JobContext

FULL_SCAN_SQL_PATTERN = re.compile(
    r"\b(order\s+by|group\s+by|join|distinct|union)\b|\b(count|sum|avg|min|max|over)\s*\(",
    re.IGNORECASE,
)


def normalize_select_sql(sql: str) -> str:
    """Normalize and validate that SQL is one read-only SELECT/CTE statement."""
    normalized = sql.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="sql must not be empty")

    if ";" in normalized:
        normalized = normalized.rstrip(";")
        if ";" in normalized:
            raise HTTPException(status_code=400, detail="multi-statement sql is not allowed")

    sql_lower = normalized.lower()
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise HTTPException(status_code=400, detail="only SELECT queries are allowed")
    return normalized


def is_large_dataset(path: Path) -> bool:
    """Return whether synchronous full-scan-prone operations should be guarded."""
    return path.stat().st_size > SYNC_OPERATION_MAX_BYTES


def requires_full_scan(path: Path, sql: str) -> bool:
    """Return whether normalized SQL on this dataset must scan it in full before showing rows."""
    return is_large_dataset(path) and FULL_SCAN_SQL_PATTERN.search(sql) is not None


def _reject_unscheduled_full_scan() -> HTTPException:
    return HTTPException(
        status_code=400,
        detail="this SQL scans the full large dataset; run it as a background job, as the SQL Console does",
    )


def configure_duckdb_limits(connection: Any) -> None:
    """Apply local resource limits to a DuckDB connection used for user SQL.

    Operators that exceed the memory limit spill to the cache directory, and
    progress is tracked for :func:`query_progress` without printing a bar.
    """
    connection.execute(f"SET memory_limit='{DUCKDB_QUERY_MEMORY_LIMIT}'")
    connection.execute(f"SET temp_directory={quote_literal(str(DUCKDB_SPILL_DIR))}")
    connection.execute("SET enable_progress_bar = true")
    connection.execute("SET enable_progress_bar_print = false")


def query_progress(connection: Any | None) -> float | None:
    """Return a running DuckDB query's progress in ``[0, 1]``, or ``None`` when unknown."""
    progress = getattr(connection, "query_progress", None)
    if not callable(progress):
        return None
    try:
        value = float(progress())
    except Exception:
        return None
    return min(1.0, value / 100) if value >= 0 else None


def interrupt_duckdb_connection(connection: Any | None) -> None:
    """Interrupt a DuckDB connection when timeout or cancellation is requested."""
    interrupt = getattr(connection, "interrupt", None)
    if callable(interrupt):
        interrupt()


def create_data_view(connection: Any, path: Path, deleted_ids: list[int]) -> None:
    """Create the SQL Console's temporary `data` view, respecting soft-deleted rows."""
    if deleted_ids:
        filtered = relation_with_rowid_literal(path, deleted_ids)
        view_sql = f"SELECT * EXCLUDE(__rowid) FROM ({filtered})"
    else:
        view_sql = f"SELECT * FROM {relation_sql_literal(path)}"
    connection.execute(f"CREATE OR REPLACE TEMP VIEW data AS {view_sql}")


def _execute_query_worker(
    *,
    path: Path,
    file_name: str,
    sql: str,
    limit_value: int,
    offset_value: int,
    deleted_ids: list[int],
    connection_holder: dict[str, Any],
) -> dict[str, Any]:
    with open_connection() as connection:
        connection_holder["connection"] = connection
        configure_duckdb_limits(connection)
        create_data_view(connection, path, deleted_ids)
        query_sql = f"SELECT * FROM ({sql}) AS q LIMIT {limit_value} OFFSET {offset_value}"
        columns, rows = fetch_rows(connection, query_sql, [])
    return build_table_response(file_name, columns, rows, limit_value, offset_value, [])


def _fetch_raw_query_row_worker(
    *,
    path: Path,
    sql: str,
    offset: int,
    deleted_ids: list[int],
    connection_holder: dict[str, Any],
) -> tuple[list[str], list[Any]]:
    with open_connection() as connection:
        connection_holder["connection"] = connection
        configure_duckdb_limits(connection)
        create_data_view(connection, path, deleted_ids)
        result = connection.execute(f"SELECT * FROM ({sql}) AS q LIMIT 1 OFFSET {offset}")
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="row not found")
        columns = [description[0] for description in result.description or []]
    return columns, list(row)


def _wait_for_duckdb_result(
    *,
    future: Future[Any],
    connection_holder: dict[str, Any],
    context: JobContext | None,
    progress_message: str,
    timeout_seconds: float = DUCKDB_QUERY_TIMEOUT_SECONDS,
) -> Any:
    started = time.monotonic()
    try:
        while True:
            try:
                return future.result(timeout=DUCKDB_QUERY_POLL_SECONDS)
            except FutureTimeoutError:
                elapsed = time.monotonic() - started
                if context is not None:
                    context.check_cancelled()
                    context.update(progress=query_progress(connection_holder.get("connection")), message=f"{progress_message} for {int(elapsed)}s")
                if timeout_seconds and elapsed >= timeout_seconds:
                    interrupt_duckdb_connection(connection_holder.get("connection"))
                    raise HTTPException(status_code=408, detail="SQL query timed out") from None
    except Exception:
        if not future.done():
            interrupt_duckdb_connection(connection_holder.get("connection"))
            future.cancel()
        raise


def _materialize_worker(
    *,
    path: Path,
    sql: str,
    deleted_ids: list[int],
    target: Path,
    connection_holder: dict[str, Any],
) -> None:
    with open_connection() as connection:
        connection_holder["connection"] = connection
        configure_duckdb_limits(connection)
        create_data_view(connection, path, deleted_ids)
        connection.execute(f"COPY (SELECT * FROM ({sql}) AS q LIMIT {QUERY_RESULT_MAX_ROWS + 1}) TO {quote_literal(str(target))} (FORMAT parquet)")


@lru_cache(maxsize=256)
def _materialization_lock(cache_key: str) -> threading.Lock:
    """Return a process-local single-flight lock for one query result file."""
    return threading.Lock()


def materialize_query_result(*, path: Path, sql: str, deleted_ids: list[int], context: JobContext) -> Path:
    """Run a full-scan query once and return its cached Parquet result.

    At most ``QUERY_RESULT_MAX_ROWS + 1`` rows are written, so callers can tell
    when the result was truncated. Identical concurrent requests share one
    scan. The scan reports DuckDB progress, honors cancellation, and is
    stopped after ``SQL_JOB_TIMEOUT_SECONDS``.

    Raises:
        HTTPException: The query timed out.
        JobCancelledError: The job was cancelled while waiting or scanning.
    """
    cache_path = query_result_cache_path(path, sql=sql, deleted_ids=deleted_ids)
    lock = _materialization_lock(str(cache_path))
    while not lock.acquire(timeout=0.25):
        context.check_cancelled()
        context.update(message="Waiting for the same query in another job")
    try:
        if cache_path.exists():
            context.update(progress=1.0, message="Using cached query results")
            return cache_path
        prune_cache_dir(QUERY_CACHE_DIR, QUERY_CACHE_MAX_BYTES)
        temporary = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        connection_holder: dict[str, Any] = {}
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-query-scan")
        future = executor.submit(
            _materialize_worker,
            path=path,
            sql=sql,
            deleted_ids=deleted_ids,
            target=temporary,
            connection_holder=connection_holder,
        )
        try:
            _wait_for_duckdb_result(
                future=future,
                connection_holder=connection_holder,
                context=context,
                progress_message="Scanning the full dataset",
                timeout_seconds=SQL_JOB_TIMEOUT_SECONDS,
            )
            temporary.replace(cache_path)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            temporary.unlink(missing_ok=True)
        prune_cache_dir(QUERY_CACHE_DIR, QUERY_CACHE_MAX_BYTES, preserve=(cache_path,))
        return cache_path
    finally:
        lock.release()


def _cached_query_page(file_name: str, cache_path: Path, limit_value: int, offset_value: int) -> dict[str, Any]:
    with open_connection() as connection:
        row = connection.execute("SELECT count(*) FROM read_parquet(?)", [str(cache_path)]).fetchone()
        materialized_rows = int(row[0]) if row else 0
        total_rows = min(materialized_rows, QUERY_RESULT_MAX_ROWS)
        page_limit = max(0, min(limit_value, total_rows - offset_value))
        columns, rows = fetch_rows(connection, f"SELECT * FROM read_parquet(?) LIMIT {page_limit} OFFSET {offset_value}", [str(cache_path)])
    response = build_table_response(file_name, columns, rows, limit_value, offset_value, [])
    response.update({"total_rows": total_rows, "cached_scan": True})
    if materialized_rows > QUERY_RESULT_MAX_ROWS:
        truncated = f"Only the first {QUERY_RESULT_MAX_ROWS:,} result rows are kept."
        existing = response.get("warning")
        response["warning"] = f"{existing} {truncated}" if existing else truncated
    return response


def execute_query_guarded(
    *,
    file_name: str,
    path: Path,
    sql: str,
    limit: int | None,
    offset: int | None,
    context: JobContext | None = None,
) -> dict[str, Any]:
    """Execute user SQL for table preview with timeout, memory, and scan-risk guards.

    On large datasets, aggregating, sorting, joining, and window queries run
    only as background jobs: the first page scans the dataset once and caches
    the result, and later pages read the cache.
    """
    deleted_ids = deleted_row_ids_for(path)
    normalized = normalize_select_sql(sql)
    limit_value, offset_value = normalize_pagination(limit, offset)
    if requires_full_scan(path, normalized):
        if context is None:
            raise _reject_unscheduled_full_scan()
        cache_path = materialize_query_result(path=path, sql=normalized, deleted_ids=deleted_ids, context=context)
        return _cached_query_page(file_name, cache_path, limit_value, offset_value)

    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-query")
    future = executor.submit(
        _execute_query_worker,
        path=path,
        file_name=file_name,
        sql=normalized,
        limit_value=limit_value,
        offset_value=offset_value,
        deleted_ids=deleted_ids,
        connection_holder=connection_holder,
    )
    try:
        response = _wait_for_duckdb_result(
            future=future,
            connection_holder=connection_holder,
            context=context,
            progress_message="Running SQL query",
            timeout_seconds=SQL_JOB_TIMEOUT_SECONDS if context is not None else DUCKDB_QUERY_TIMEOUT_SECONDS,
        )
        if is_large_dataset(path):
            existing = response.get("warning")
            warning = "SQL Console used timeout, memory, and scan-risk guards for this large dataset."
            response["warning"] = f"{existing} {warning}" if isinstance(existing, str) and existing else warning
        return response
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def fetch_raw_query_row_guarded(*, path: Path, sql: str, offset: int) -> tuple[list[str], list[Any]]:
    """Read one SQL Console result row with the same timeout and memory guards.

    Full-scan results on large datasets are read from the cache that the SQL
    Console job created.
    """
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must not be negative")
    normalized = normalize_select_sql(sql)
    deleted_ids = deleted_row_ids_for(path)
    if requires_full_scan(path, normalized):
        cache_path = query_result_cache_path(path, sql=normalized, deleted_ids=deleted_ids)
        if not cache_path.exists() or offset >= QUERY_RESULT_MAX_ROWS:
            raise HTTPException(status_code=404, detail="query results are not cached; run the query again")
        with open_connection() as connection:
            result = connection.execute(f"SELECT * FROM read_parquet(?) LIMIT 1 OFFSET {offset}", [str(cache_path)])
            row = result.fetchone()
            columns = [description[0] for description in result.description or []]
        if row is None:
            raise HTTPException(status_code=404, detail="row not found")
        return columns, list(row)
    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-raw-query")
    future = executor.submit(
        _fetch_raw_query_row_worker,
        path=path,
        sql=normalized,
        offset=offset,
        deleted_ids=deleted_ids,
        connection_holder=connection_holder,
    )
    try:
        return _wait_for_duckdb_result(
            future=future,
            connection_holder=connection_holder,
            context=None,
            progress_message="Loading raw query row",
        )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _load_query_dataframe_worker(
    *,
    path: Path,
    sql: str,
    sample: int,
    deleted_ids: list[int],
    connection_holder: dict[str, Any],
) -> Any:
    with open_connection() as connection:
        connection_holder["connection"] = connection
        configure_duckdb_limits(connection)
        create_data_view(connection, path, deleted_ids)
        # fg-data-profiling expects pandas (and otherwise attempts its Spark path,
        # which requires an ``rdd`` attribute). Keep query EDA aligned with the
        # full-dataset EDA loader in ``eda.py``.
        limit_clause = "" if sample == -1 else f" LIMIT {sample}"
        return connection.execute(f"SELECT * FROM ({sql}) AS q{limit_clause}").df()


def load_query_dataframe_guarded(
    *,
    path: Path,
    sql: str,
    sample: int,
    context: JobContext | None,
) -> Any:
    """Materialize configured SQL query results with resource and cancellation guards.

    A sample value of ``-1`` intentionally materializes all query rows. Full
    scans of large datasets reuse the SQL Console's cached result and require
    a job context.
    """
    normalized = normalize_select_sql(sql)
    deleted_ids = deleted_row_ids_for(path)
    if requires_full_scan(path, normalized):
        if context is None:
            raise _reject_unscheduled_full_scan()
        cache_path = materialize_query_result(path=path, sql=normalized, deleted_ids=deleted_ids, context=context)
        limit_clause = "" if sample == -1 else f" LIMIT {sample}"
        with open_connection() as connection:
            return connection.execute(f"SELECT * FROM read_parquet(?){limit_clause}", [str(cache_path)]).df()
    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-eda-query")
    future = executor.submit(
        _load_query_dataframe_worker,
        path=path,
        sql=normalized,
        sample=sample,
        deleted_ids=deleted_ids,
        connection_holder=connection_holder,
    )
    try:
        return _wait_for_duckdb_result(
            future=future,
            connection_holder=connection_holder,
            context=context,
            progress_message="Running SQL query",
            timeout_seconds=SQL_JOB_TIMEOUT_SECONDS if context is not None else DUCKDB_QUERY_TIMEOUT_SECONDS,
        )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

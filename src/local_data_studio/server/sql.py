"""Guarded DuckDB SQL execution for user-authored SELECT queries."""

from __future__ import annotations

import re
import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from .config import (
    DUCKDB_QUERY_MEMORY_LIMIT,
    DUCKDB_QUERY_POLL_SECONDS,
    DUCKDB_QUERY_TIMEOUT_SECONDS,
    SYNC_OPERATION_MAX_BYTES,
)
from .db import (
    build_table_response,
    fetch_rows,
    normalize_pagination,
    open_connection,
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


def guard_select_sql_for_dataset(path: Path, sql: str) -> str:
    """Validate a user SELECT and reject known full-scan-prone SQL for large datasets."""
    normalized = normalize_select_sql(sql)
    if is_large_dataset(path) and FULL_SCAN_SQL_PATTERN.search(normalized):
        raise HTTPException(
            status_code=400,
            detail="this SQL can require a full scan on large datasets; use a simpler preview query or a background workflow",
        )
    return normalized


def configure_duckdb_limits(connection: Any) -> None:
    """Apply local resource limits to a DuckDB connection used for user SQL."""
    connection.execute(f"SET memory_limit='{DUCKDB_QUERY_MEMORY_LIMIT}'")


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
                    context.update(message=f"{progress_message} for {int(elapsed)}s")
                if elapsed >= DUCKDB_QUERY_TIMEOUT_SECONDS:
                    interrupt_duckdb_connection(connection_holder.get("connection"))
                    raise HTTPException(status_code=408, detail="SQL query timed out") from None
    except Exception:
        if not future.done():
            interrupt_duckdb_connection(connection_holder.get("connection"))
            future.cancel()
        raise


def execute_query_guarded(
    *,
    file_name: str,
    path: Path,
    sql: str,
    limit: int | None,
    offset: int | None,
    context: JobContext | None = None,
) -> dict[str, Any]:
    """Execute user SQL for table preview with timeout, memory, and scan-risk guards."""
    deleted_ids = deleted_row_ids_for(path)
    guarded_sql = guard_select_sql_for_dataset(path, sql)
    limit_value, offset_value = normalize_pagination(limit, offset)

    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-query")
    future = executor.submit(
        _execute_query_worker,
        path=path,
        file_name=file_name,
        sql=guarded_sql,
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
        )
        if is_large_dataset(path):
            existing = response.get("warning")
            warning = "SQL Console used timeout, memory, and scan-risk guards for this large dataset."
            response["warning"] = f"{existing} {warning}" if isinstance(existing, str) and existing else warning
        return response
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def fetch_raw_query_row_guarded(*, path: Path, sql: str, offset: int) -> tuple[list[str], list[Any]]:
    """Read one SQL Console result row with the same timeout and memory guards."""
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must not be negative")
    guarded_sql = guard_select_sql_for_dataset(path, sql)
    deleted_ids = deleted_row_ids_for(path)
    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-raw-query")
    future = executor.submit(
        _fetch_raw_query_row_worker,
        path=path,
        sql=guarded_sql,
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
        # YData Profiling expects pandas (and otherwise attempts its Spark path,
        # which requires an ``rdd`` attribute). Keep query EDA aligned with the
        # full-dataset EDA loader in ``eda.py``.
        return connection.execute(f"SELECT * FROM ({sql}) AS q LIMIT {sample}").df()


def load_query_dataframe_guarded(
    *,
    path: Path,
    sql: str,
    sample: int,
    context: JobContext | None,
) -> Any:
    """Materialize bounded SQL query results for EDA with timeout, memory, and cancellation guards."""
    guarded_sql = guard_select_sql_for_dataset(path, sql)
    deleted_ids = deleted_row_ids_for(path)
    connection_holder: dict[str, Any] = {}
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-data-studio-eda-query")
    future = executor.submit(
        _load_query_dataframe_worker,
        path=path,
        sql=guarded_sql,
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
        )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

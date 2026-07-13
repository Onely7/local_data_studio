"""Deterministic row limiting for Atlas projection inputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..db import open_connection, quote_ident, quote_literal, relation_sql_literal
from ..sql import configure_duckdb_limits

ATLAS_SAMPLE_RANDOM_STATE = 42
ATLAS_SOURCE_FILE_COLUMN = "FILE_NAME"
ATLAS_INTERNAL_ORDINAL_COLUMN = "__local_data_studio_atlas_ordinal"


def sample_atlas_frame(data_frame: Any, sample_limit: int | None) -> Any:
    """Return at most ``sample_limit`` rows using a deterministic sample.

    The input frame is returned unchanged when the limit is absent or no
    smaller than the available row count. Sampling therefore behaves as a
    maximum rather than requiring the dataset to contain exactly that many
    rows.
    """
    if not sample_limit or len(data_frame) <= sample_limit:
        return data_frame
    return data_frame.sample(n=sample_limit, axis=0, random_state=ATLAS_SAMPLE_RANDOM_STATE).reset_index(drop=True)


def _unique_column_name(existing: set[str], candidate: str) -> str:
    if candidate not in existing:
        return candidate
    index = 1
    while f"{candidate}_{index}" in existing:
        index += 1
    return f"{candidate}_{index}"


def load_bounded_atlas_frame(path: Path, sql: str | None, sample_limit: int) -> Any:
    """Load at most ``sample_limit`` post-query rows into a DataFrame.

    Sampling and limiting execute inside DuckDB, so candidate rows outside the
    deterministic sample are never materialized as a pandas DataFrame. The
    returned frame is owned by the caller.

    Raises:
        ValueError: ``sample_limit`` is not positive.
        duckdb.Error: The dataset or guarded query cannot be read.
    """
    if sample_limit < 1:
        raise ValueError("sample_limit must be greater than or equal to 1")
    relation = relation_sql_literal(path)
    with open_connection() as connection:
        configure_duckdb_limits(connection)
        described = connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        source_columns = {str(row[0]) for row in described}
        file_column = _unique_column_name(source_columns, ATLAS_SOURCE_FILE_COLUMN)
        connection.execute(f"CREATE TEMP VIEW data AS SELECT *, {quote_literal(str(path))} AS {quote_ident(file_column)} FROM {relation}")
        source_sql = sql or "SELECT * FROM data"
        result_columns = {str(row[0]) for row in connection.execute(f"DESCRIBE SELECT * FROM ({source_sql}) AS atlas_source").fetchall()}
        ordinal_column = _unique_column_name(result_columns, ATLAS_INTERNAL_ORDINAL_COLUMN)
        quoted_ordinal = quote_ident(ordinal_column)
        query = f"""
            WITH atlas_source AS ({source_sql}),
            atlas_numbered AS (
                SELECT *, row_number() OVER () AS {quoted_ordinal}
                FROM atlas_source
            )
            SELECT * EXCLUDE ({quoted_ordinal})
            FROM atlas_numbered
            ORDER BY hash({quoted_ordinal}, {ATLAS_SAMPLE_RANDOM_STATE})
            LIMIT {int(sample_limit)}
        """
        return connection.execute(query).df()

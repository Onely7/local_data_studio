"""DuckDB helpers for reading datasets and building responses."""

from pathlib import Path
from typing import Any

import duckdb
from fastapi import HTTPException

from .config import DEFAULT_LIMIT, MAX_LIMIT
from .deleted_rows import deleted_row_ids_for
from .serialization import serialize_rows


def quote_ident(name: str) -> str:
    """Escape an identifier for safe interpolation into SQL."""
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Escape a literal string for safe interpolation into SQL."""
    return "'" + value.replace("'", "''") + "'"


def relation_sql(path: Path) -> tuple[str, list[Any]]:
    """Return a parameterized DuckDB relation for a dataset file."""
    ext = path.suffix.lower()
    if ext == ".parquet":
        return "read_parquet(?)", [str(path)]
    if ext in {".csv", ".tsv"}:
        if ext == ".tsv":
            return "read_csv_auto(?, delim='\t')", [str(path)]
        return "read_csv_auto(?)", [str(path)]
    if ext in {".json", ".jsonl"}:
        return "read_json_auto(?)", [str(path)]
    raise HTTPException(status_code=400, detail="unsupported file extension")


def relation_sql_literal(path: Path) -> str:
    """Return a literal DuckDB relation string for a dataset file."""
    ext = path.suffix.lower()
    path_literal = quote_literal(str(path))
    if ext == ".parquet":
        return f"read_parquet({path_literal})"
    if ext in {".csv", ".tsv"}:
        if ext == ".tsv":
            return f"read_csv_auto({path_literal}, delim='\\t')"
        return f"read_csv_auto({path_literal})"
    if ext in {".json", ".jsonl"}:
        return f"read_json_auto({path_literal})"
    raise HTTPException(status_code=400, detail="unsupported file extension")


def relation_with_rowid_sql(path: Path, deleted_ids: list[int]) -> tuple[str, list[Any]]:
    """Return a relation with __rowid applied and session deletes excluded."""
    rel_sql, params = relation_sql(path)
    base = f"SELECT *, row_number() OVER () AS __rowid FROM {rel_sql}"
    if deleted_ids:
        placeholders = ", ".join(["?"] * len(deleted_ids))
        base = f"SELECT * FROM ({base}) WHERE __rowid NOT IN ({placeholders})"
        params = params + deleted_ids
    return base, params


def relation_with_rowid_literal(path: Path, deleted_ids: list[int]) -> str:
    """Return a literal relation with __rowid and exclusions applied."""
    base = f"SELECT *, row_number() OVER () AS __rowid FROM {relation_sql_literal(path)}"
    if deleted_ids:
        id_list = ", ".join(str(int(row_id)) for row_id in deleted_ids if row_id > 0)
        if id_list:
            base = f"SELECT * FROM ({base}) WHERE __rowid NOT IN ({id_list})"
    return base


def split_row_ids(columns: list[str], rows: list[tuple[Any, ...]]) -> tuple[list[str], list[list[Any]], list[int]]:
    """Split __rowid out of the row tuples for API responses."""
    if "__rowid" not in columns:
        return columns, [list(row) for row in rows], []
    idx = columns.index("__rowid")
    row_ids: list[int] = []
    cleaned_rows: list[list[Any]] = []
    for row in rows:
        row_ids.append(int(row[idx]))
        cleaned_rows.append(list(row[:idx]) + list(row[idx + 1 :]))
    cleaned_columns = columns[:idx] + columns[idx + 1 :]
    return cleaned_columns, cleaned_rows, row_ids


def clamp_limit(limit: int | None) -> int:
    """Clamp a page size to configured bounds."""
    if limit is None:
        return DEFAULT_LIMIT
    return max(1, min(MAX_LIMIT, limit))


def normalize_pagination(limit: int | None, offset: int | None) -> tuple[int, int]:
    """Normalize pagination inputs into safe limit/offset values."""
    limit_value = clamp_limit(limit)
    offset_value = max(0, offset or 0)
    return limit_value, offset_value


def build_table_response(
    file: str,
    columns: list[str],
    rows: list[list[Any]],
    limit_value: int,
    offset_value: int,
    row_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Standard response payload for tabular endpoints."""
    return {
        "file": file,
        "columns": columns,
        "rows": rows,
        "row_ids": row_ids or [],
        "limit": limit_value,
        "offset": offset_value,
    }


def open_connection() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection."""
    return duckdb.connect(database=":memory:")


def count_relation_rows(path: Path) -> int:
    """Count rows for a dataset, respecting session deletions."""
    deleted_ids = deleted_row_ids_for(path)
    if deleted_ids:
        rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
        query = f"SELECT COUNT(*) FROM ({rel_sql})"
    else:
        rel_sql, params = relation_sql(path)
        query = f"SELECT COUNT(*) FROM {rel_sql}"
    with open_connection() as con:
        row = con.execute(query, params).fetchone()
        if row is None:
            return 0
        return row[0]


def count_relation_rows_raw(path: Path) -> int:
    """Count rows for a dataset without session deletions."""
    rel_sql, params = relation_sql(path)
    with open_connection() as con:
        row = con.execute(f"SELECT COUNT(*) FROM {rel_sql}", params).fetchone()
        if row is None:
            return 0
        return row[0]


def describe_relation(con: duckdb.DuckDBPyConnection, rel_sql: str, params: list[Any]) -> list[dict[str, str]]:
    """Return column name/type metadata for a relation."""
    result = con.execute(f"DESCRIBE SELECT * FROM {rel_sql}", params).fetchall()
    return [{"name": row[0], "type": row[1]} for row in result]


def fetch_rows(con: duckdb.DuckDBPyConnection, query: str, params: list[Any]) -> tuple[list[str], list[list[Any]]]:
    """Execute a query and serialize result rows."""
    result = con.execute(query, params)
    description = result.description or []
    columns = [desc[0] for desc in description]
    rows = serialize_rows(result.fetchall())
    return columns, rows


def fetch_rows_with_rowid(con: duckdb.DuckDBPyConnection, query: str, params: list[Any]) -> tuple[list[str], list[list[Any]], list[int]]:
    """Execute a query, split __rowid, and serialize results."""
    result = con.execute(query, params)
    description = result.description or []
    columns = [desc[0] for desc in description]
    raw_rows = result.fetchall()
    columns, rows, row_ids = split_row_ids(columns, raw_rows)
    rows = serialize_rows(rows)
    return columns, rows, row_ids

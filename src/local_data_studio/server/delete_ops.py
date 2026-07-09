"""Delete operations for rows/columns with optional file persistence."""

import json
import os
import tempfile
from pathlib import Path

from fastapi import HTTPException

from .db import (
    describe_relation,
    open_connection,
    quote_ident,
    quote_literal,
    relation_sql,
    relation_sql_literal,
)


def delete_row_via_duckdb(path: Path, row_id: int) -> None:
    """Delete a row from CSV/TSV/Parquet via DuckDB copy-out."""
    rel_literal = relation_sql_literal(path)
    exists_sql = f"SELECT COUNT(*) FROM (SELECT row_number() OVER () AS __rowid FROM {rel_literal}) WHERE __rowid = {row_id}"
    query = f"SELECT * EXCLUDE(__rowid) FROM (SELECT *, row_number() OVER () AS __rowid FROM {rel_literal}) WHERE __rowid <> {row_id}"
    ext = path.suffix.lower()
    if ext == ".parquet":
        format_opts = "FORMAT PARQUET"
    elif ext == ".csv":
        format_opts = "FORMAT CSV, HEADER TRUE"
    elif ext == ".tsv":
        format_opts = "FORMAT CSV, HEADER TRUE, DELIMITER '\\t'"
    else:
        raise HTTPException(status_code=400, detail="unsupported file extension")

    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix)
    temp_path = Path(temp_file.name)
    temp_file.close()

    try:
        with open_connection() as con:
            row = con.execute(exists_sql).fetchone()
            exists = int(row[0]) if row else 0
            if not exists:
                raise HTTPException(status_code=404, detail="row not found")
            con.execute(f"COPY ({query}) TO {quote_literal(str(temp_path))} ({format_opts})")
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def delete_row_jsonl(path: Path, row_id: int) -> None:
    """Delete a row from a JSONL file by line number."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix)
    temp_path = Path(temp_file.name)
    temp_file.close()

    removed = False
    index = 0
    try:
        with path.open("r", encoding="utf-8") as src, temp_path.open("w", encoding="utf-8") as dst:
            for line in src:
                if not line.strip():
                    continue
                index += 1
                if index == row_id:
                    removed = True
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise HTTPException(status_code=400, detail="invalid jsonl format") from exc
                dst.write(json.dumps(obj, ensure_ascii=False))
                dst.write("\n")
        if not removed:
            raise HTTPException(status_code=404, detail="row not found")
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def delete_row_json(path: Path, row_id: int) -> None:
    """Delete a row from a JSON list file by index."""
    try:
        with path.open("r", encoding="utf-8") as src:
            data = json.load(src)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc
    if not isinstance(data, list):
        raise HTTPException(status_code=400, detail="json root must be a list")
    if row_id < 1 or row_id > len(data):
        raise HTTPException(status_code=404, detail="row not found")
    data.pop(row_id - 1)

    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix)
    temp_path = Path(temp_file.name)
    temp_file.close()
    try:
        with temp_path.open("w", encoding="utf-8") as dst:
            json.dump(data, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def delete_row_from_file(path: Path, row_id: int) -> None:
    """Delete a row from a supported dataset file."""
    ext = path.suffix.lower()
    if ext in {".csv", ".tsv", ".parquet"}:
        delete_row_via_duckdb(path, row_id)
        return
    if ext == ".jsonl":
        delete_row_jsonl(path, row_id)
        return
    if ext == ".json":
        delete_row_json(path, row_id)
        return
    raise HTTPException(status_code=400, detail="unsupported file extension")


def delete_column_via_duckdb(path: Path, column: str) -> None:
    """Delete a column from CSV/TSV/Parquet via DuckDB copy-out."""
    rel_sql, params = relation_sql(path)
    with open_connection() as con:
        columns = describe_relation(con, rel_sql, params)
    column_names = [item["name"] for item in columns]
    if column not in column_names:
        raise HTTPException(status_code=404, detail="column not found")
    if len(column_names) <= 1:
        raise HTTPException(status_code=400, detail="cannot delete last column")

    rel_literal = relation_sql_literal(path)
    query = f"SELECT * EXCLUDE({quote_ident(column)}) FROM {rel_literal}"

    ext = path.suffix.lower()
    if ext == ".parquet":
        format_opts = "FORMAT PARQUET"
    elif ext == ".csv":
        format_opts = "FORMAT CSV, HEADER TRUE"
    elif ext == ".tsv":
        format_opts = "FORMAT CSV, HEADER TRUE, DELIMITER '\\t'"
    else:
        raise HTTPException(status_code=400, detail="unsupported file extension")

    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix)
    temp_path = Path(temp_file.name)
    temp_file.close()
    try:
        with open_connection() as con:
            con.execute(f"COPY ({query}) TO {quote_literal(str(temp_path))} ({format_opts})")
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def delete_column_jsonl(path: Path, column: str) -> None:
    """Delete a column from all JSONL objects."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix)
    temp_path = Path(temp_file.name)
    temp_file.close()

    removed = False
    try:
        with path.open("r", encoding="utf-8") as src, temp_path.open("w", encoding="utf-8") as dst:
            for line in src:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise HTTPException(status_code=400, detail="invalid jsonl format") from exc
                if not isinstance(obj, dict):
                    raise HTTPException(status_code=400, detail="jsonl rows must be objects")
                if column in obj:
                    removed = True
                    obj.pop(column, None)
                dst.write(json.dumps(obj, ensure_ascii=False))
                dst.write("\n")
        if not removed:
            raise HTTPException(status_code=404, detail="column not found")
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def delete_column_json(path: Path, column: str) -> None:
    """Delete a column from a JSON object or list of objects."""
    try:
        with path.open("r", encoding="utf-8") as src:
            data = json.load(src)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc
    removed = False
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                raise HTTPException(status_code=400, detail="json rows must be objects")
            if column in row:
                removed = True
                row.pop(column, None)
    elif isinstance(data, dict):
        if column in data:
            removed = True
            data.pop(column, None)
        else:
            removed = False
    else:
        raise HTTPException(status_code=400, detail="json root must be an object or list")

    if not removed:
        raise HTTPException(status_code=404, detail="column not found")

    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix)
    temp_path = Path(temp_file.name)
    temp_file.close()
    try:
        with temp_path.open("w", encoding="utf-8") as dst:
            json.dump(data, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def delete_column_from_file(path: Path, column: str) -> None:
    """Delete a column from a supported dataset file."""
    ext = path.suffix.lower()
    if ext in {".csv", ".tsv", ".parquet"}:
        delete_column_via_duckdb(path, column)
        return
    if ext == ".jsonl":
        delete_column_jsonl(path, column)
        return
    if ext == ".json":
        delete_column_json(path, column)
        return
    raise HTTPException(status_code=400, detail="unsupported file extension")

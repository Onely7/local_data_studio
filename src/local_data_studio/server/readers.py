"""Format-aware dataset readers for lightweight preview and scans."""

from __future__ import annotations

import base64
import binascii
import csv
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Protocol

from fastapi import HTTPException

from .cache import metadata_cache_path
from .config import COLUMN_LIMIT_WARNING, DEFAULT_LIMIT, MAX_COLUMNS, MAX_JSON_PREVIEW_BYTES, MAX_OFFSET_FALLBACK, PARQUET_PREVIEW_BATCH_SIZE
from .db import (
    build_table_response,
    describe_relation,
    fetch_rows_with_rowid,
    normalize_pagination,
    open_connection,
    relation_sql,
    relation_with_rowid_sql,
)
from .line_index import LineOffsetIndex
from .serialization import serialize_value

LINE_DATASET_EXTENSIONS = {".jsonl", ".csv", ".tsv"}
PARQUET_EXTENSION = ".parquet"
TB_SAFE_EXTENSIONS = {".jsonl", ".csv", ".tsv", ".parquet"}
JSON_NOT_TB_SAFE_WARNING = "JSON array/object files are not TB-friendly. Convert large datasets to JSONL or Parquet for bounded preview."
CSV_FIELD_SIZE_LOCK = Lock()


class ScanControl(Protocol):
    """Progress and cancellation contract for long-running dataset scans."""

    def check_cancelled(self) -> None: ...

    def update(self, *, progress: float | None = None, message: str | None = None) -> None: ...


@dataclass(frozen=True, slots=True)
class DatasetMetadata:
    """Cached lightweight dataset metadata used by the UI."""

    file_format: str
    columns: list[dict[str, str]]
    warning: str | None = None

    def to_response(self, file_name: str) -> dict[str, Any]:
        response: dict[str, Any] = {
            "file": file_name,
            "format": self.file_format,
            "columns": self.columns,
        }
        if self.warning:
            response["warning"] = self.warning
        return response


def _merge_warnings(*warnings: str | None) -> str | None:
    active_warnings = [warning for warning in warnings if warning]
    if not active_warnings:
        return None
    return " ".join(dict.fromkeys(active_warnings))


def _limit_metadata_columns(metadata: DatasetMetadata) -> DatasetMetadata:
    if len(metadata.columns) <= MAX_COLUMNS:
        return metadata
    return DatasetMetadata(
        file_format=metadata.file_format,
        columns=metadata.columns[:MAX_COLUMNS],
        warning=_merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING),
    )


def _mark_columns_truncated(response: dict[str, Any], total_columns: int | None = None) -> None:
    response["columns_truncated"] = True
    if total_columns is not None:
        response["total_columns"] = total_columns


def _encode_page_token(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_page_token(token: str | None) -> dict[str, Any]:
    if not token:
        return {}
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii"))
        payload = json.loads(decoded.decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="invalid page token") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid page token")
    return payload


def _decode_page_token_for(token: str | None, expected_kind: str) -> dict[str, Any]:
    payload = _decode_page_token(token)
    kind = payload.get("kind")
    if kind is not None and kind != expected_kind:
        raise HTTPException(status_code=400, detail="invalid page token")
    return payload


def _token_int(payload: dict[str, Any], key: str, default: int, *, minimum: int = 0) -> int:
    raw_value = payload.get(key, default)
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="invalid page token") from exc
    if value < minimum:
        raise HTTPException(status_code=400, detail="invalid page token")
    return value


def _visible_row_count(total_rows: int, deleted_ids: list[int] | None) -> int:
    deleted_existing_rows = {row_id for row_id in deleted_ids or [] if 1 <= row_id <= total_rows}
    return max(0, total_rows - len(deleted_existing_rows))


def _reject_large_offset_without_token(offset: int, page_token: str | None) -> None:
    if page_token or offset <= MAX_OFFSET_FALLBACK:
        return
    raise HTTPException(
        status_code=400,
        detail="offset pagination is limited for large datasets; use page_token navigation",
    )


def _indexed_line_start(path: Path, target_row_number: int, hidden_row_ids: set[int]) -> tuple[int, int, int]:
    index = LineOffsetIndex(path)
    indexed = index.nearest_before(target_row_number)
    if indexed is None:
        return 0, 1, 0

    visible_rows_before_index = indexed.line_number - 1
    if hidden_row_ids:
        visible_rows_before_index -= sum(1 for row_id in hidden_row_ids if row_id < indexed.line_number)
    return indexed.byte_offset, indexed.line_number, max(0, visible_rows_before_index)


def _line_cursor_for_offset(path: Path, offset: int, *, first_data_offset: int = 0, deleted_ids: set[int] | None = None) -> tuple[int, int]:
    """Return byte offset and 1-based row id after skipping offset visible rows."""
    if offset <= 0:
        return first_data_offset, 1

    hidden_row_ids = deleted_ids or set()
    start_offset, next_row_number, visible_rows_skipped = _indexed_line_start(path, offset + 1, hidden_row_ids)
    byte_offset = max(first_data_offset, start_offset)
    if byte_offset == first_data_offset and start_offset < first_data_offset:
        next_row_number = 1
        visible_rows_skipped = 0

    index = LineOffsetIndex(path)
    with path.open("rb") as file:
        file.seek(byte_offset)
        while visible_rows_skipped < offset:
            line_start = file.tell()
            line = file.readline()
            if not line:
                return file.tell(), next_row_number
            if not line.strip():
                continue
            index.record(next_row_number, line_start)
            if next_row_number not in hidden_row_ids:
                visible_rows_skipped += 1
            next_row_number += 1
        return file.tell(), next_row_number


def _format_name(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".tsv":
        return "tsv"
    if suffix == ".csv":
        return "csv"
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".json":
        return "json"
    return suffix.lstrip(".") or "unknown"


def _infer_type_name(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    type_checks: tuple[tuple[Callable[[Any], bool], str], ...] = (
        (lambda item: isinstance(item, bool), "BOOLEAN"),
        (lambda item: isinstance(item, int) and not isinstance(item, bool), "BIGINT"),
        (lambda item: isinstance(item, float), "DOUBLE"),
        (lambda item: isinstance(item, list), "LIST"),
        (lambda item: isinstance(item, dict), "STRUCT"),
    )
    for matches, type_name in type_checks:
        if matches(value):
            return type_name
    return "VARCHAR"


def _merge_column_type(current: str | None, value: Any) -> str:
    inferred = _infer_type_name(value)
    if current is None or current == "UNKNOWN":
        return inferred
    if inferred in ("UNKNOWN", current):
        return current
    return "VARCHAR"


def _load_cached_metadata(path: Path) -> DatasetMetadata | None:
    cache_path = metadata_cache_path(path)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    columns = payload.get("columns")
    if not isinstance(columns, list):
        return None
    return _limit_metadata_columns(
        DatasetMetadata(
            file_format=str(payload.get("format") or _format_name(path)),
            columns=[
                {"name": str(item["name"]), "type": str(item["type"])} for item in columns if isinstance(item, dict) and "name" in item and "type" in item
            ],
            warning=payload.get("warning") if isinstance(payload.get("warning"), str) else None,
        )
    )


def _write_cached_metadata(path: Path, metadata: DatasetMetadata) -> None:
    cache_path = metadata_cache_path(path)
    payload = {
        "format": metadata.file_format,
        "columns": metadata.columns,
        "warning": metadata.warning,
    }
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_parquet_metadata(path: Path) -> DatasetMetadata:
    import pyarrow.parquet as pq  # noqa: PLC0415

    parquet_file = pq.ParquetFile(path)
    columns = []
    for index, field in enumerate(parquet_file.schema_arrow):
        if index >= MAX_COLUMNS:
            break
        columns.append({"name": field.name, "type": str(field.type)})
    warning = COLUMN_LIMIT_WARNING if len(parquet_file.schema_arrow) > MAX_COLUMNS else None
    return DatasetMetadata(file_format="parquet", columns=columns, warning=warning)


def _read_delimited_header(path: Path, delimiter: str) -> tuple[list[str], int]:
    with path.open("rb") as file:
        header_bytes = file.readline()
        header_end = file.tell()
    if not header_bytes:
        return [], 0
    header_text = header_bytes.decode("utf-8-sig", "replace").rstrip("\r\n")
    header = _parse_delimited_record(header_text, delimiter)
    columns = [name or f"column_{index + 1}" for index, name in enumerate(header)]
    return columns, header_end


def _parse_delimited_record(text: str, delimiter: str) -> list[str]:
    """Parse one CSV/TSV record without the stdlib's small default field cap."""
    with CSV_FIELD_SIZE_LOCK:
        previous_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(min(len(text), sys.maxsize))
            return next(csv.reader([text], delimiter=delimiter), [])
        finally:
            csv.field_size_limit(previous_limit)


def _load_delimited_metadata(path: Path, delimiter: str) -> DatasetMetadata:
    columns, _ = _read_delimited_header(path, delimiter)
    return DatasetMetadata(
        file_format="tsv" if delimiter == "\t" else "csv",
        columns=[{"name": column, "type": "VARCHAR"} for column in columns[:MAX_COLUMNS]],
        warning=COLUMN_LIMIT_WARNING if len(columns) > MAX_COLUMNS else None,
    )


def _load_jsonl_metadata(path: Path, sample_rows: int = 100) -> DatasetMetadata:
    column_types: dict[str, str] = {}
    columns_truncated = False
    sampled = 0
    with path.open("rb") as file:
        while sampled < sample_rows:
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            sampled += 1
            if isinstance(value, dict):
                for key, item in value.items():
                    name = str(key)
                    if name not in column_types and len(column_types) >= MAX_COLUMNS:
                        columns_truncated = True
                        continue
                    column_types[name] = _merge_column_type(column_types.get(name), item)
            else:
                column_types["value"] = _merge_column_type(column_types.get("value"), value)
    columns = [{"name": name, "type": type_name} for name, type_name in column_types.items()]
    return DatasetMetadata(file_format="jsonl", columns=columns, warning=COLUMN_LIMIT_WARNING if columns_truncated else None)


def _load_json_metadata(path: Path) -> DatasetMetadata:
    return DatasetMetadata(
        file_format="json",
        columns=[{"name": "value", "type": "JSON"}],
        warning=JSON_NOT_TB_SAFE_WARNING,
    )


def _load_duckdb_metadata(path: Path) -> DatasetMetadata:
    if path.suffix.lower() not in TB_SAFE_EXTENSIONS:
        return DatasetMetadata(
            file_format=_format_name(path),
            columns=[{"name": "value", "type": "UNKNOWN"}],
            warning="This format is not TB-safe for schema inference.",
        )
    rel_sql, params = relation_sql(path)
    with open_connection() as con:
        columns = describe_relation(con, rel_sql, params)
    warning = None
    if path.suffix.lower() == ".json":
        warning = "Large JSON arrays are not TB-friendly. Prefer JSONL or Parquet for scalable preview."
    return DatasetMetadata(file_format=_format_name(path), columns=columns, warning=warning)


def load_dataset_metadata(path: Path, *, use_cache: bool = True) -> DatasetMetadata:
    """Load lightweight schema metadata without scanning the full dataset when possible."""
    if use_cache:
        cached = _load_cached_metadata(path)
        if cached is not None:
            return cached

    suffix = path.suffix.lower()
    if suffix == ".parquet":
        metadata = _load_parquet_metadata(path)
    elif suffix == ".jsonl":
        metadata = _load_jsonl_metadata(path)
    elif suffix == ".csv":
        metadata = _load_delimited_metadata(path, ",")
    elif suffix == ".tsv":
        metadata = _load_delimited_metadata(path, "\t")
    elif suffix == ".json":
        metadata = _load_json_metadata(path)
    else:
        metadata = _load_duckdb_metadata(path)

    metadata = _limit_metadata_columns(metadata)
    _write_cached_metadata(path, metadata)
    return metadata


def _row_from_mapping(columns: list[str], value: Any) -> list[Any]:
    if isinstance(value, dict):
        return [serialize_value(value.get(column)) for column in columns]
    return [serialize_value(value)]


def _extend_columns_from_value(columns: list[str], value: Any) -> tuple[list[str], bool]:
    if not isinstance(value, dict):
        return columns or ["value"], False
    next_columns = list(columns)
    existing_columns = set(next_columns)
    truncated = False
    for key in value:
        name = str(key)
        if name in existing_columns:
            continue
        if len(next_columns) >= MAX_COLUMNS:
            truncated = True
            continue
        next_columns.append(name)
        existing_columns.add(name)
    return next_columns, truncated


def _align_existing_rows(rows: list[list[Any]], old_column_count: int, new_column_count: int) -> None:
    if new_column_count <= old_column_count:
        return
    for row in rows:
        row.extend([None] * (new_column_count - old_column_count))


def _json_preview(file_name: str, path: Path, limit: int) -> dict[str, Any]:
    if path.stat().st_size > MAX_JSON_PREVIEW_BYTES:
        response = build_table_response(file_name, ["value"], [], limit, 0, [])
        response.update(
            {
                "next_page_token": None,
                "has_next": False,
                "warning": JSON_NOT_TB_SAFE_WARNING,
            }
        )
        return response

    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc

    values = payload[:limit] if isinstance(payload, list) else [payload]
    columns: list[str] = []
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    columns_truncated = False

    for index, value in enumerate(values, start=1):
        old_column_count = len(columns)
        columns, row_columns_truncated = _extend_columns_from_value(columns, value)
        columns_truncated = columns_truncated or row_columns_truncated
        _align_existing_rows(rows, old_column_count, len(columns))
        rows.append(_row_from_mapping(columns, value))
        row_ids.append(index)

    response = build_table_response(file_name, columns or ["value"], rows, limit, 0, row_ids)
    warning = _merge_warnings(JSON_NOT_TB_SAFE_WARNING, COLUMN_LIMIT_WARNING if columns_truncated else None)
    if columns_truncated:
        _mark_columns_truncated(response)
    response.update(
        {
            "next_page_token": None,
            "has_next": isinstance(payload, list) and len(payload) > limit,
            "warning": warning,
        }
    )
    return response


def _raw_row_values(value: Any) -> tuple[list[str], list[Any]]:
    if isinstance(value, dict):
        columns = [str(key) for key in value]
        return columns, [value[key] for key in value]
    return ["value"], [value]


def _raw_line_value(path: Path, row_id: int, *, first_data_offset: int = 0) -> bytes:
    index = LineOffsetIndex(path)
    indexed = index.nearest_before(row_id)
    byte_offset = max(first_data_offset, indexed.byte_offset) if indexed else first_data_offset
    row_number = indexed.line_number if indexed else 1

    with path.open("rb") as file:
        file.seek(byte_offset)
        while True:
            line_start = file.tell()
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            index.record(row_number, line_start)
            if row_number == row_id:
                return line
            row_number += 1
    raise HTTPException(status_code=404, detail="row not found")


def _raw_jsonl_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    line = _raw_line_value(path, row_id)
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid jsonl format") from exc
    return _raw_row_values(value)


def _raw_delimited_row(path: Path, row_id: int, delimiter: str) -> tuple[list[str], list[Any]]:
    columns, first_data_offset = _read_delimited_header(path, delimiter)
    line = _raw_line_value(path, row_id, first_data_offset=first_data_offset)
    text = line.decode("utf-8-sig", "replace").rstrip("\r\n")
    values = _parse_delimited_record(text, delimiter)
    return columns, values + [None] * max(0, len(columns) - len(values))


def _raw_parquet_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    import pyarrow.parquet as pq  # noqa: PLC0415

    parquet_file = pq.ParquetFile(path)
    columns = parquet_file.schema_arrow.names
    target_offset = row_id - 1
    for row_group in range(parquet_file.num_row_groups):
        group_rows = parquet_file.metadata.row_group(row_group).num_rows
        if target_offset >= group_rows:
            target_offset -= group_rows
            continue
        for batch in parquet_file.iter_batches(batch_size=1, row_groups=[row_group], columns=columns):
            if target_offset:
                target_offset -= batch.num_rows
                continue
            record = batch.to_pylist()[0]
            return columns, [record.get(column) for column in columns]
    raise HTTPException(status_code=404, detail="row not found")


def _raw_json_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    if path.stat().st_size > MAX_JSON_PREVIEW_BYTES:
        raise HTTPException(status_code=400, detail=JSON_NOT_TB_SAFE_WARNING)
    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid json format") from exc
    if isinstance(payload, list):
        if row_id > len(payload):
            raise HTTPException(status_code=404, detail="row not found")
        return _raw_row_values(payload[row_id - 1])
    if row_id != 1:
        raise HTTPException(status_code=404, detail="row not found")
    return _raw_row_values(payload)


def fetch_raw_row(path: Path, row_id: int) -> tuple[list[str], list[Any]]:
    """Read one user-selected row without Preview response truncation."""
    if row_id < 1:
        raise HTTPException(status_code=400, detail="row_id must be positive")
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return _raw_jsonl_row(path, row_id)
    if suffix == ".csv":
        return _raw_delimited_row(path, row_id, ",")
    if suffix == ".tsv":
        return _raw_delimited_row(path, row_id, "\t")
    if suffix == ".parquet":
        return _raw_parquet_row(path, row_id)
    if suffix == ".json":
        return _raw_json_row(path, row_id)
    raise HTTPException(status_code=400, detail="raw row retrieval is not supported for this file format")


def _jsonl_preview(file_name: str, path: Path, limit: int, offset: int, page_token: str | None, deleted_ids: set[int]) -> dict[str, Any]:
    token = _decode_page_token_for(page_token, "jsonl")
    if page_token:
        byte_offset = _token_int(token, "byte_offset", 0)
        row_number = _token_int(token, "row_number", 1, minimum=1)
    else:
        byte_offset, row_number = _line_cursor_for_offset(path, offset, deleted_ids=deleted_ids)
    metadata = load_dataset_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    next_byte_offset = byte_offset
    next_row_number = row_number
    columns_truncated = False

    with path.open("rb") as file:
        file.seek(byte_offset)
        while len(rows) < limit:
            line = file.readline()
            if not line:
                next_byte_offset = file.tell()
                break
            next_byte_offset = file.tell()
            if not line.strip():
                continue
            current_row_id = next_row_number
            next_row_number += 1
            if current_row_id in deleted_ids:
                continue
            value = json.loads(line)
            old_column_count = len(columns)
            columns, row_columns_truncated = _extend_columns_from_value(columns, value)
            columns_truncated = columns_truncated or row_columns_truncated
            _align_existing_rows(rows, old_column_count, len(columns))
            rows.append(_row_from_mapping(columns, value))
            row_ids.append(current_row_id)

    has_next = next_byte_offset < path.stat().st_size
    next_token = _encode_page_token({"kind": "jsonl", "byte_offset": next_byte_offset, "row_number": next_row_number}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, max(0, row_number - 1), row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = _merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated or (metadata.warning and COLUMN_LIMIT_WARNING in metadata.warning):
        _mark_columns_truncated(response)
    return response


def _delimited_preview(
    file_name: str,
    path: Path,
    *,
    delimiter: str,
    limit: int,
    offset: int,
    page_token: str | None,
    deleted_ids: set[int],
) -> dict[str, Any]:
    source_columns, first_data_offset = _read_delimited_header(path, delimiter)
    columns = source_columns[:MAX_COLUMNS]
    columns_truncated = len(source_columns) > MAX_COLUMNS
    kind = "tsv" if delimiter == "\t" else "csv"
    token = _decode_page_token_for(page_token, kind)
    if page_token:
        byte_offset = _token_int(token, "byte_offset", first_data_offset)
        row_number = _token_int(token, "row_number", 1, minimum=1)
    else:
        byte_offset, row_number = _line_cursor_for_offset(path, offset, first_data_offset=first_data_offset, deleted_ids=deleted_ids)
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    next_byte_offset = byte_offset
    next_row_number = row_number

    with path.open("rb") as file:
        file.seek(byte_offset)
        while len(rows) < limit:
            line = file.readline()
            if not line:
                next_byte_offset = file.tell()
                break
            next_byte_offset = file.tell()
            if not line.strip():
                continue
            current_row_id = next_row_number
            next_row_number += 1
            if current_row_id in deleted_ids:
                continue
            text = line.decode("utf-8-sig", "replace").rstrip("\r\n")
            values = next(csv.reader([text], delimiter=delimiter), [])
            padded = values + [None] * max(0, len(columns) - len(values))
            rows.append([serialize_value(value) for value in padded[: len(columns)]])
            row_ids.append(current_row_id)

    has_next = next_byte_offset < path.stat().st_size
    next_token = _encode_page_token({"kind": kind, "byte_offset": next_byte_offset, "row_number": next_row_number}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, max(0, row_number - 1), row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = _merge_warnings(COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated:
        _mark_columns_truncated(response, len(source_columns))
    return response


def _parquet_cursor_for_offset(parquet_file: Any, offset: int, deleted_ids: set[int]) -> tuple[int, int, int]:
    if offset <= 0:
        return 0, 0, 1

    visible_rows_skipped = 0
    absolute_row = 1
    for row_group in range(parquet_file.num_row_groups):
        group_rows = parquet_file.metadata.row_group(row_group).num_rows
        for row_offset in range(group_rows):
            if absolute_row not in deleted_ids:
                if visible_rows_skipped >= offset:
                    return row_group, row_offset, absolute_row
                visible_rows_skipped += 1
            absolute_row += 1
    return parquet_file.num_row_groups, 0, absolute_row


def _read_parquet_group_slice(parquet_file: Any, row_group: int, row_offset: int, limit: int, columns: list[str]) -> tuple[list[dict[str, Any]], int]:
    """Read up to limit records from a row group without materializing the whole group."""
    if limit <= 0:
        return [], row_offset

    records: list[dict[str, Any]] = []
    batch_start = 0
    next_row_offset = row_offset
    for batch in parquet_file.iter_batches(batch_size=max(PARQUET_PREVIEW_BATCH_SIZE, limit), row_groups=[row_group], columns=columns):
        batch_length = batch.num_rows
        batch_end = batch_start + batch_length
        if batch_end <= row_offset:
            batch_start = batch_end
            continue

        local_start = max(0, row_offset - batch_start)
        local_length = min(limit - len(records), batch_length - local_start)
        if local_length <= 0:
            break

        records.extend(batch.slice(local_start, local_length).to_pylist())
        next_row_offset = batch_start + local_start + local_length
        if len(records) >= limit:
            break
        batch_start = batch_end

    return records, next_row_offset


def _parquet_preview(file_name: str, path: Path, limit: int, offset: int, page_token: str | None, deleted_ids: set[int]) -> dict[str, Any]:
    import pyarrow.parquet as pq  # noqa: PLC0415

    token = _decode_page_token_for(page_token, "parquet")
    parquet_file = pq.ParquetFile(path)
    source_columns = parquet_file.schema_arrow.names
    columns = source_columns[:MAX_COLUMNS]
    columns_truncated = len(source_columns) > MAX_COLUMNS
    if page_token:
        row_group = _token_int(token, "row_group", 0)
        row_offset = _token_int(token, "row_offset", 0)
        absolute_row = _token_int(token, "absolute_row", 1, minimum=1)
    else:
        row_group, row_offset, absolute_row = _parquet_cursor_for_offset(parquet_file, offset, deleted_ids)
    current_row_group = row_group
    current_row_offset = row_offset
    current_absolute_row = absolute_row
    rows: list[list[Any]] = []
    row_ids: list[int] = []

    while len(rows) < limit and current_row_group < parquet_file.num_row_groups:
        group_metadata = parquet_file.metadata.row_group(current_row_group)
        group_rows = group_metadata.num_rows
        if current_row_offset >= group_rows:
            current_row_group += 1
            current_row_offset = 0
            continue

        remaining = limit - len(rows)
        records, next_group_offset = _read_parquet_group_slice(parquet_file, current_row_group, current_row_offset, remaining, columns)
        for record in records:
            current_row_id = current_absolute_row
            current_absolute_row += 1
            current_row_offset += 1
            if current_row_id in deleted_ids:
                continue
            rows.append([serialize_value(record.get(column)) for column in columns])
            row_ids.append(current_row_id)
            if len(rows) >= limit:
                break

        if not records:
            current_row_offset = group_rows
        else:
            current_row_offset = max(current_row_offset, next_group_offset)

        if current_row_offset >= group_rows:
            current_row_group += 1
            current_row_offset = 0

    has_next = current_row_group < parquet_file.num_row_groups
    next_token = (
        _encode_page_token(
            {
                "kind": "parquet",
                "row_group": current_row_group,
                "row_offset": current_row_offset,
                "absolute_row": current_absolute_row,
            }
        )
        if has_next
        else None
    )
    response = build_table_response(file_name, columns, rows, limit, absolute_row - 1, row_ids)
    response.update({"next_page_token": next_token, "has_next": has_next})
    warning = _merge_warnings(COLUMN_LIMIT_WARNING if columns_truncated else None, response.get("warning"))
    if warning:
        response["warning"] = warning
    if columns_truncated:
        _mark_columns_truncated(response, len(source_columns))
    return response


def _duckdb_preview(file_name: str, path: Path, limit: int, offset: int, page_token: str | None, deleted_ids: list[int]) -> dict[str, Any]:
    token = _decode_page_token_for(page_token, "duckdb")
    if page_token:
        offset = _token_int(token, "offset", offset)
    rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
    with open_connection() as con:
        query = f"SELECT * FROM ({rel_sql}) LIMIT {limit} OFFSET {offset}"
        columns, rows, row_ids = fetch_rows_with_rowid(con, query, params)
    next_offset = offset + len(rows)
    has_next = len(rows) == limit
    next_token = _encode_page_token({"kind": "duckdb", "offset": next_offset}) if has_next else None
    response = build_table_response(file_name, columns, rows, limit, offset, row_ids)
    warning = _merge_warnings(
        response.get("warning"), "This format uses OFFSET fallback. Convert TB-scale JSON arrays to JSONL or Parquet for responsive paging."
    )
    response.update(
        {
            "next_page_token": next_token,
            "has_next": has_next,
            "warning": warning,
        }
    )
    return response


def fetch_preview_page(
    file_name: str,
    path: Path,
    *,
    limit: int | None = DEFAULT_LIMIT,
    offset: int | None = 0,
    page_token: str | None = None,
    deleted_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Fetch one preview page using a format-specific cursor token."""
    limit_value, offset_value = normalize_pagination(limit, offset)
    _reject_large_offset_without_token(offset_value, page_token)
    hidden_row_ids = set(deleted_ids or [])
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return _jsonl_preview(file_name, path, limit_value, offset_value, page_token, hidden_row_ids)
    if suffix == ".csv":
        return _delimited_preview(file_name, path, delimiter=",", limit=limit_value, offset=offset_value, page_token=page_token, deleted_ids=hidden_row_ids)
    if suffix == ".tsv":
        return _delimited_preview(file_name, path, delimiter="\t", limit=limit_value, offset=offset_value, page_token=page_token, deleted_ids=hidden_row_ids)
    if suffix == ".parquet":
        return _parquet_preview(file_name, path, limit_value, offset_value, page_token, hidden_row_ids)
    if suffix == ".json":
        return _json_preview(file_name, path, limit_value)
    return _duckdb_preview(file_name, path, limit_value, offset_value, page_token, deleted_ids or [])


def count_rows_with_progress(path: Path, control: ScanControl, *, deleted_ids: list[int] | None = None) -> int:
    """Count rows with format-aware shortcuts and cooperative cancellation."""
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        import pyarrow.parquet as pq  # noqa: PLC0415

        control.check_cancelled()
        total_rows = int(pq.ParquetFile(path).metadata.num_rows)
        return _visible_row_count(total_rows, deleted_ids)

    if suffix in LINE_DATASET_EXTENSIONS:
        result = build_line_index_with_progress(path, control)
        return _visible_row_count(int(result["row_count"]), deleted_ids)

    deleted_ids = deleted_ids or []
    if deleted_ids:
        rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
        query = f"SELECT COUNT(*) FROM ({rel_sql})"
    else:
        rel_sql, params = relation_sql(path)
        query = f"SELECT COUNT(*) FROM {rel_sql}"
    with open_connection() as con:
        control.check_cancelled()
        row = con.execute(query, params).fetchone()
    return int(row[0]) if row else 0


def build_line_index_with_progress(path: Path, control: ScanControl) -> dict[str, Any]:
    """Build or refresh the sparse byte-offset index for a line-oriented dataset."""
    suffix = path.suffix.lower()
    if suffix not in LINE_DATASET_EXTENSIONS:
        raise HTTPException(status_code=400, detail="line index is only supported for jsonl, csv, and tsv")

    index = LineOffsetIndex(path)
    row_count = 0
    size = max(path.stat().st_size, 1)
    with path.open("rb") as file:
        if suffix in {".csv", ".tsv"}:
            file.readline()
        while True:
            control.check_cancelled()
            offset = file.tell()
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            row_count += 1
            index.record(row_count, offset)
            if row_count % 10_000 == 0:
                control.update(progress=min(file.tell() / size, 0.999), message=f"Indexed {row_count:,} rows")

    byte_count = path.stat().st_size
    index.mark_complete(row_count=row_count, byte_count=byte_count)
    status = index.status()
    control.update(progress=1.0, message=f"Indexed {row_count:,} rows")
    return {
        "format": _format_name(path),
        "row_count": row_count,
        "byte_count": byte_count,
        "index": status,
    }


def _search_jsonl(
    path: Path, query: str, limit: int, deleted_ids: set[int], control: ScanControl
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    metadata = load_dataset_metadata(path)
    columns = [column["name"] for column in metadata.columns]
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    lowered = query.lower()
    row_number = 0
    size = max(path.stat().st_size, 1)
    columns_truncated = False
    index = LineOffsetIndex(path)
    with path.open("rb") as file:
        while len(rows) < limit:
            control.check_cancelled()
            line_start = file.tell()
            line = file.readline()
            if not line:
                return columns, rows, row_ids, False, _merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None)
            if not line.strip():
                continue
            row_number += 1
            index.record(row_number, line_start)
            if row_number in deleted_ids:
                continue
            text = line.decode("utf-8", "replace")
            if lowered not in text.lower():
                continue
            value = json.loads(line)
            old_column_count = len(columns)
            columns, row_columns_truncated = _extend_columns_from_value(columns, value)
            columns_truncated = columns_truncated or row_columns_truncated
            _align_existing_rows(rows, old_column_count, len(columns))
            rows.append(_row_from_mapping(columns, value))
            row_ids.append(row_number)
            control.update(progress=min(file.tell() / size, 0.999), message=f"Found {len(rows):,} rows")
    return columns, rows, row_ids, True, _merge_warnings(metadata.warning, COLUMN_LIMIT_WARNING if columns_truncated else None)


def _search_delimited(
    path: Path,
    query: str,
    delimiter: str,
    limit: int,
    deleted_ids: set[int],
    control: ScanControl,
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    source_columns, first_data_offset = _read_delimited_header(path, delimiter)
    columns = source_columns[:MAX_COLUMNS]
    warning = COLUMN_LIMIT_WARNING if len(source_columns) > MAX_COLUMNS else None
    rows: list[list[Any]] = []
    row_ids: list[int] = []
    lowered = query.lower()
    row_number = 0
    size = max(path.stat().st_size, 1)
    index = LineOffsetIndex(path)
    with path.open("rb") as file:
        file.seek(first_data_offset)
        while len(rows) < limit:
            control.check_cancelled()
            line_start = file.tell()
            line = file.readline()
            if not line:
                return columns, rows, row_ids, False, warning
            if not line.strip():
                continue
            row_number += 1
            index.record(row_number, line_start)
            if row_number in deleted_ids:
                continue
            text = line.decode("utf-8-sig", "replace").rstrip("\r\n")
            if lowered not in text.lower():
                continue
            values = next(csv.reader([text], delimiter=delimiter), [])
            padded = values + [None] * max(0, len(columns) - len(values))
            rows.append([serialize_value(value) for value in padded[: len(columns)]])
            row_ids.append(row_number)
            control.update(progress=min(file.tell() / size, 0.999), message=f"Found {len(rows):,} rows")
    return columns, rows, row_ids, True, warning


def _search_duckdb(
    path: Path, query: str, limit: int, deleted_ids: list[int], control: ScanControl
) -> tuple[list[str], list[list[Any]], list[int], bool, str | None]:
    rel_sql, params = relation_with_rowid_sql(path, deleted_ids)
    with open_connection() as con:
        base_rel_sql, base_params = relation_sql(path)
        columns_meta = describe_relation(con, base_rel_sql, base_params)
        text_columns = [col["name"] for col in columns_meta if "CHAR" in col["type"].upper() or "TEXT" in col["type"].upper()]
        if not text_columns:
            return [col["name"] for col in columns_meta], [], [], False, None
        from .db import quote_ident  # noqa: PLC0415

        like_clauses = " OR ".join([f"CAST({quote_ident(col)} AS VARCHAR) ILIKE ?" for col in text_columns])
        values = params + [f"%{query}%"] * len(text_columns)
        control.check_cancelled()
        result_columns, rows, row_ids = fetch_rows_with_rowid(con, f"SELECT * FROM ({rel_sql}) WHERE {like_clauses} LIMIT {limit}", values)
    return result_columns, rows, row_ids, len(rows) == limit, None


def search_dataset(
    file_name: str,
    path: Path,
    *,
    query: str,
    limit: int | None,
    control: ScanControl,
    deleted_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Search a dataset in a background job and return first matching rows."""
    search_term = query.strip()
    if not search_term:
        raise HTTPException(status_code=400, detail="query must not be empty")
    limit_value, _ = normalize_pagination(limit, 0)
    hidden_row_ids = set(deleted_ids or [])
    searchers: dict[str, Callable[[], tuple[list[str], list[list[Any]], list[int], bool, str | None]]] = {
        ".jsonl": lambda: _search_jsonl(path, search_term, limit_value, hidden_row_ids, control),
        ".csv": lambda: _search_delimited(path, search_term, ",", limit_value, hidden_row_ids, control),
        ".tsv": lambda: _search_delimited(path, search_term, "\t", limit_value, hidden_row_ids, control),
    }
    columns, rows, row_ids, truncated, warning = searchers.get(
        path.suffix.lower(),
        lambda: _search_duckdb(path, search_term, limit_value, deleted_ids or [], control),
    )()
    response = build_table_response(file_name, columns, rows, limit_value, 0, row_ids)
    response.update({"query": search_term, "truncated": truncated})
    warning = _merge_warnings(response.get("warning"), warning)
    if warning:
        response["warning"] = warning
    if warning and COLUMN_LIMIT_WARNING in warning:
        _mark_columns_truncated(response)
    return response

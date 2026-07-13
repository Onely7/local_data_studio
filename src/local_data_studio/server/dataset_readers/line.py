"""Compatibility facade for line-oriented dataset readers."""

from ..line_index import LineOffsetIndex
from .delimited import (
    CSV_FIELD_SIZE_LOCK,
    load_delimited_metadata,
    parse_delimited_record,
    preview_delimited,
    raw_delimited_row,
    read_delimited_header,
    search_delimited,
)
from .jsonl import (
    JSONL_SCHEMA_MAX_BYTES,
    JSONL_SCHEMA_MAX_SCANNED_LINES,
    JSONL_SCHEMA_SAMPLE_ROWS,
    _create_jsonl_metadata,
    load_jsonl_metadata,
    preview_jsonl,
    raw_jsonl_row,
    search_jsonl,
)
from .line_cursor import _indexed_line_start, _line_cursor_for_offset, _raw_line_value
from .line_indexing import INDEX_CHECKPOINT_BATCH_SIZE, LINE_DATASET_EXTENSIONS, build_line_index_with_progress

__all__ = [
    "LINE_DATASET_EXTENSIONS",
    "build_line_index_with_progress",
    "load_delimited_metadata",
    "load_jsonl_metadata",
    "parse_delimited_record",
    "preview_delimited",
    "preview_jsonl",
    "raw_delimited_row",
    "raw_jsonl_row",
    "read_delimited_header",
    "search_delimited",
    "search_jsonl",
]

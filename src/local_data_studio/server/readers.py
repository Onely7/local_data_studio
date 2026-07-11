"""Compatibility facade for format-specific dataset readers."""

from .dataset_readers.common import JSON_NOT_TB_SAFE_WARNING, TB_SAFE_EXTENSIONS
from .dataset_readers.contracts import DatasetMetadata, ScanControl
from .dataset_readers.line import LINE_DATASET_EXTENSIONS
from .dataset_readers.parquet import PARQUET_EXTENSION
from .dataset_readers.service import (
    build_line_index_with_progress,
    count_rows_with_progress,
    fetch_preview_page,
    fetch_raw_row,
    load_dataset_metadata,
    search_dataset,
)

__all__ = [
    "JSON_NOT_TB_SAFE_WARNING",
    "LINE_DATASET_EXTENSIONS",
    "PARQUET_EXTENSION",
    "TB_SAFE_EXTENSIONS",
    "DatasetMetadata",
    "ScanControl",
    "build_line_index_with_progress",
    "count_rows_with_progress",
    "fetch_preview_page",
    "fetch_raw_row",
    "load_dataset_metadata",
    "search_dataset",
]

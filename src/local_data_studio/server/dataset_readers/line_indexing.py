"""Sparse index construction for line-oriented datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..line_index import LineOffsetIndex
from .common import format_name
from .contracts import ScanControl
from .delimited import delimited_rows_are_quoted, read_delimited_header
from .line_cursor import iter_line_blocks

LINE_DATASET_EXTENSIONS = {".jsonl", ".csv", ".tsv"}
INDEX_CHECKPOINT_BATCH_SIZE = 256


def build_line_index_with_progress(path: Path, control: ScanControl) -> dict[str, Any]:
    """Build or refresh the sparse byte-offset index for a line-oriented dataset."""
    suffix = path.suffix.lower()
    if suffix not in LINE_DATASET_EXTENSIONS:
        raise HTTPException(status_code=400, detail="line index is only supported for jsonl, csv, and tsv")
    index = LineOffsetIndex(path)
    byte_count = path.stat().st_size
    cached_status = index.status()
    if cached_status["complete"] and cached_status["byte_count"] == byte_count and cached_status["row_count"] is not None:
        row_count = int(cached_status["row_count"])
        control.update(progress=1.0, message=f"Using index for {row_count:,} rows")
        return {"format": format_name(path), "row_count": row_count, "byte_count": byte_count, "index": cached_status}

    row_count = 0
    checkpoints: list[tuple[int, int]] = []
    size = max(byte_count, 1)
    first_data_offset = 0
    quoted = False
    if suffix in {".csv", ".tsv"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        quoted = delimited_rows_are_quoted(path, delimiter)
        _, first_data_offset = read_delimited_header(path, delimiter)
    for block in iter_line_blocks(path, start_offset=first_data_offset, first_row=1, control=control, parallel=True, quoted=quoted):
        checkpoints.extend(block.checkpoints(index.stride))
        if len(checkpoints) >= INDEX_CHECKPOINT_BATCH_SIZE:
            index.record_checkpoints(checkpoints)
            checkpoints.clear()
        row_count += block.row_count
        control.update(progress=min(block.end_offset / size, 0.999), message=f"Indexed {row_count:,} rows")
    index.record_checkpoints(checkpoints)
    index.mark_complete(row_count=row_count, byte_count=byte_count)
    status = index.status()
    control.update(progress=1.0, message=f"Indexed {row_count:,} rows")
    return {"format": format_name(path), "row_count": row_count, "byte_count": byte_count, "index": status}

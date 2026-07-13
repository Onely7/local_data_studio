"""Sparse index construction for line-oriented datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..line_index import LineOffsetIndex
from .common import format_name
from .contracts import ScanControl

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
    with path.open("rb") as file:
        if suffix in {".csv", ".tsv"}:
            file.readline()
        while True:
            control.check_cancelled()
            byte_offset = file.tell()
            line = file.readline()
            if not line:
                break
            if not line.strip():
                continue
            row_count += 1
            if row_count % index.stride == 0:
                checkpoints.append((row_count, byte_offset))
                if len(checkpoints) >= INDEX_CHECKPOINT_BATCH_SIZE:
                    index.record_checkpoints(checkpoints)
                    checkpoints.clear()
            if row_count % 10_000 == 0:
                control.update(progress=min(file.tell() / size, 0.999), message=f"Indexed {row_count:,} rows")
    index.record_checkpoints(checkpoints)
    index.mark_complete(row_count=row_count, byte_count=byte_count)
    status = index.status()
    control.update(progress=1.0, message=f"Indexed {row_count:,} rows")
    return {"format": format_name(path), "row_count": row_count, "byte_count": byte_count, "index": status}

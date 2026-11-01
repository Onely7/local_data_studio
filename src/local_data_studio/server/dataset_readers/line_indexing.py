"""Sparse row index construction for row-oriented datasets.

One builder per dataset extends the index from where its last scan stopped.
Callers register goals, such as a byte budget, a row, or the end of the
file, and the builder's thread scans until no registered goal is unmet.
Count jobs, jumps past the index, and the background scan of the open
dataset therefore share one sequential read.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from fastapi import HTTPException

from ..config import BACKGROUND_INDEX, BACKGROUND_INDEX_MAX_BYTES
from ..jobs import FOREGROUND_ACTIVITY
from ..line_index import IndexProgress, LineOffsetIndex
from .common import dataset_cache_key, format_name
from .contracts import ScanControl
from .delimited import delimited_rows_are_quoted, read_delimited_header
from .json_reader import array_start_offset, iter_array_blocks, json_layout
from .line_cursor import iter_line_blocks

LINE_DATASET_EXTENSIONS = {".jsonl", ".csv", ".tsv"}
# Background scans pause in steps of this length while interactive requests
# run, but never longer than the maximum per block, so they cannot starve.
FOREGROUND_YIELD_SECONDS = 0.05
FOREGROUND_YIELD_MAX_SECONDS = 2.0
WAIT_POLL_SECONDS = 0.5


class ScanBlock(Protocol):
    """Rows found by one scan step: line blocks and JSON array blocks both qualify."""

    @property
    def first_row(self) -> int:
        """Return the one-based row number of the block's first row."""
        ...

    @property
    def row_count(self) -> int:
        """Return the number of rows in the block."""
        ...

    @property
    def end_offset(self) -> int:
        """Return where the next block begins."""
        ...

    def checkpoints(self, stride: int) -> list[tuple[int, int]]:
        """Return ``(row, byte offset)`` pairs for rows that are multiples of ``stride``."""
        ...


@dataclass(frozen=True, slots=True)
class RowScan:
    """Where a dataset's first row begins and how to scan rows from a cursor."""

    first_offset: int
    blocks: Callable[[int, int, ScanControl | None], Iterator[ScanBlock]]


def row_scan(path: Path) -> RowScan | None:
    """Return the row scanner for JSONL, CSV, TSV, and JSON Lines or array files.

    Other files, including single-value JSON documents, return ``None``.
    """
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        quoted = delimited_rows_are_quoted(path, delimiter)
        _, first_offset = read_delimited_header(path, delimiter)
        return RowScan(
            first_offset, lambda start, row, control: iter_line_blocks(path, start_offset=start, first_row=row, control=control, parallel=True, quoted=quoted)
        )
    layout = "lines" if suffix == ".jsonl" else json_layout(path) if suffix == ".json" else None
    if layout == "lines":
        return RowScan(0, lambda start, row, control: iter_line_blocks(path, start_offset=start, first_row=row, control=control, parallel=True))
    if layout == "array":
        return RowScan(array_start_offset(path), lambda start, row, control: iter_array_blocks(path, start_offset=start, first_row=row, control=control))
    return None


def is_row_indexed(path: Path) -> bool:
    """Return whether rows of this dataset are addressed through a sparse index."""
    suffix = path.suffix.lower()
    return suffix in LINE_DATASET_EXTENSIONS or (suffix == ".json" and json_layout(path) != "value")


@dataclass(frozen=True, slots=True)
class IndexGoal:
    """How far one request needs the index; with neither bound, the end of the file."""

    until_row: int | None = None
    until_byte: int | None = None
    background: bool = False

    def met_by(self, progress: IndexProgress | None) -> bool:
        """Return whether ``progress`` satisfies this goal."""
        if progress is None:
            return False
        if progress.complete:
            return True
        if self.until_row is not None:
            return progress.next_row > self.until_row
        if self.until_byte is not None:
            return progress.next_byte >= self.until_byte
        return False


@dataclass(frozen=True, slots=True)
class BuilderSnapshot:
    """Index progress and builder state for status responses."""

    progress: IndexProgress | None
    running: bool
    background: bool
    error: str | None


class _ScanStoppedError(Exception):
    """Raised inside a scan when no registered goal needs more rows."""


class _BuilderControl:
    def __init__(self, builder: RowIndexBuilder) -> None:
        self._builder = builder

    def check_cancelled(self) -> None:
        if self._builder.should_stop():
            raise _ScanStoppedError

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        return None


class RowIndexBuilder:
    """Extend one dataset's sparse row index on a dedicated thread."""

    def __init__(self, path: Path) -> None:
        """Create an idle builder; the index file keeps progress across restarts."""
        self.path = path
        self._condition = threading.Condition()
        self._goals: dict[int, IndexGoal] = {}
        self._tickets = 0
        self._thread: threading.Thread | None = None
        self._shutdown = False
        self._error: Exception | None = None
        self._progress: IndexProgress | None = None
        self._loaded = False

    def _progress_locked(self) -> IndexProgress | None:
        if not self._loaded:
            self._progress = LineOffsetIndex(self.path).progress()
            self._loaded = True
        return self._progress

    def _unmet_locked(self) -> list[IndexGoal]:
        progress = self._progress_locked()
        return [goal for goal in self._goals.values() if not goal.met_by(progress)]

    def should_stop(self) -> bool:
        """Return whether the running scan should stop at the next block."""
        with self._condition:
            return self._shutdown or not self._unmet_locked()

    def request(self, goal: IndexGoal) -> int:
        """Register a goal and start scanning if it is unmet; return its ticket."""
        with self._condition:
            self._tickets += 1
            ticket = self._tickets
            self._goals[ticket] = goal
            if not goal.met_by(self._progress_locked()) and not self._shutdown and self._thread is None:
                self._error = None
                self._thread = threading.Thread(target=self._run, name="local-data-studio-index", daemon=True)
                self._thread.start()
            self._condition.notify_all()
            return ticket

    def release(self, ticket: int) -> None:
        """Remove a goal; the scan stops once no remaining goal is unmet."""
        with self._condition:
            self._goals.pop(ticket, None)
            self._condition.notify_all()

    def snapshot(self) -> BuilderSnapshot:
        """Return current progress and whether a scan or background goal is active."""
        with self._condition:
            return BuilderSnapshot(
                progress=self._progress_locked(),
                running=self._thread is not None,
                background=any(goal.background for goal in self._goals.values()),
                error=str(getattr(self._error, "detail", None) or self._error) if self._error else None,
            )

    def idle(self) -> bool:
        """Return whether the builder has no goals and no running scan."""
        with self._condition:
            return not self._goals and self._thread is None

    def wait(self, ticket: int, control: ScanControl) -> IndexProgress:
        """Block until the ticket's goal is met, reporting progress through ``control``.

        Raises:
            Exception: The scan failed; its error is re-raised.
            RuntimeError: The scan stopped before meeting the goal.
        """
        size = max(1, self.path.stat().st_size)
        while True:
            control.check_cancelled()
            with self._condition:
                goal = self._goals.get(ticket)
                progress = self._progress_locked()
                if goal is None:
                    raise RuntimeError("the index request was released")
                if progress is not None and goal.met_by(progress):
                    return progress
                if self._error is not None:
                    raise self._error
                if self._thread is None:
                    raise RuntimeError("indexing stopped before reaching the requested row")
                self._condition.wait(timeout=WAIT_POLL_SECONDS)
                progress = self._progress
            rows = progress.next_row - 1 if progress else 0
            fraction = min(rows / goal.until_row, 0.999) if goal.until_row else min((progress.next_byte if progress else 0) / size, 0.999)
            control.update(progress=fraction, message=f"Indexed {rows:,} rows")

    def shutdown(self, timeout: float) -> None:
        """Stop scanning and wait up to ``timeout`` seconds for the thread."""
        with self._condition:
            self._shutdown = True
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def _yield_to_foreground(self) -> None:
        deadline = time.monotonic() + FOREGROUND_YIELD_MAX_SECONDS
        while FOREGROUND_ACTIVITY.busy() and time.monotonic() < deadline and not self.should_stop():
            time.sleep(FOREGROUND_YIELD_SECONDS)

    def _run(self) -> None:
        # The thread clears ``_thread`` under the lock only when no goal is
        # unmet, so a goal registered while a scan winds down is never lost.
        while True:
            try:
                self._scan()
            except _ScanStoppedError:
                pass
            except Exception as exc:  # noqa: BLE001 - waiters re-raise the scan error
                with self._condition:
                    self._error = exc
                    self._thread = None
                    self._condition.notify_all()
                return
            with self._condition:
                if self._shutdown or not self._unmet_locked():
                    self._thread = None
                    self._condition.notify_all()
                    return

    def _scan(self) -> None:
        scan = row_scan(self.path)
        if scan is None:
            raise HTTPException(status_code=400, detail="row index is only supported for jsonl, csv, tsv, and JSON Lines or array files")
        index = LineOffsetIndex(self.path)
        with self._condition:
            progress = self._progress_locked()
        if progress is not None and progress.complete:
            return
        next_row, next_byte = (progress.next_row, progress.next_byte) if progress else (1, scan.first_offset)
        for block in scan.blocks(next_byte, next_row, _BuilderControl(self)):
            next_row, next_byte = block.first_row + block.row_count, block.end_offset
            index.record_progress(block.checkpoints(index.stride), next_row=next_row, next_byte=next_byte)
            with self._condition:
                self._progress = IndexProgress(next_row, next_byte, False, None)
                self._condition.notify_all()
                unmet = self._unmet_locked()
            if not unmet:
                return
            if all(goal.background for goal in unmet):
                self._yield_to_foreground()
        index.record_progress([], next_row=next_row, next_byte=next_byte)
        index.mark_complete(row_count=next_row - 1, byte_count=self.path.stat().st_size)
        with self._condition:
            self._progress = IndexProgress(next_row, next_byte, True, next_row - 1)
            self._condition.notify_all()


_REGISTRY_LOCK = threading.RLock()
_BUILDERS: dict[str, RowIndexBuilder] = {}
_BACKGROUND: tuple[RowIndexBuilder, int] | None = None


def index_builder(path: Path) -> RowIndexBuilder:
    """Return the builder for the dataset's current contents, creating it when needed.

    Idle builders of other files, or of earlier versions of this file, are
    dropped; their progress stays in the index files.
    """
    key = dataset_cache_key(path)
    with _REGISTRY_LOCK:
        builder = _BUILDERS.get(key)
        if builder is None:
            for stale in [name for name, existing in _BUILDERS.items() if existing.idle()]:
                del _BUILDERS[stale]
            builder = _BUILDERS[key] = RowIndexBuilder(path)
        return builder


def start_background_index(path: Path) -> bool:
    """Index the first ``BACKGROUND_INDEX_MAX_BYTES`` of the dataset in the background.

    The request replaces the background scan of any other dataset. It returns
    whether a background scan is registered for this dataset, which is false
    when the setting is off or the format has no row index.
    """
    global _BACKGROUND  # noqa: PLW0603
    with _REGISTRY_LOCK:
        enabled = BACKGROUND_INDEX and is_row_indexed(path)
        builder = index_builder(path) if enabled else None
        if _BACKGROUND is not None and _BACKGROUND[0] is builder:
            return True
        if _BACKGROUND is not None:
            _BACKGROUND[0].release(_BACKGROUND[1])
            _BACKGROUND = None
        if builder is None:
            return False
        scan = row_scan(path)
        until_byte = None if BACKGROUND_INDEX_MAX_BYTES == 0 or scan is None else scan.first_offset + BACKGROUND_INDEX_MAX_BYTES
        _BACKGROUND = (builder, builder.request(IndexGoal(until_byte=until_byte, background=True)))
        return True


def stop_background_index() -> None:
    """Release the background scan's goal, if one is registered."""
    global _BACKGROUND  # noqa: PLW0603
    with _REGISTRY_LOCK:
        if _BACKGROUND is not None:
            _BACKGROUND[0].release(_BACKGROUND[1])
            _BACKGROUND = None


def shutdown_index_builders(timeout: float = 5.0) -> None:
    """Stop every scan, waiting up to ``timeout`` seconds in total."""
    stop_background_index()
    with _REGISTRY_LOCK:
        builders = list(_BUILDERS.values())
        _BUILDERS.clear()
    deadline = time.monotonic() + timeout
    for builder in builders:
        builder.shutdown(max(0.0, deadline - time.monotonic()))


def build_line_index_with_progress(path: Path, control: ScanControl, *, until_row: int | None = None) -> dict[str, Any]:
    """Extend the sparse row index to ``until_row``, or to the end of the file.

    The scan continues from where earlier scans stopped and is shared with
    other requests for the same dataset. Cancelling ``control`` withdraws
    this request without discarding indexed rows.

    Raises:
        HTTPException: The format has no row index.
    """
    if not is_row_indexed(path):
        raise HTTPException(status_code=400, detail="row index is only supported for jsonl, csv, tsv, and JSON Lines or array files")
    builder = index_builder(path)
    ticket = builder.request(IndexGoal(until_row=until_row))
    try:
        progress = builder.wait(ticket, control)
    finally:
        builder.release(ticket)
    message = f"Indexed {progress.row_count:,} rows" if progress.complete and progress.row_count is not None else f"Indexed {progress.next_row - 1:,} rows"
    control.update(progress=1.0, message=message)
    return {
        "format": format_name(path),
        "row_count": progress.row_count if progress.complete else None,
        "indexed_rows": progress.next_row - 1,
        "complete": progress.complete,
        "byte_count": path.stat().st_size,
        "index": LineOffsetIndex(path).status(),
    }

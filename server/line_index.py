"""Incremental byte-offset indexes for newline-delimited local datasets."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .cache import index_cache_path

INDEX_STRIDE = 10_000


@dataclass(frozen=True, slots=True)
class IndexedLine:
    """Line number and byte offset pair stored in a sidecar index."""

    line_number: int
    byte_offset: int


class LineOffsetIndex:
    """SQLite-backed sparse index for jumping through large text datasets."""

    def __init__(self, dataset_path: Path, *, stride: int = INDEX_STRIDE) -> None:
        self.dataset_path = dataset_path
        self.stride = stride
        self.path = index_cache_path(dataset_path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS line_offsets (
                    line_number INTEGER PRIMARY KEY,
                    byte_offset INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def record(self, line_number: int, byte_offset: int) -> None:
        """Persist a sparse offset when the line number matches the configured stride."""
        if line_number <= 0 or line_number % self.stride != 0:
            return
        self.record_checkpoint(line_number, byte_offset)

    def record_checkpoint(self, line_number: int, byte_offset: int) -> None:
        """Persist a line offset regardless of stride."""
        if line_number <= 0:
            return
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO line_offsets(line_number, byte_offset) VALUES (?, ?)",
                (line_number, byte_offset),
            )

    def mark_complete(self, *, row_count: int, byte_count: int) -> None:
        """Store completion metadata for a fully built sparse index."""
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
                [
                    ("complete", "1"),
                    ("row_count", str(row_count)),
                    ("byte_count", str(byte_count)),
                ],
            )

    def status(self) -> dict[str, int | bool | None]:
        """Return lightweight metadata about the cached index."""
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM metadata").fetchall()
            checkpoint = connection.execute("SELECT MAX(line_number), MAX(byte_offset) FROM line_offsets").fetchone()
        metadata = {str(key): str(value) for key, value in rows}
        return {
            "complete": metadata.get("complete") == "1",
            "row_count": int(metadata["row_count"]) if metadata.get("row_count", "").isdigit() else None,
            "byte_count": int(metadata["byte_count"]) if metadata.get("byte_count", "").isdigit() else None,
            "max_indexed_line": int(checkpoint[0]) if checkpoint and checkpoint[0] is not None else None,
            "max_indexed_byte": int(checkpoint[1]) if checkpoint and checkpoint[1] is not None else None,
        }

    def nearest_before(self, line_number: int) -> IndexedLine | None:
        """Return the nearest indexed line at or before the requested line."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT line_number, byte_offset
                FROM line_offsets
                WHERE line_number <= ?
                ORDER BY line_number DESC
                LIMIT 1
                """,
                (line_number,),
            ).fetchone()
        if row is None:
            return None
        return IndexedLine(line_number=int(row[0]), byte_offset=int(row[1]))

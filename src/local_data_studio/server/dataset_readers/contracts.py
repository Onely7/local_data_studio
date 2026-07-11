"""Contracts shared by format-specific dataset readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


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

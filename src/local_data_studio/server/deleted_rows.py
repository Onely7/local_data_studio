"""Session-only delete markers for rows."""

import threading
from pathlib import Path

_DELETED_LOCK = threading.Lock()
_DELETED_ROWS: dict[str, set[int]] = {}


def deleted_row_ids_for(path: Path) -> list[int]:
    """Return sorted row ids that are hidden in the current session."""
    key = str(path.resolve())
    with _DELETED_LOCK:
        return sorted(_DELETED_ROWS.get(key, set()))


def add_deleted_row_id(path: Path, row_id: int) -> None:
    """Mark a row id as deleted for the current session."""
    key = str(path.resolve())
    with _DELETED_LOCK:
        if key not in _DELETED_ROWS:
            _DELETED_ROWS[key] = set()
        _DELETED_ROWS[key].add(row_id)


def clear_deleted_row_ids(path: Path) -> None:
    """Clear all session-only deletions for a dataset."""
    key = str(path.resolve())
    with _DELETED_LOCK:
        _DELETED_ROWS.pop(key, None)

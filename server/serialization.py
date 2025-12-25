"""Value serialization for API responses."""

import datetime
import decimal
from typing import Any, Sequence

from .config import MAX_CELL_CHARS


def serialize_value(value: Any) -> Any:
    """Convert values into JSON-friendly primitives with size limits."""
    result: Any = value

    if value is None:
        result = None

    elif isinstance(value, (datetime.date, datetime.datetime)):
        result = value.isoformat()

    elif isinstance(value, decimal.Decimal):
        result = float(value)

    elif isinstance(value, bytes):
        result = value.hex()

    elif isinstance(value, str):
        if len(value) > MAX_CELL_CHARS:
            result = value[:MAX_CELL_CHARS] + "... (truncated)"
        else:
            result = value

    elif isinstance(value, (list, tuple)):
        result = [serialize_value(item) for item in value]

    elif isinstance(value, dict):
        result = {str(key): serialize_value(val) for key, val in value.items()}

    return result


def serialize_rows(rows: Sequence[Sequence[Any]]) -> list[list[Any]]:
    """Serialize a list of row sequences into JSON-friendly lists."""
    return [[serialize_value(cell) for cell in row] for row in rows]

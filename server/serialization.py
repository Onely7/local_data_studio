"""Value serialization for API responses."""

import datetime
import decimal
import re
from typing import Any, Sequence

from .config import MAX_CELL_CHARS, MAX_SEQ_ITEMS

IMAGE_REFERENCE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp|svg)(\?.*)?$", re.IGNORECASE)
IMAGE_HEX_PREFIXES = ("89504e47", "ffd8ff", "47494638")
IMAGE_BASE64_PREFIXES = ("iVBORw0KGgo", "/9j/", "R0lGOD", "UklGR")


def _is_image_reference(value: str) -> bool:
    if value.startswith("data:image"):
        return True
    return IMAGE_REFERENCE_PATTERN.search(value) is not None


def _is_image_bytes(value: bytes) -> bool:
    return (
        value.startswith(b"\x89PNG\r\n\x1a\n")
        or value.startswith(b"\xff\xd8\xff")
        or value.startswith(b"GIF87a")
        or value.startswith(b"GIF89a")
        or (value.startswith(b"RIFF") and value[8:12] == b"WEBP")
        or value.lstrip().startswith(b"<svg")
    )


def _is_image_bytes_string(value: str) -> bool:
    text = value.strip()
    compact = re.sub(r"\s+", "", text)
    lowered = compact.lower()
    if text.startswith("data:image"):
        return True
    if lowered.startswith(IMAGE_HEX_PREFIXES):
        return True
    return compact.startswith(IMAGE_BASE64_PREFIXES)


def serialize_value(value: Any, *, key: str | None = None, sibling_image_path: bool = False) -> Any:
    """Convert values into JSON-friendly primitives with size limits."""
    result: Any = value

    if value is None:
        result = None

    elif isinstance(value, (datetime.date, datetime.datetime)):
        result = value.isoformat()

    elif isinstance(value, decimal.Decimal):
        result = float(value)

    elif isinstance(value, bytes):
        hex_value = value.hex()
        if key == "bytes" and (_is_image_bytes(value) or sibling_image_path):
            result = hex_value
        elif len(hex_value) > MAX_CELL_CHARS:
            result = hex_value[:MAX_CELL_CHARS] + "... (truncated)"
        else:
            result = hex_value

    elif isinstance(value, str):
        if key == "bytes" and (_is_image_bytes_string(value) or sibling_image_path):
            result = value
        elif len(value) > MAX_CELL_CHARS and not _is_image_reference(value):
            result = value[:MAX_CELL_CHARS] + "... (truncated)"
        else:
            result = value

    elif isinstance(value, (list, tuple)):
        result = [serialize_value(item) for item in value[:MAX_SEQ_ITEMS]]
        if len(value) > MAX_SEQ_ITEMS:
            result.append(f"... ({len(value) - MAX_SEQ_ITEMS} more items truncated)")

    elif isinstance(value, dict):
        image_path = value.get("path")
        has_image_path = isinstance(image_path, str) and _is_image_reference(image_path)
        result = {
            str(item_key): serialize_value(val, key=str(item_key), sibling_image_path=has_image_path) for item_key, val in list(value.items())[:MAX_SEQ_ITEMS]
        }
        if len(value) > MAX_SEQ_ITEMS:
            result["__truncated__"] = f"{len(value) - MAX_SEQ_ITEMS} more fields truncated"

    return result


def serialize_rows(rows: Sequence[Sequence[Any]]) -> list[list[Any]]:
    """Serialize a list of row sequences into JSON-friendly lists."""
    return [[serialize_value(cell) for cell in row] for row in rows]

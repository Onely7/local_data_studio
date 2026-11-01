"""Value serialization for API responses."""

import base64
import binascii
import datetime
import decimal
import os
import re
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from io import BytesIO
from itertools import islice
from typing import Any, Sequence

from .config import MAX_CELL_CHARS, MAX_SEQ_ITEMS

IMAGE_REFERENCE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp|svg)(\?.*)?$", re.IGNORECASE)
IMAGE_HEX_PREFIXES = ("89504e47", "ffd8ff", "47494638")
IMAGE_BASE64_PREFIXES = ("iVBORw0KGgo", "/9j/", "R0lGOD", "UklGR")
# Preview cells draw images at 64-100 CSS pixels. 256 pixels stays sharp on
# high-density screens while a page of photos remains a few megabytes.
THUMBNAIL_MAX_SIDE = 256
THUMBNAIL_MEDIA_TYPE = "image/webp;thumbnail=1"
BROWSER_IMAGE_FORMATS = {"PNG": "image/png", "JPEG": "image/jpeg", "GIF": "image/gif", "WEBP": "image/webp"}


def _is_image_reference(value: str) -> bool:
    if value.startswith("data:image"):
        return True
    return IMAGE_REFERENCE_PATTERN.search(value) is not None


def _image_mime(value: bytes) -> str | None:
    signatures = (
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
    )
    for signature, mime in signatures:
        if value.startswith(signature):
            return mime
    if value.startswith(b"RIFF") and value[8:12] == b"WEBP":
        return "image/webp"
    if value.lstrip().startswith(b"<svg"):
        return "image/svg+xml"
    return None


def _is_image_bytes(value: bytes) -> bool:
    return _image_mime(value) is not None


def _data_url(media_type: str, value: bytes) -> str:
    return f"data:{media_type};base64,{base64.b64encode(value).decode('ascii')}"


def image_preview_data_url(value: bytes) -> str | None:
    """Return a bounded data URL for one image shown in a preview page.

    Images that already fit the thumbnail box in a browser-native format are
    returned unchanged. Larger or non-native images become WebP thumbnails
    whose media type carries ``thumbnail=1``, which tells the browser to load
    the full row for enlarged views. Undecodable images keep their original
    bytes when the signature is browser-native and otherwise return ``None``.
    """
    from PIL import Image, UnidentifiedImageError  # noqa: PLC0415

    try:
        with Image.open(BytesIO(value)) as image:
            native_type = BROWSER_IMAGE_FORMATS.get(image.format or "")
            if native_type and max(image.size) <= THUMBNAIL_MAX_SIDE:
                return _data_url(native_type, value)
            # ``thumbnail`` uses JPEG draft decoding, so large photos are
            # decoded at a reduced scale instead of at full resolution.
            image.thumbnail((THUMBNAIL_MAX_SIDE, THUMBNAIL_MAX_SIDE))
            has_alpha = image.mode in {"RGBA", "LA", "PA"} or "transparency" in image.info
            output = BytesIO()
            image.convert("RGBA" if has_alpha else "RGB").save(output, format="WEBP", quality=80, method=0)
    except (OSError, ValueError, UnidentifiedImageError, Image.DecompressionBombError):
        mime = _image_mime(value)
        return _data_url(mime, value) if mime else None
    return _data_url(THUMBNAIL_MEDIA_TYPE, output.getvalue())


def _decode_image_string(value: str) -> bytes | None:
    compact = re.sub(r"\s+", "", value)
    try:
        if compact.startswith("data:image"):
            header, _, payload = compact.partition(",")
            return base64.b64decode(payload) if header.endswith(";base64") else None
        if compact.lower().startswith(IMAGE_HEX_PREFIXES):
            return bytes.fromhex(compact)
        return base64.b64decode(compact, validate=True)
    except (ValueError, binascii.Error):
        return None


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
        image_bytes = key == "bytes" and sibling_image_path
        image_url = image_preview_data_url(value) if image_bytes or _is_image_bytes(value) else None
        hex_value = value.hex() if image_url is None else ""
        if image_url is not None:
            result = image_url
        elif image_bytes:
            result = hex_value
        elif len(hex_value) > MAX_CELL_CHARS:
            result = hex_value[:MAX_CELL_CHARS] + "... (truncated)"
        else:
            result = hex_value

    elif isinstance(value, str):
        if key == "bytes" and (_is_image_bytes_string(value) or sibling_image_path):
            decoded = _decode_image_string(value)
            result = (image_preview_data_url(decoded) if decoded else None) or value
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
            str(item_key): serialize_value(val, key=str(item_key), sibling_image_path=has_image_path) for item_key, val in islice(value.items(), MAX_SEQ_ITEMS)
        }
        if len(value) > MAX_SEQ_ITEMS:
            result["__truncated__"] = f"{len(value) - MAX_SEQ_ITEMS} more fields truncated"

    return result


def serialize_raw_value(value: Any) -> Any:
    """Convert one explicitly requested row to JSON-friendly values without limits.

    Preview responses use size limits to remain responsive. This function is used
    only after a user explicitly requests the full selected row, so it must not
    apply the preview's character or collection limits. Browser-native image
    bytes become full-resolution data URLs; other bytes become hex strings.
    """
    result: Any = value

    if value is None:
        result = None
    elif isinstance(value, (datetime.date, datetime.datetime)):
        result = value.isoformat()
    elif isinstance(value, decimal.Decimal):
        result = float(value)
    elif isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        mime = _image_mime(raw)
        result = _data_url(mime, raw) if mime else raw.hex()
    elif isinstance(value, (list, tuple)):
        result = [serialize_raw_value(item) for item in value]
    elif isinstance(value, dict):
        result = {str(item_key): serialize_raw_value(item_value) for item_key, item_value in value.items()}

    return result


def _serialize_row(row: Sequence[Any]) -> list[Any]:
    return [serialize_value(cell) for cell in row]


@lru_cache(maxsize=1)
def _serialization_pool() -> ThreadPoolExecutor:
    # Pillow releases the GIL while decoding and encoding, so image pages
    # build their thumbnails concurrently; text-only pages pay little overhead.
    return ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 1), thread_name_prefix="local-data-studio-serialize")


def serialize_rows(rows: Sequence[Sequence[Any]]) -> list[list[Any]]:
    """Serialize row sequences into JSON-friendly lists, preserving row order.

    Rows are converted on a shared worker pool so image thumbnails for one
    page are generated in parallel.
    """
    if len(rows) < 2:
        return [_serialize_row(row) for row in rows]
    return list(_serialization_pool().map(_serialize_row, rows))

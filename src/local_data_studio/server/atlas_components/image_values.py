"""Image detection and bounded URL, path, and bytes resolution."""

from __future__ import annotations

import re
import time
from base64 import b64decode
from binascii import Error as BinasciiError
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

IMAGE_REFERENCE_PATTERN = re.compile(r"\.(png|jpg|jpeg|gif|webp|svg)(?:\?.*)?$", re.IGNORECASE)
IMAGE_HEX_PREFIXES = ("89504e47", "ffd8ff", "47494638", "52494646")
IMAGE_BASE64_PREFIXES = ("ivborw0kggo", "/9j/", "r0lgod", "uklgr")
IMAGE_COLUMN_HINTS = ("image", "img", "photo", "picture", "thumbnail", "patch")
ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS = 20
ATLAS_IMAGE_FETCH_RETRIES = 3
ATLAS_IMAGE_MAX_BYTES = 50 * 1024 * 1024
ATLAS_TRUNCATION_SUFFIX = "... (truncated for Atlas)"


def is_image_reference(value: str) -> bool:
    """Return whether a string has a supported image URL or filename suffix."""
    text = value.strip()
    return bool(text and (text.startswith("data:image") or IMAGE_REFERENCE_PATTERN.search(text)))


def is_image_bytes_string(value: str) -> bool:
    """Return whether a string is a plausible hex, base64, or data-image payload."""
    compact = re.sub(r"\s+", "", value.strip())
    if not compact:
        return False
    lowered = compact.lower()
    return lowered.startswith(IMAGE_HEX_PREFIXES) or lowered.startswith(IMAGE_BASE64_PREFIXES)


def is_image_bytes(value: bytes) -> bool:
    """Return whether bytes begin with a recognized image signature."""
    return (
        value.startswith(b"\x89PNG\r\n\x1a\n")
        or value.startswith(b"\xff\xd8\xff")
        or value.startswith(b"GIF87a")
        or value.startswith(b"GIF89a")
        or (value.startswith(b"RIFF") and value[8:12] == b"WEBP")
        or value.lstrip().startswith(b"<svg")
    )


def is_image_like_value(value: Any) -> bool:
    """Return whether a scalar or ``{bytes, path}`` object may contain an image."""
    if isinstance(value, bytes):
        return is_image_bytes(value)
    if isinstance(value, bytearray):
        return is_image_bytes(bytes(value))
    if isinstance(value, str):
        return is_image_reference(value) or is_image_bytes_string(value)
    if isinstance(value, dict):
        raw_bytes = value.get("bytes")
        path = value.get("path")
        return is_image_like_value(raw_bytes) or (isinstance(path, str) and is_image_reference(path))
    if isinstance(value, list):
        return any(is_image_like_value(item) for item in value[:3])
    return False


def decode_data_image(value: str) -> bytes | None:
    """Decode a base64 data-image URL, returning ``None`` when invalid."""
    text = value.strip()
    if not text.startswith("data:image"):
        return None
    _, _, payload = text.partition(",")
    if not payload:
        return None
    try:
        return b64decode(payload, validate=False)
    except BinasciiError:
        return None


def decode_image_bytes_string(value: str) -> bytes | None:
    """Decode supported textual bytes and verify an image signature."""
    text = re.sub(r"\s+", "", value.strip())
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith(IMAGE_HEX_PREFIXES):
        try:
            return bytes.fromhex(text)
        except ValueError:
            return None
    if lowered.startswith(IMAGE_BASE64_PREFIXES):
        try:
            return b64decode(text, validate=False)
        except BinasciiError:
            return None
    return None


def _read_url_bytes_once(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "local-data-studio/atlas"})
    with urlopen(request, timeout=ATLAS_IMAGE_FETCH_TIMEOUT_SECONDS) as response:
        chunks: list[bytes] = []
        total = 0
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            if total > ATLAS_IMAGE_MAX_BYTES:
                raise ValueError(f"image exceeds {ATLAS_IMAGE_MAX_BYTES} bytes")
            chunks.append(chunk)
    return b"".join(chunks)


def read_url_bytes(url: str) -> bytes:
    """Download an image URL with bounded retries and payload size.

    Raises:
        ValueError: All attempts fail or the response exceeds the byte limit.
    """
    last_error: Exception | None = None
    for attempt in range(ATLAS_IMAGE_FETCH_RETRIES):
        try:
            return _read_url_bytes_once(url)
        except (OSError, URLError, ValueError) as exc:
            last_error = exc
            if attempt + 1 < ATLAS_IMAGE_FETCH_RETRIES:
                time.sleep(0.4 * (attempt + 1))
    raise ValueError(f"failed to read image URL {url}: {last_error}") from last_error


def _resolve_image_path(reference: str, dataset_path: Path) -> Path:
    text = reference.strip()
    if text.startswith("file://"):
        text = text[len("file://") :]
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = dataset_path.parent / candidate
    return candidate.resolve()


def _read_path_bytes(reference: str, dataset_path: Path) -> bytes:
    candidate = _resolve_image_path(reference, dataset_path)
    try:
        stat = candidate.stat()
    except OSError as exc:
        raise ValueError(f"image path does not exist: {reference}") from exc
    if not candidate.is_file():
        raise ValueError(f"image path is not a file: {reference}")
    if stat.st_size > ATLAS_IMAGE_MAX_BYTES:
        raise ValueError(f"image exceeds {ATLAS_IMAGE_MAX_BYTES} bytes: {reference}")
    return candidate.read_bytes()


def image_value_to_bytes(value: Any, dataset_path: Path) -> bytes:
    """Resolve an image value to encoder-owned bytes.

    ``bytes`` wins over ``path`` for object values. Relative paths are resolved
    from the dataset directory.

    Raises:
        ValueError: The value cannot be decoded, fetched, or resolved.
    """
    result: bytes | None = None
    if isinstance(value, bytes):
        result = value
    elif isinstance(value, bytearray):
        result = bytes(value)
    elif isinstance(value, dict):
        raw_bytes = value.get("bytes")
        if raw_bytes not in (None, ""):
            try:
                result = image_value_to_bytes(raw_bytes, dataset_path)
            except ValueError:
                pass
        if result is None and isinstance(value.get("path"), str) and value["path"].strip():
            result = image_value_to_bytes(value["path"], dataset_path)
    elif isinstance(value, list) and all(isinstance(item, int) for item in value):
        result = bytes(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty image value")
        result = decode_data_image(text) or decode_image_bytes_string(text)
        if result is None:
            parsed = urlparse(text)
            if parsed.scheme in {"http", "https"}:
                result = read_url_bytes(text)
            elif parsed.scheme in {"", "file"} and is_image_reference(text):
                result = _read_path_bytes(text, dataset_path)
    if result is None:
        raise ValueError(f"Cannot convert value of type {type(value)} to image/audio format")
    return result

"""Audio detection, preview markers, and source resolution for dataset cells.

Preview pages replace embedded audio bytes with a short marker, a data URL
without a payload whose media type carries ``preview=1``, the byte size, and
the duration when the container header states it. The browser plays the cell
by requesting its bytes from ``/api/raw_media``.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

AUDIO_MEDIA_TYPES_BY_EXTENSION: dict[str, str] = {
    ".wav": "audio/wav",
    ".wave": "audio/wav",
    ".mp3": "audio/mpeg",
    ".flac": "audio/flac",
    ".ogg": "audio/ogg",
    ".oga": "audio/ogg",
    ".opus": "audio/ogg",
    ".m4a": "audio/mp4",
    ".aac": "audio/aac",
    ".aif": "audio/aiff",
    ".aiff": "audio/aiff",
    ".weba": "audio/webm",
}
AUDIO_FILE_EXTENSIONS = frozenset(AUDIO_MEDIA_TYPES_BY_EXTENSION)
AUDIO_REFERENCE_PATTERN = re.compile(r"\.(?:wav|wave|mp3|flac|ogg|oga|opus|m4a|aac|aif|aiff|weba)(?:\?.*)?$", re.IGNORECASE)
# Hex and base64 spellings of the RIFF, fLaC, OggS, ID3, and FORM signatures.
AUDIO_HEX_PREFIXES = ("52494646", "664c6143", "4f676753", "494433", "464f524d")
AUDIO_BASE64_PREFIXES = ("UklGR", "ZkxhQ", "T2dnU", "SUQz", "Rk9ST")
AUDIO_PREVIEW_PARAMETER = "preview=1"
M4A_BRANDS = (b"M4A ", b"M4B ", b"M4P ", b"F4A ")


def _frame_sync_mime(data: bytes) -> str | None:
    second, third = data[1], data[2]
    # ADTS AAC frames use the 12-bit sync word with layer bits 00.
    if second & 0xF6 == 0xF0:
        return "audio/aac"
    # MPEG audio frames use an 11-bit sync word, a defined version and layer,
    # and a valid bitrate and sampling-rate index.
    version, layer = (second >> 3) & 0x3, (second >> 1) & 0x3
    is_mpeg = second & 0xE0 == 0xE0 and version != 1 and layer != 0 and third >> 4 != 0xF and (third >> 2) & 0x3 != 0x3
    return "audio/mpeg" if is_mpeg else None


CONTAINER_SIGNATURES: tuple[tuple[Callable[[bytes], bool], str], ...] = (
    (lambda data: data.startswith(b"RIFF") and data[8:12] == b"WAVE", "audio/wav"),
    (lambda data: data.startswith(b"fLaC"), "audio/flac"),
    (lambda data: data.startswith(b"OggS"), "audio/ogg"),
    (lambda data: data.startswith(b"ID3"), "audio/mpeg"),
    (lambda data: data.startswith(b"FORM") and data[8:12] in (b"AIFF", b"AIFC"), "audio/aiff"),
    (lambda data: data[4:8] == b"ftyp" and data[8:12] in M4A_BRANDS, "audio/mp4"),
)


def audio_mime_from_bytes(data: bytes) -> str | None:
    """Return the media type of bytes that start with a supported audio signature."""
    if len(data) < 4:
        return None
    for matches, media_type in CONTAINER_SIGNATURES:
        if matches(data):
            return media_type
    return _frame_sync_mime(data) if data[0] == 0xFF else None


def audio_mime_from_path(path: str) -> str | None:
    """Return the media type implied by an audio filename or URL suffix."""
    text = path.strip().split("?", 1)[0].lower()
    return AUDIO_MEDIA_TYPES_BY_EXTENSION.get(Path(text).suffix)


def is_audio_reference(value: str) -> bool:
    """Return whether a string is an audio data URL or ends with an audio suffix."""
    text = value.strip()
    if not text:
        return False
    if text.startswith("data:audio/"):
        return True
    return len(text) <= 4096 and AUDIO_REFERENCE_PATTERN.search(text) is not None


def is_audio_bytes_string(value: str) -> bool:
    """Return whether a string plausibly holds audio bytes as a data URL, hex, or base64."""
    text = value.lstrip()
    if text.startswith("data:audio/"):
        return True
    return text[:8].lower().startswith(AUDIO_HEX_PREFIXES) or text.startswith(AUDIO_BASE64_PREFIXES)


def is_audio_preview_marker(value: str) -> bool:
    """Return whether a string is a payload-free preview marker made by this module."""
    return value.startswith("data:audio/") and f";{AUDIO_PREVIEW_PARAMETER};" in value.partition(",")[0] + ";"


def decode_audio_string(value: str) -> bytes | None:
    """Decode a base64 data URL, hex string, or base64 string into bytes."""
    text = value.strip()
    try:
        if text.startswith("data:"):
            header, _, payload = text.partition(",")
            if not header.endswith(";base64") or not payload:
                return None
            return base64.b64decode(payload)
        compact = re.sub(r"\s+", "", text)
        if compact[:8].lower().startswith(AUDIO_HEX_PREFIXES) and re.fullmatch(r"[0-9a-fA-F]+", compact):
            return bytes.fromhex(compact)
        return base64.b64decode(compact, validate=True)
    except (ValueError, binascii.Error):
        return None


def _wav_duration(data: bytes) -> float | None:
    position, byte_rate = 12, 0
    while position + 8 <= len(data):
        chunk_id = data[position : position + 4]
        size = int.from_bytes(data[position + 4 : position + 8], "little")
        if chunk_id == b"fmt " and size >= 16:
            byte_rate = int.from_bytes(data[position + 16 : position + 20], "little")
        elif chunk_id == b"data":
            available = min(size, len(data) - position - 8)
            return available / byte_rate if byte_rate else None
        position += 8 + size + (size & 1)
    return None


def _flac_duration(data: bytes) -> float | None:
    # The first metadata block after the signature is STREAMINFO. Its bytes
    # 10-17 pack the sample rate (20 bits), channels and sample size (8 bits),
    # and the total sample count (36 bits).
    if len(data) < 26 or data[4] & 0x7F != 0:
        return None
    packed = int.from_bytes(data[18:26], "big")
    sample_rate = packed >> 44
    total_samples = packed & ((1 << 36) - 1)
    return total_samples / sample_rate if sample_rate and total_samples else None


def audio_duration_seconds(data: bytes, media_type: str) -> float | None:
    """Return the duration stated by a WAV or FLAC header, or ``None``."""
    if media_type == "audio/wav":
        return _wav_duration(data)
    if media_type == "audio/flac":
        return _flac_duration(data)
    return None


def audio_data_url(data: bytes, media_type: str) -> str:
    """Return a playable base64 data URL for audio bytes."""
    return f"data:{media_type};base64,{base64.b64encode(data).decode('ascii')}"


def audio_preview_marker(data: bytes, media_type: str) -> str:
    """Return the payload-free preview marker that stands in for audio bytes."""
    parameters = [AUDIO_PREVIEW_PARAMETER, f"size={len(data)}"]
    duration = audio_duration_seconds(data, media_type)
    if duration is not None:
        parameters.append(f"duration={duration:.3f}")
    return f"data:{media_type};{';'.join(parameters)};base64,"


def audio_bytes_from_value(value: Any, *, path_hint: str | None = None) -> tuple[bytes, str] | None:
    """Return embedded audio bytes and their media type from a scalar value.

    ``value`` may be bytes, a data URL, or a hex or base64 string. A sibling
    path such as the ``path`` field of a Hugging Face ``Audio`` struct names
    the format when the bytes have no recognizable signature.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
    elif isinstance(value, str) and (is_audio_bytes_string(value) or (path_hint and audio_mime_from_path(path_hint))):
        if is_audio_preview_marker(value):
            return None
        data = decode_audio_string(value)
        if data is None:
            return None
        if value.lstrip().startswith("data:audio/"):
            declared = value.lstrip()[5:].split(";", 1)[0].split(",", 1)[0]
            return data, audio_mime_from_bytes(data) or declared
    else:
        return None
    media_type = audio_mime_from_bytes(data) or (audio_mime_from_path(path_hint) if path_hint else None)
    return (data, media_type) if media_type else None


def is_audio_like_value(value: Any) -> bool:
    """Return whether a scalar, ``{bytes, path}`` struct, or list may hold audio."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return audio_mime_from_bytes(bytes(value[:16])) is not None
    if isinstance(value, str):
        return is_audio_reference(value) or (is_audio_bytes_string(value) and audio_bytes_from_value(value[:64]) is not None)
    if isinstance(value, dict):
        path = value.get("path")
        has_audio_path = isinstance(path, str) and is_audio_reference(path)
        raw = value.get("bytes")
        if raw is not None and audio_bytes_from_value(raw, path_hint=path if isinstance(path, str) else None) is not None:
            return True
        return has_audio_path
    if isinstance(value, list):
        return any(is_audio_like_value(item) for item in value[:3])
    return False


def resolve_audio_path(reference: str, *, dataset_path: Path | None, allowed_roots: Sequence[Path]) -> Path:
    """Resolve a local audio path below an allowed root.

    Relative paths are tried next to the dataset first and then below each
    allowed root. Remote URLs are rejected.

    Raises:
        ValueError: The reference is remote, outside the allowed roots, or missing.
    """
    from .files import resolve_path_within_allowed_roots  # noqa: PLC0415

    text = reference.strip()
    if re.match(r"^[a-z][a-z0-9+.-]*://", text, re.IGNORECASE) and not text.lower().startswith("file://"):
        raise ValueError("remote audio URLs cannot be read on the server")
    text = text[len("file://") :] if text.lower().startswith("file://") else text
    candidates = [Path(text)] if Path(text).is_absolute() else []
    if not candidates:
        if dataset_path is not None:
            candidates.append(dataset_path.parent / text)
        candidates.extend(Path(root) / text for root in allowed_roots)
    for candidate in candidates:
        resolved = resolve_path_within_allowed_roots(str(candidate), allowed_roots)
        if resolved is not None and resolved.is_file():
            return resolved
    raise ValueError(f"audio file is missing or outside the allowed roots: {reference}")


def find_audio_source(value: Any) -> tuple[bytes, str] | str | None:
    """Return embedded audio as ``(bytes, media type)`` or a path reference string.

    Structs prefer their embedded bytes over their path, as Hugging Face
    ``Audio`` features do. Lists return their first audio item.
    """
    if isinstance(value, list):
        return next((found for item in value if (found := find_audio_source(item)) is not None), None)
    if isinstance(value, dict):
        path = value.get("path")
        reference = path if isinstance(path, str) else None
        embedded = audio_bytes_from_value(value.get("bytes"), path_hint=reference)
    else:
        reference = value if isinstance(value, str) else None
        embedded = audio_bytes_from_value(value)
    if embedded is not None:
        return embedded
    if reference is not None and not reference.startswith("data:") and is_audio_reference(reference):
        return reference
    return None


def value_at_pointer(value: Any, pointer: Sequence[str | int]) -> Any:
    """Follow dictionary keys and list indexes from a cell value.

    Raises:
        KeyError: A key or index along the pointer does not exist.
    """
    current = value
    for step in pointer:
        if isinstance(current, dict) and str(step) in current:
            current = current[str(step)]
        elif isinstance(current, (list, tuple)) and isinstance(step, int) and 0 <= step < len(current):
            current = current[step]
        else:
            raise KeyError(step)
    return current

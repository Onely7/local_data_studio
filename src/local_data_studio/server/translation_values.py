"""Bounded recursive extraction and restoration of translatable JSON strings."""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

_MEDIA_EXTENSIONS = frozenset(
    {
        ".aac",
        ".avif",
        ".bmp",
        ".flac",
        ".gif",
        ".heic",
        ".jpeg",
        ".jpg",
        ".m4a",
        ".mp3",
        ".mp4",
        ".oga",
        ".ogg",
        ".png",
        ".svg",
        ".tif",
        ".tiff",
        ".wav",
        ".webm",
        ".webp",
    }
)
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$")
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")
_NUMERIC_RE = re.compile(r"^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:e[+-]?\d+)?$", re.IGNORECASE)
_MEDIA_HEX_PREFIX_RE = re.compile(
    r"^(?:89504e47|ffd8ff|47494638|424d|49492a00|4d4d002a|3c737667|494433|4f676753|664c6143|1a45dfa3|52494646)",
    re.IGNORECASE,
)
_MEDIA_BASE64_PREFIX_RE = re.compile(r"^(?:iVBORw0KGgo|/9j/|R0lGOD|Qk|SUkq|TU0AKg|PHN2Zy|PD94bW|SUQz|T2dnUw|ZkxhQw|GkXfo|UklGR)")
_CODE_RE = re.compile(r"(?:^|\s)(?:def |class |function |SELECT\s|INSERT\s|UPDATE\s|DELETE\s|import |from\s+\S+\s+import\s)", re.IGNORECASE)
_PathPart = str | int


@dataclass(frozen=True, slots=True)
class TranslationLeaf:
    """One natural-language string and its location in a copied item."""

    id: str
    item_index: int
    path: tuple[_PathPart, ...]
    text: str


@dataclass(slots=True)
class ExtractedTranslations:
    """Copied source items plus their translatable leaf strings."""

    item_ids: tuple[str, ...]
    values: list[Any]
    leaves: tuple[TranslationLeaf, ...]


def is_translatable_string(value: str) -> bool:
    """Return whether a string appears to contain human-readable language."""
    text = value.strip()
    parsed = urlsplit(text)
    path_text = parsed.path.lower()
    compact = "".join(text.split())
    excluded = (
        not text
        or text.lower().startswith(("data:", "<binary "))
        or parsed.scheme in {"http", "https", "ftp", "file"}
        or bool(_EMAIL_RE.fullmatch(text) or _UUID_RE.fullmatch(text))
        or any(path_text.endswith(extension) for extension in _MEDIA_EXTENSIONS)
        or bool(_NUMERIC_RE.fullmatch(text))
        or text.lower() in {"true", "false", "null"}
        or (not any(character.isspace() for character in text) and ("/" in text or "\\" in text))
        or bool(_MEDIA_HEX_PREFIX_RE.match(compact) or _MEDIA_BASE64_PREFIX_RE.match(compact))
        or (len(compact) >= 64 and bool(_HEX_RE.fullmatch(compact)))
        or (len(compact) >= 96 and len(compact) % 4 == 0 and bool(_BASE64_RE.fullmatch(compact)))
        or bool(_CODE_RE.search(text))
    )
    if excluded:
        return False
    if text[:1] in {"{", "["}:
        try:
            json.loads(text)
        except (TypeError, ValueError):
            pass
        else:
            return False
    return True


def extract_translatable_strings(items: list[tuple[str, Any]], *, max_depth: int = 64) -> ExtractedTranslations:
    """Copy JSON values and collect natural-language leaves in stable order."""
    values = [copy.deepcopy(value) for _, value in items]
    leaves: list[TranslationLeaf] = []

    def visit(value: Any, item_index: int, path: tuple[_PathPart, ...], depth: int) -> None:
        if depth > max_depth:
            raise ValueError(f"translation value nesting exceeds {max_depth} levels")
        if isinstance(value, str):
            if is_translatable_string(value):
                leaves.append(TranslationLeaf(f"s{len(leaves)}", item_index, path, value))
            return
        if isinstance(value, list):
            for index, nested in enumerate(value):
                visit(nested, item_index, (*path, index), depth + 1)
        elif isinstance(value, dict):
            path_hint = value.get("path")
            if "bytes" in value or (
                isinstance(path_hint, str) and any(urlsplit(path_hint).path.lower().endswith(extension) for extension in _MEDIA_EXTENSIONS)
            ):
                return
            for key, nested in value.items():
                visit(nested, item_index, (*path, key), depth + 1)

    for item_index, value in enumerate(values):
        visit(value, item_index, (), 0)
    return ExtractedTranslations(tuple(item_id for item_id, _ in items), values, tuple(leaves))


def restore_translations(extracted: ExtractedTranslations, translated: dict[str, str]) -> list[dict[str, Any]]:
    """Apply an exact translation mapping to copied values and return API items."""
    values = copy.deepcopy(extracted.values)
    for leaf in extracted.leaves:
        current = values[leaf.item_index]
        if not leaf.path:
            values[leaf.item_index] = translated[leaf.id]
            continue
        for part in leaf.path[:-1]:
            current = current[part]
        current[leaf.path[-1]] = translated[leaf.id]
    return [{"id": item_id, "value": value} for item_id, value in zip(extracted.item_ids, values, strict=True)]

"""Pure inference helpers for sampled column values."""

import decimal
from collections import Counter
from typing import Any


def numeric_histogram(values: list[float], bins: int = 8) -> list[int]:
    """Return histogram bin counts for numeric values."""
    if not values:
        return []
    if len(values) == 1:
        return [1]
    min_val = min(values)
    max_val = max(values)
    if min_val == max_val:
        return [len(values)]
    span = max_val - min_val
    width = span / bins if span else 1.0
    counts = [0] * bins
    for value in values:
        index = int((value - min_val) / width) if width else 0
        index = min(bins - 1, max(0, index))
        counts[index] += 1
    return counts


def format_number(value: float) -> str:
    """Format numbers with a compact precision for axis labels."""
    if isinstance(value, int):
        return str(value)
    return f"{value:.3g}"


def infer_kind(values: list[Any]) -> str:
    """Infer a coarse value kind from sample values."""
    kinds: set[str] = set()

    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            kinds.add("boolean")
        elif isinstance(value, (int, float, decimal.Decimal)):
            kinds.add("number")
        elif isinstance(value, str):
            kinds.add("string")
        elif isinstance(value, (list, tuple)):
            kinds.add("list")
        elif isinstance(value, dict):
            kinds.add("dict")
        else:
            kinds.add("other")

    result = "other"
    if not kinds:
        result = "empty"
    else:
        # Keep the historical priority because mixed JSON values depend on it.
        for candidate in ("dict", "list", "string", "number", "boolean", "other"):
            if candidate in kinds:
                result = candidate
                break

    return result


def is_integer_type(type_name: str) -> bool:
    """Check whether a DuckDB type string is integer-like."""
    if not type_name:
        return False
    upper = type_name.upper()
    return any(
        token in upper
        for token in (
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
        )
    )


def number_type_label(type_name: str, is_integer: bool) -> str:
    """Map DuckDB type strings to compact dtype labels."""
    upper = (type_name or "").upper()

    label = "float"
    if is_integer:
        label = "int"
        patterns: list[tuple[str, str]] = [
            ("UTINYINT", "uint8"),
            ("USMALLINT", "uint16"),
            ("UINTEGER", "uint32"),
            ("UBIGINT", "uint64"),
            ("TINYINT", "int8"),
            ("SMALLINT", "int16"),
            ("INTEGER", "int32"),
            ("BIGINT", "int64"),
            ("HUGEINT", "int128"),
        ]
        for pattern, mapped in patterns:
            if pattern in upper:
                label = mapped
                break
    elif "DOUBLE" in upper:
        label = "float64"
    elif "REAL" in upper or "FLOAT" in upper:
        label = "float32"
    elif "DECIMAL" in upper or "NUMERIC" in upper:
        label = "decimal"

    return label


def is_class_like_column(name: str, distinct: int, total: int) -> bool:
    """Heuristically identify class or label string columns."""
    lowered = name.lower()
    keywords = ("label", "class", "category", "source", "type", "tag")
    if any(keyword in lowered for keyword in keywords):
        return True
    if distinct <= 20:
        return True
    return total > 0 and distinct <= 50 and (distinct / total) <= 0.3


def looks_like_url(value: str) -> bool:
    """Return whether a string resembles a supported URL value."""
    lower = value.strip().lower()
    if not lower:
        return False
    return lower.startswith(("http://", "https://", "data:image"))


def looks_like_path(value: str) -> bool:
    """Return whether a string resembles a local file path."""
    if not value or "://" in value:
        return False
    if value.startswith(("./", "../", "/", "~")):
        return True
    return "\\" in value or "/" in value


def is_url_like_column(name: str, values: list[str]) -> bool:
    """Heuristically identify URL-like string columns."""
    lowered = name.lower()
    if any(token in lowered for token in ("url", "uri", "href", "link")):
        return True
    if not values:
        return False
    matches = sum(1 for value in values if looks_like_url(value))
    return matches / len(values) >= 0.4


def is_path_like_column(name: str, values: list[str]) -> bool:
    """Heuristically identify path-like string columns."""
    lowered = name.lower()
    if any(token in lowered for token in ("path", "file", "filename", "filepath", "dir", "folder")):
        return True
    if not values:
        return False
    matches = sum(1 for value in values if looks_like_path(value))
    return matches / len(values) >= 0.4


def format_axis(left: float, right: float) -> dict[str, str]:
    """Create display labels for a histogram range."""
    return {"left": format_number(left), "right": format_number(right)}


def discrete_counts(values: list[int], max_bins: int = 12) -> tuple[list[int], list[str] | None, dict[str, str] | None]:
    """Return discrete bins or a numeric histogram when values are numerous."""
    if not values:
        return [], None, None
    counts = Counter(values)
    if len(counts) <= max_bins:
        ordered = sorted(counts.keys())
        labels = [str(value) for value in ordered]
        bins = [counts[value] for value in ordered]
        return bins, labels, None
    bins = numeric_histogram([float(value) for value in values], bins=8)
    return bins, None, format_axis(min(values), max(values))

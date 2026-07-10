"""Compute lightweight column statistics for the UI."""

import decimal
from collections import Counter
from pathlib import Path
from typing import Any

from .config import DEFAULT_SAMPLE, MAX_SAMPLE
from .db import describe_relation, open_connection, relation_sql


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
        # 元の優先順位: dict > list > string > number > boolean > other
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
        for pat, mapped in patterns:
            if pat in upper:
                label = mapped
                break
    elif "DOUBLE" in upper:
        label = "float64"
    elif "REAL" in upper or "FLOAT" in upper:
        label = "float32"
    elif "DECIMAL" in upper or "NUMERIC" in upper:
        label = "decimal"
    else:
        label = "float"

    return label


def is_class_like_column(name: str, distinct: int, total: int) -> bool:
    """Heuristic for class/label-like string columns."""
    lowered = name.lower()
    keywords = ("label", "class", "category", "source", "type", "tag")
    if any(keyword in lowered for keyword in keywords):
        return True
    if distinct <= 20:
        return True
    if total > 0 and distinct <= 50 and (distinct / total) <= 0.3:
        return True
    return False


def looks_like_url(value: str) -> bool:
    """Return True when a string resembles a URL."""
    lower = value.strip().lower()
    if not lower:
        return False
    return lower.startswith(("http://", "https://", "data:image"))


def looks_like_path(value: str) -> bool:
    """Return True when a string resembles a file path."""
    if not value:
        return False
    if "://" in value:
        return False
    if value.startswith(("./", "../", "/", "~")):
        return True
    return "\\" in value or "/" in value


def is_url_like_column(name: str, values: list[str]) -> bool:
    """Heuristic for URL-like string columns."""
    lowered = name.lower()
    if any(token in lowered for token in ("url", "uri", "href", "link")):
        return True
    if not values:
        return False
    matches = sum(1 for value in values if looks_like_url(value))
    return matches / len(values) >= 0.4


def is_path_like_column(name: str, values: list[str]) -> bool:
    """Heuristic for path-like string columns."""
    lowered = name.lower()
    if any(token in lowered for token in ("path", "file", "filename", "filepath", "dir", "folder")):
        return True
    if not values:
        return False
    matches = sum(1 for value in values if looks_like_path(value))
    return matches / len(values) >= 0.4


def format_axis(left: float, right: float) -> dict[str, str]:
    """Create axis label strings for a histogram range."""
    return {"left": format_number(left), "right": format_number(right)}


def discrete_counts(values: list[int], max_bins: int = 12) -> tuple[list[int], list[str] | None, dict[str, str] | None]:
    """Return discrete bins or fallback numeric histogram when needed."""
    if not values:
        return [], None, None
    counts = Counter(values)
    if len(counts) <= max_bins:
        ordered = sorted(counts.keys())
        labels = [str(val) for val in ordered]
        bins = [counts[val] for val in ordered]
        return bins, labels, None
    bins = numeric_histogram([float(val) for val in values], bins=8)
    return bins, None, format_axis(min(values), max(values))


def compute_column_stats(file: str, path: Path, sample: int | None) -> dict[str, Any]:
    """Compute column summary stats from a sample of rows."""
    rel_sql, params = relation_sql(path)
    sample_size = max(50, min(MAX_SAMPLE, sample or DEFAULT_SAMPLE))

    with open_connection() as con:
        column_info = describe_relation(con, rel_sql, params)
        column_types = {item["name"]: item["type"] for item in column_info}
        result = con.execute(f"SELECT * FROM {rel_sql} LIMIT {sample_size}", params)
        description = result.description or []
        column_names = [desc[0] for desc in description]
        rows = result.fetchall()

    if not rows:
        return {"file": file, "columns": [], "sample": 0}

    values_by_column = [[] for _ in column_names]
    for row in rows:
        for idx, value in enumerate(row):
            values_by_column[idx].append(value)

    stats: list[dict[str, Any]] = []
    for name, values in zip(column_names, values_by_column):
        non_null = [val for val in values if val is not None]
        kind = infer_kind(non_null)

        if not non_null:
            stats.append({"name": name, "kind": "empty", "label": "empty", "bins": []})
            continue

        if kind == "number":
            numeric_values = [float(val) for val in non_null if isinstance(val, (int, float, decimal.Decimal)) and not isinstance(val, bool)]
            if not numeric_values:
                stats.append({"name": name, "kind": "number", "label": "number", "bins": []})
                continue
            column_type = column_types.get(name, "")
            integer_type = is_integer_type(column_type)
            integer_values = [int(val) for val in non_null if isinstance(val, int) and not isinstance(val, bool)]
            is_integer = integer_type and len(integer_values) == len(numeric_values)
            if is_integer:
                bins, labels, axis = discrete_counts(integer_values)
                label = number_type_label(column_type, is_integer=True)
                stats.append(
                    {
                        "name": name,
                        "kind": "number",
                        "label": label,
                        "bins": bins,
                        "axis": axis,
                        "labels": labels,
                    }
                )
            else:
                bins = numeric_histogram(numeric_values)
                axis = format_axis(min(numeric_values), max(numeric_values))
                label = number_type_label(column_type, is_integer=False)
                stats.append(
                    {
                        "name": name,
                        "kind": "number",
                        "label": label,
                        "bins": bins,
                        "axis": axis,
                    }
                )
            continue

        if kind == "boolean":
            true_count = sum(1 for val in non_null if bool(val))
            false_count = len(non_null) - true_count
            stats.append(
                {
                    "name": name,
                    "kind": "boolean",
                    "label": "boolean",
                    "bins": [false_count, true_count],
                    "labels": ["false", "true"],
                }
            )
            continue

        if kind == "string":
            strings = [val for val in non_null if isinstance(val, str)]
            if not strings:
                stats.append({"name": name, "kind": "string", "label": "string", "bins": []})
                continue
            string_label = None
            if is_url_like_column(name, strings):
                string_label = "string / url"
            elif is_path_like_column(name, strings):
                string_label = "string / path"
            distinct = Counter(strings)
            if is_class_like_column(name, len(distinct), len(strings)):
                top = [count for _, count in distinct.most_common(8)]
                stats.append(
                    {
                        "name": name,
                        "kind": "string",
                        "label": string_label or "string / classes",
                        "bins": top,
                        "note": f"{len(distinct)} values",
                    }
                )
            else:
                lengths = [len(val) for val in strings]
                bins = numeric_histogram([float(val) for val in lengths])
                stats.append(
                    {
                        "name": name,
                        "kind": "string",
                        "label": string_label or "string / length",
                        "bins": bins,
                        "axis": format_axis(min(lengths), max(lengths)),
                    }
                )
            continue

        if kind == "list":
            lengths = [len(val) for val in non_null if isinstance(val, (list, tuple))]
            if not lengths:
                stats.append({"name": name, "kind": "list", "label": "list", "bins": []})
                continue
            bins, labels, axis = discrete_counts(lengths)
            stats.append(
                {
                    "name": name,
                    "kind": "list",
                    "label": "list / length",
                    "bins": bins,
                    "axis": axis,
                    "labels": labels,
                }
            )
            continue

        if kind == "dict":
            stats.append({"name": name, "kind": "object", "label": "dict", "bins": []})
            continue

        stats.append({"name": name, "kind": "other", "label": "value", "bins": []})

    return {"file": file, "columns": stats, "sample": len(rows)}

"""Dataset sampling and response construction for column statistics."""

import decimal
from collections import Counter
from pathlib import Path
from typing import Any

from ..config import DEFAULT_SAMPLE, MAX_SAMPLE
from ..db import describe_relation, open_connection, relation_sql
from .heuristics import (
    discrete_counts,
    format_axis,
    infer_kind,
    is_class_like_column,
    is_integer_type,
    is_path_like_column,
    is_url_like_column,
    number_type_label,
    numeric_histogram,
)


def compute_column_stats(file: str, path: Path, sample: int | None) -> dict[str, Any]:
    """Compute column summary stats from a bounded sample of dataset rows."""
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
        for index, value in enumerate(row):
            values_by_column[index].append(value)

    stats: list[dict[str, Any]] = []
    for name, values in zip(column_names, values_by_column):
        non_null = [value for value in values if value is not None]
        kind = infer_kind(non_null)

        if not non_null:
            stats.append({"name": name, "kind": "empty", "label": "empty", "bins": []})
            continue

        if kind == "number":
            numeric_values = [float(value) for value in non_null if isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool)]
            if not numeric_values:
                stats.append({"name": name, "kind": "number", "label": "number", "bins": []})
                continue
            column_type = column_types.get(name, "")
            integer_type = is_integer_type(column_type)
            integer_values = [int(value) for value in non_null if isinstance(value, int) and not isinstance(value, bool)]
            is_integer = integer_type and len(integer_values) == len(numeric_values)
            if is_integer:
                bins, labels, axis = discrete_counts(integer_values)
                stats.append(
                    {
                        "name": name,
                        "kind": "number",
                        "label": number_type_label(column_type, is_integer=True),
                        "bins": bins,
                        "axis": axis,
                        "labels": labels,
                    }
                )
            else:
                stats.append(
                    {
                        "name": name,
                        "kind": "number",
                        "label": number_type_label(column_type, is_integer=False),
                        "bins": numeric_histogram(numeric_values),
                        "axis": format_axis(min(numeric_values), max(numeric_values)),
                    }
                )
            continue

        if kind == "boolean":
            true_count = sum(1 for value in non_null if bool(value))
            stats.append(
                {
                    "name": name,
                    "kind": "boolean",
                    "label": "boolean",
                    "bins": [len(non_null) - true_count, true_count],
                    "labels": ["false", "true"],
                }
            )
            continue

        if kind == "string":
            strings = [value for value in non_null if isinstance(value, str)]
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
                stats.append(
                    {
                        "name": name,
                        "kind": "string",
                        "label": string_label or "string / classes",
                        "bins": [count for _, count in distinct.most_common(8)],
                        "note": f"{len(distinct)} values",
                    }
                )
            else:
                lengths = [len(value) for value in strings]
                stats.append(
                    {
                        "name": name,
                        "kind": "string",
                        "label": string_label or "string / length",
                        "bins": numeric_histogram([float(value) for value in lengths]),
                        "axis": format_axis(min(lengths), max(lengths)),
                    }
                )
            continue

        if kind == "list":
            lengths = [len(value) for value in non_null if isinstance(value, (list, tuple))]
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

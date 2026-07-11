"""Memory-conscious accumulation for one sampled dataset column."""

from __future__ import annotations

import decimal
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from .heuristics import (
    discrete_counts,
    format_axis,
    is_class_like_column,
    is_integer_type,
    looks_like_path,
    looks_like_url,
    number_type_label,
    numeric_histogram,
)

KIND_PRIORITY = ("dict", "list", "string", "number", "boolean", "other")


@dataclass(slots=True)
class ColumnSampleAccumulator:
    """Aggregate only the values required to build one statistics response.

    Mappings and other structured cells are not retained. Strings are retained once
    per distinct value for class frequencies; numeric values and collection lengths
    remain owned because histogram boundaries are known only after sampling.
    """

    non_null_count: int = 0
    kinds: set[str] = field(default_factory=set)
    numeric_values: list[float] = field(default_factory=list)
    integer_values: list[int] = field(default_factory=list)
    truthy_count: int = 0
    string_counts: Counter[str] = field(default_factory=Counter)
    string_lengths: list[int] = field(default_factory=list)
    url_count: int = 0
    path_count: int = 0
    list_lengths: list[int] = field(default_factory=list)

    def add(self, value: Any) -> None:
        """Consume one value without retaining unsupported or structured cells."""
        if value is None:
            return
        self.non_null_count += 1
        self.truthy_count += int(bool(value))
        if isinstance(value, bool):
            self.kinds.add("boolean")
        elif isinstance(value, (int, float, decimal.Decimal)):
            self.kinds.add("number")
            self.numeric_values.append(float(value))
            if isinstance(value, int):
                self.integer_values.append(int(value))
        elif isinstance(value, str):
            self.kinds.add("string")
            self.string_counts[value] += 1
            self.string_lengths.append(len(value))
            self.url_count += int(looks_like_url(value))
            self.path_count += int(looks_like_path(value))
        elif isinstance(value, (list, tuple)):
            self.kinds.add("list")
            self.list_lengths.append(len(value))
        elif isinstance(value, dict):
            self.kinds.add("dict")
        else:
            self.kinds.add("other")

    def inferred_kind(self) -> str:
        """Return the historical highest-priority kind in the bounded sample."""
        if not self.kinds:
            return "empty"
        return next(kind for kind in KIND_PRIORITY if kind in self.kinds)

    def to_response(self, name: str, column_type: str) -> dict[str, Any]:
        """Create the existing JSON-compatible column summary."""
        kind = self.inferred_kind()
        if kind == "empty":
            response = {"name": name, "kind": "empty", "label": "empty", "bins": []}
        elif kind == "number":
            response = self._number_response(name, column_type)
        elif kind == "boolean":
            response = {
                "name": name,
                "kind": "boolean",
                "label": "boolean",
                "bins": [self.non_null_count - self.truthy_count, self.truthy_count],
                "labels": ["false", "true"],
            }
        elif kind == "string":
            response = self._string_response(name)
        elif kind == "list":
            response = self._list_response(name)
        elif kind == "dict":
            response = {"name": name, "kind": "object", "label": "dict", "bins": []}
        else:
            response = {"name": name, "kind": "other", "label": "value", "bins": []}
        return response

    def _number_response(self, name: str, column_type: str) -> dict[str, Any]:
        if not self.numeric_values:
            return {"name": name, "kind": "number", "label": "number", "bins": []}
        is_integer = is_integer_type(column_type) and len(self.integer_values) == len(self.numeric_values)
        if is_integer:
            bins, labels, axis = discrete_counts(self.integer_values)
            return {
                "name": name,
                "kind": "number",
                "label": number_type_label(column_type, is_integer=True),
                "bins": bins,
                "axis": axis,
                "labels": labels,
            }
        return {
            "name": name,
            "kind": "number",
            "label": number_type_label(column_type, is_integer=False),
            "bins": numeric_histogram(self.numeric_values),
            "axis": format_axis(min(self.numeric_values), max(self.numeric_values)),
        }

    def _string_response(self, name: str) -> dict[str, Any]:
        if not self.string_lengths:
            return {"name": name, "kind": "string", "label": "string", "bins": []}
        string_label = None
        if any(token in name.lower() for token in ("url", "uri", "href", "link")) or self.url_count / len(self.string_lengths) >= 0.4:
            string_label = "string / url"
        elif (
            any(token in name.lower() for token in ("path", "file", "filename", "filepath", "dir", "folder"))
            or self.path_count / len(self.string_lengths) >= 0.4
        ):
            string_label = "string / path"
        if is_class_like_column(name, len(self.string_counts), len(self.string_lengths)):
            return {
                "name": name,
                "kind": "string",
                "label": string_label or "string / classes",
                "bins": [count for _, count in self.string_counts.most_common(8)],
                "note": f"{len(self.string_counts)} values",
            }
        return {
            "name": name,
            "kind": "string",
            "label": string_label or "string / length",
            "bins": numeric_histogram([float(value) for value in self.string_lengths]),
            "axis": format_axis(min(self.string_lengths), max(self.string_lengths)),
        }

    def _list_response(self, name: str) -> dict[str, Any]:
        if not self.list_lengths:
            return {"name": name, "kind": "list", "label": "list", "bins": []}
        bins, labels, axis = discrete_counts(self.list_lengths)
        return {
            "name": name,
            "kind": "list",
            "label": "list / length",
            "bins": bins,
            "axis": axis,
            "labels": labels,
        }

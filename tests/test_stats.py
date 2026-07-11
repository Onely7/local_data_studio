"""Characterize the public column-statistics contract."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from local_data_studio.server.stats import (
    compute_column_stats,
    discrete_counts,
    infer_kind,
    numeric_histogram,
)


class ColumnStatisticsContractTests(TestCase):
    """Protect response details used by the column-statistics UI."""

    def test_value_kind_priority_remains_stable(self) -> None:
        """Prefer structured and string kinds when a sample is heterogeneous."""
        self.assertEqual("empty", infer_kind([None]))
        self.assertEqual("boolean", infer_kind([True, False]))
        self.assertEqual("number", infer_kind([True, 3]))
        self.assertEqual("string", infer_kind([3, "three"]))
        self.assertEqual("list", infer_kind(["three", [3]]))
        self.assertEqual("dict", infer_kind([[3], {"value": 3}]))

    def test_histogram_boundaries_remain_stable(self) -> None:
        """Keep empty, constant, discrete, and continuous bin semantics stable."""
        self.assertEqual([], numeric_histogram([]))
        self.assertEqual([1], numeric_histogram([2.0]))
        self.assertEqual([3], numeric_histogram([2.0, 2.0, 2.0]))
        self.assertEqual(([2, 1], ["1", "2"], None), discrete_counts([1, 2, 1]))
        self.assertEqual(([1, 1, 1, 1, 1, 1, 1, 2], None, {"left": "0", "right": "8"}), discrete_counts(list(range(9)), max_bins=2))

    def test_jsonl_statistics_response_remains_stable(self) -> None:
        """Preserve all response fields, ordering, labels, and sampled row count."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sample.jsonl"
            rows = [
                {
                    "integer": index % 4,
                    "floating": index + 0.5,
                    "flag": index % 2 == 0,
                    "url": f"https://example.com/image-{index}.png",
                    "path": f"images/group-{index}/image.png",
                    "category": ("alpha", "beta", "alpha")[index % 3],
                    "items": list(range(index % 4)),
                    "metadata": {"index": index},
                    "empty": None,
                }
                for index in range(24)
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            result = compute_column_stats(path.name, path, sample=50)

        self.assertEqual(
            {
                "file": "sample.jsonl",
                "columns": [
                    {
                        "name": "integer",
                        "kind": "number",
                        "label": "int64",
                        "bins": [6, 6, 6, 6],
                        "axis": None,
                        "labels": ["0", "1", "2", "3"],
                    },
                    {
                        "name": "floating",
                        "kind": "number",
                        "label": "float64",
                        "bins": [3, 3, 3, 3, 3, 3, 3, 3],
                        "axis": {"left": "0.5", "right": "23.5"},
                    },
                    {
                        "name": "flag",
                        "kind": "boolean",
                        "label": "boolean",
                        "bins": [12, 12],
                        "labels": ["false", "true"],
                    },
                    {
                        "name": "url",
                        "kind": "string",
                        "label": "string / url",
                        "bins": [10, 0, 0, 0, 0, 0, 0, 14],
                        "axis": {"left": "31", "right": "32"},
                    },
                    {
                        "name": "path",
                        "kind": "string",
                        "label": "string / path",
                        "bins": [10, 0, 0, 0, 0, 0, 0, 14],
                        "axis": {"left": "24", "right": "25"},
                    },
                    {
                        "name": "category",
                        "kind": "string",
                        "label": "string / classes",
                        "bins": [16, 8],
                        "note": "2 values",
                    },
                    {
                        "name": "items",
                        "kind": "list",
                        "label": "list / length",
                        "bins": [6, 6, 6, 6],
                        "axis": None,
                        "labels": ["0", "1", "2", "3"],
                    },
                    {"name": "metadata", "kind": "object", "label": "dict", "bins": []},
                    {"name": "empty", "kind": "empty", "label": "empty", "bins": []},
                ],
                "sample": 24,
            },
            result,
        )

    def test_statistics_fetch_rows_in_bounded_batches(self) -> None:
        """Verify that statistics avoid materializing DuckDB's complete row matrix."""

        class _FakeResult:
            description = [("value",)]

            def __init__(self) -> None:
                self.fetch_sizes: list[int] = []
                self.batches = [[(1,)], [(2,)], []]

            def fetchmany(self, size: int) -> list[tuple[int]]:
                self.fetch_sizes.append(size)
                return self.batches.pop(0)

        class _FakeConnection:
            def __init__(self, result: _FakeResult) -> None:
                self.result = result

            def __enter__(self) -> _FakeConnection:
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def execute(self, query: str, params: list[str]) -> _FakeResult:
                self.query = query
                self.params = params
                return self.result

        cursor = _FakeResult()
        connection = _FakeConnection(cursor)
        with (
            patch("local_data_studio.server.column_stats.service.relation_sql", return_value=("relation", ["dataset"])),
            patch("local_data_studio.server.column_stats.service.open_connection", return_value=connection),
            patch(
                "local_data_studio.server.column_stats.service.describe_relation",
                return_value=[{"name": "value", "type": "BIGINT"}],
            ),
        ):
            result = compute_column_stats("sample.jsonl", Path("sample.jsonl"), sample=50)

        self.assertEqual([1_024, 1_024, 1_024], cursor.fetch_sizes)
        self.assertEqual(2, result["sample"])
        self.assertEqual([1, 1], result["columns"][0]["bins"])

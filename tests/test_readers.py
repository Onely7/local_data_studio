"""Tests for readers behavior."""

import json
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import HTTPException

from local_data_studio.server.config import MAX_COLUMNS, MAX_JSON_PREVIEW_BYTES, MAX_OFFSET_FALLBACK
from local_data_studio.server.dataset_readers import jsonl as jsonl_reader
from local_data_studio.server.dataset_readers import line as line_reader
from local_data_studio.server.dataset_readers import line_indexing
from local_data_studio.server.dataset_readers import parquet as parquet_reader
from local_data_studio.server.readers import count_rows_with_progress, fetch_preview_page, fetch_raw_row, load_dataset_metadata, search_dataset


class _NoCancelControl:
    def check_cancelled(self) -> None:
        """Exercise check cancelled behavior."""
        return None

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        """Exercise update behavior."""
        return None


class ReaderPreviewTests(TestCase):
    """Test reader preview behavior."""

    def test_jsonl_schema_stops_at_byte_budget_when_rows_are_invalid(self) -> None:
        """Verify that jsonl schema stops at byte budget when rows are invalid."""

        class TrackingBytesIO(BytesIO):
            """Test tracking bytes i o behavior."""

            def close(self) -> None:
                """Exercise close behavior."""
                return None

        invalid_line = b"x" * 1023 + b"\n"
        stream = TrackingBytesIO(invalid_line * (line_reader.JSONL_SCHEMA_MAX_BYTES // len(invalid_line) + 2))

        with patch.object(Path, "open", return_value=stream):
            metadata = jsonl_reader._create_jsonl_metadata(Path("unused.jsonl"))

        self.assertEqual([], metadata.columns)
        self.assertLessEqual(stream.tell(), line_reader.JSONL_SCHEMA_MAX_BYTES + len(invalid_line))

    def test_delimited_reader_supports_fields_larger_than_128_kib(self) -> None:
        """Verify that delimited reader supports fields larger than 128 kib."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "large.csv"
            long_text = "x" * (129 * 1024)
            path.write_text(f"name,description\nAda,{long_text}\n", encoding="utf-8")

            metadata = load_dataset_metadata(path, use_cache=False)
            preview = fetch_preview_page("large.csv", path, limit=1)
            columns, raw = fetch_raw_row(path, 1)
            search = search_dataset("large.csv", path, query="xxx", limit=1, control=_NoCancelControl())

            self.assertEqual(["name", "description"], [column["name"] for column in metadata.columns])
            self.assertIn("truncated", preview["rows"][0][1])
            self.assertEqual(["name", "description"], columns)
            self.assertEqual(long_text, raw[1])
            self.assertEqual([1], search["row_ids"])

    def test_completed_line_index_is_reused_without_reading_dataset(self) -> None:
        """Verify that completed line index is reused without reading dataset."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")
            first = line_reader.build_line_index_with_progress(path, _NoCancelControl())

            with patch.object(Path, "open", side_effect=AssertionError("dataset should not be scanned")):
                second = line_reader.build_line_index_with_progress(path, _NoCancelControl())

            self.assertEqual(first["row_count"], second["row_count"])
            self.assertTrue(second["index"]["complete"])

    def test_line_index_checkpoints_are_saved_as_a_batch(self) -> None:
        """Verify that line index checkpoints are saved as a batch."""

        class FakeIndex:
            """Test fake index behavior."""

            stride = 2

            def __init__(self) -> None:
                self.batches: list[list[tuple[int, int]]] = []

            def status(self) -> dict[str, int | bool | None]:
                """Exercise status behavior."""
                return {"complete": False, "row_count": None, "byte_count": None, "max_indexed_line": None, "max_indexed_byte": None}

            def record_checkpoints(self, checkpoints: list[tuple[int, int]]) -> None:
                """Exercise record checkpoints behavior."""
                self.batches.append(list(checkpoints))

            def mark_complete(self, *, row_count: int, byte_count: int) -> None:
                """Exercise mark complete behavior."""
                return None

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(f'{{"id": {index}}}\n' for index in range(10)), encoding="utf-8")
            fake_index = FakeIndex()
            with patch.object(line_indexing, "LineOffsetIndex", return_value=fake_index):
                result = line_reader.build_line_index_with_progress(path, _NoCancelControl())

            self.assertEqual(10, result["row_count"])
            self.assertEqual(1, len(fake_index.batches))
            self.assertEqual([2, 4, 6, 8, 10], [line_number for line_number, _ in fake_index.batches[0]])

    def test_parquet_raw_row_uses_bounded_record_batches(self) -> None:
        """Verify that parquet raw row uses bounded record batches."""

        class FakeMetadata:
            """Test fake metadata behavior."""

            def row_group(self, row_group: int):  # noqa: ANN001, ARG002
                """Exercise row group behavior."""
                return type("Group", (), {"num_rows": 100})()

        class FakeParquet:
            """Test fake parquet behavior."""

            num_row_groups = 1
            metadata = FakeMetadata()
            schema_arrow = type("Schema", (), {"names": ["id"]})()

            def __init__(self) -> None:
                self.batch_sizes: list[int] = []

            def iter_batches(self, *, batch_size: int, row_groups: list[int], columns: list[str]):  # noqa: ANN001, ARG002
                """Exercise iter batches behavior."""
                self.batch_sizes.append(batch_size)
                yield pa.RecordBatch.from_pylist([{"id": index} for index in range(100)])

        fake = FakeParquet()
        with patch("pyarrow.parquet.ParquetFile", return_value=fake):
            columns, values = parquet_reader.raw_row(Path("data.parquet"), 75)

        self.assertEqual(["id"], columns)
        self.assertEqual([74], values)
        self.assertGreater(fake.batch_sizes[0], 1)

    def test_parquet_offset_cursor_uses_metadata_and_binary_search(self) -> None:
        """Verify that parquet offset cursor uses metadata and binary search."""

        class FakeMetadata:
            """Test fake metadata behavior."""

            def __init__(self) -> None:
                self.calls = 0

            def row_group(self, row_group: int):  # noqa: ANN001, ARG002
                """Exercise row group behavior."""
                self.calls += 1
                return type("Group", (), {"num_rows": 100_000})()

        metadata = FakeMetadata()
        fake = type("Parquet", (), {"num_row_groups": 10, "metadata": metadata})()

        row_group, row_offset, absolute_row = parquet_reader._cursor_for_offset(fake, 750_000, {2, 500_000})

        self.assertEqual((7, 50_002, 750_003), (row_group, row_offset, absolute_row))
        self.assertEqual(10, metadata.calls)

    def test_raw_jsonl_row_is_not_limited_like_preview(self) -> None:
        """Verify that raw jsonl row is not limited like preview."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            long_text = "x" * (MAX_JSON_PREVIEW_BYTES // 64)
            payload = {"text": long_text, "items": list(range(MAX_COLUMNS + 2))}
            path.write_text(f"{json.dumps(payload)}\n", encoding="utf-8")

            preview = fetch_preview_page("data.jsonl", path, limit=1)
            columns, values = fetch_raw_row(path, 1)

            self.assertIn("truncated", preview["rows"][0][0])
            self.assertEqual(["text", "items"], columns)
            self.assertEqual(long_text, values[0])
            self.assertEqual(payload["items"], values[1])

    def test_raw_csv_row_is_not_limited_like_preview(self) -> None:
        """Verify that raw csv row is not limited like preview."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.csv"
            long_text = "x" * (MAX_JSON_PREVIEW_BYTES // 64)
            path.write_text(f"name,description\nAda,{long_text}\n", encoding="utf-8")

            columns, values = fetch_raw_row(path, 1)

            self.assertEqual(["name", "description"], columns)
            self.assertEqual(["Ada", long_text], values)

    def test_raw_parquet_row_reads_one_full_record(self) -> None:
        """Verify that raw parquet row reads one full record."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.parquet"
            long_text = "x" * (MAX_JSON_PREVIEW_BYTES // 64)
            table = pa.Table.from_pylist([{"id": 1, "text": long_text}, {"id": 2, "text": "second"}])
            pq.write_table(table, path, row_group_size=1)

            columns, values = fetch_raw_row(path, 1)

            self.assertEqual(["id", "text"], columns)
            self.assertEqual([1, long_text], values)

    def test_jsonl_preview_uses_next_page_token(self) -> None:
        """Verify that jsonl preview uses next page token."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1, "name": "Ada"}\n{"id": 2, "name": "Linus"}\n', encoding="utf-8")

            first = fetch_preview_page("data.jsonl", path, limit=1)
            second = fetch_preview_page("data.jsonl", path, limit=1, page_token=first["next_page_token"])

            self.assertEqual([[1, "Ada"]], first["rows"])
            self.assertEqual([[2, "Linus"]], second["rows"])
            self.assertIsNone(second["next_page_token"])

    def test_csv_preview_uses_header_and_next_page_token(self) -> None:
        """Verify that csv preview uses header and next page token."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.csv"
            path.write_text("id,name\n1,Ada\n2,Linus\n", encoding="utf-8")

            first = fetch_preview_page("data.csv", path, limit=1)
            second = fetch_preview_page("data.csv", path, limit=1, page_token=first["next_page_token"])

            self.assertEqual(["id", "name"], first["columns"])
            self.assertEqual([["1", "Ada"]], first["rows"])
            self.assertEqual([["2", "Linus"]], second["rows"])

    def test_csv_preview_limits_wide_headers(self) -> None:
        """Verify that csv preview limits wide headers."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "wide.csv"
            header = ",".join(f"col_{index}" for index in range(MAX_COLUMNS + 2))
            row = ",".join(str(index) for index in range(MAX_COLUMNS + 2))
            path.write_text(f"{header}\n{row}\n", encoding="utf-8")

            page = fetch_preview_page("wide.csv", path, limit=1)
            metadata = load_dataset_metadata(path, use_cache=False)

            self.assertEqual(MAX_COLUMNS, len(page["columns"]))
            self.assertEqual(MAX_COLUMNS, len(page["rows"][0]))
            self.assertEqual(MAX_COLUMNS, len(metadata.columns))
            self.assertTrue(page["columns_truncated"])
            self.assertIn("warning", page)
            self.assertIn("warning", metadata.to_response("wide.csv"))

    def test_jsonl_preview_supports_offset_without_page_token(self) -> None:
        """Verify that jsonl preview supports offset without page token."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n', encoding="utf-8")

            page = fetch_preview_page("data.jsonl", path, limit=1, offset=1)

            self.assertEqual([[2]], page["rows"])
            self.assertEqual([2], page["row_ids"])

    def test_jsonl_preview_offset_counts_visible_rows_after_deletes(self) -> None:
        """Verify that jsonl preview offset counts visible rows after deletes."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n', encoding="utf-8")

            page = fetch_preview_page("data.jsonl", path, limit=1, offset=1, deleted_ids=[1])

            self.assertEqual([[3]], page["rows"])
            self.assertEqual([3], page["row_ids"])

    def test_jsonl_preview_limits_dynamic_columns(self) -> None:
        """Verify that jsonl preview limits dynamic columns."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "wide.jsonl"
            payload = {f"col_{index}": index for index in range(MAX_COLUMNS + 2)}
            path.write_text(f"{json.dumps(payload)}\n", encoding="utf-8")

            page = fetch_preview_page("wide.jsonl", path, limit=1)

            self.assertEqual(MAX_COLUMNS, len(page["columns"]))
            self.assertEqual(MAX_COLUMNS, len(page["rows"][0]))
            self.assertIn("warning", page)

    def test_invalid_page_token_is_rejected(self) -> None:
        """Verify that invalid page token is rejected."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n', encoding="utf-8")

            with self.assertRaises(HTTPException) as raised:
                fetch_preview_page("data.jsonl", path, limit=1, page_token="not-a-token")

            self.assertEqual(400, raised.exception.status_code)

    def test_large_offset_without_page_token_is_rejected(self) -> None:
        """Verify that large offset without page token is rejected."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n', encoding="utf-8")

            with self.assertRaises(HTTPException) as raised:
                fetch_preview_page("data.jsonl", path, limit=1, offset=MAX_OFFSET_FALLBACK + 1)

            self.assertEqual(400, raised.exception.status_code)

    def test_json_metadata_is_bounded_and_warns(self) -> None:
        """Verify that json metadata is bounded and warns."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.json"
            path.write_text('[{"id": 1, "name": "Ada"}]', encoding="utf-8")

            metadata = load_dataset_metadata(path, use_cache=False)

            self.assertEqual("json", metadata.file_format)
            self.assertEqual([{"name": "value", "type": "JSON"}], metadata.columns)
            self.assertIsNotNone(metadata.warning)

    def test_large_json_preview_returns_warning_without_parsing_file(self) -> None:
        """Verify that large json preview returns warning without parsing file."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "large.json"
            path.write_text("[", encoding="utf-8")
            with path.open("ab") as file:
                file.truncate(MAX_JSON_PREVIEW_BYTES + 1)

            page = fetch_preview_page("large.json", path, limit=1)

            self.assertEqual([], page["rows"])
            self.assertIn("warning", page)

    def test_small_json_preview_is_limited(self) -> None:
        """Verify that small json preview is limited."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.json"
            path.write_text('[{"id": 1}, {"id": 2}]', encoding="utf-8")

            page = fetch_preview_page("data.json", path, limit=1)

            self.assertEqual([[1]], page["rows"])
            self.assertTrue(page["has_next"])

    def test_metadata_for_jsonl_is_sampled(self) -> None:
        """Verify that metadata for jsonl is sampled."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1, "active": true}\n', encoding="utf-8")

            metadata = load_dataset_metadata(path, use_cache=False)

            self.assertEqual(
                [{"name": "id", "type": "BIGINT"}, {"name": "active", "type": "BOOLEAN"}],
                metadata.columns,
            )

    def test_count_rows_for_line_dataset(self) -> None:
        """Verify that count rows for line dataset."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n\n{"id": 2}\n', encoding="utf-8")

            count = count_rows_with_progress(path, _NoCancelControl())

            self.assertEqual(2, count)

    def test_count_rows_respects_deleted_ids_for_line_dataset(self) -> None:
        """Verify that count rows respects deleted ids for line dataset."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")

            count = count_rows_with_progress(path, _NoCancelControl(), deleted_ids=[2, 999])

            self.assertEqual(1, count)

    def test_search_dataset_skips_deleted_ids_for_jsonl(self) -> None:
        """Verify that search dataset skips deleted ids for jsonl."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1, "name": "Ada"}\n{"id": 2, "name": "Ada"}\n', encoding="utf-8")

            result = search_dataset("data.jsonl", path, query="Ada", limit=10, control=_NoCancelControl(), deleted_ids=[1])

            self.assertEqual([[2, "Ada"]], result["rows"])
            self.assertEqual([2], result["row_ids"])

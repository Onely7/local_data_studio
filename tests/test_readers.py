"""Tests for readers behavior."""

import json
import os
import time
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
from local_data_studio.server.dataset_readers import line_cursor, line_indexing
from local_data_studio.server.dataset_readers import parquet as parquet_reader
from local_data_studio.server.dataset_readers.common import RECENT_ROWS, encode_page_token
from local_data_studio.server.delete_ops import delete_row_from_file
from local_data_studio.server.line_index import LineOffsetIndex
from local_data_studio.server.readers import count_rows_with_progress, dataset_status, fetch_preview_page, fetch_raw_row, load_dataset_metadata, search_dataset


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
        self.assertLessEqual(stream.tell(), line_reader.JSONL_SCHEMA_MAX_BYTES + 1)

    def test_jsonl_schema_does_not_materialize_one_oversized_line(self) -> None:
        """Bound a single ``readline`` call by the remaining schema budget."""

        class TrackingBytesIO(BytesIO):
            """Record the largest bounded read request."""

            def __init__(self, value: bytes) -> None:
                """Initialize the stream and request tracker."""
                super().__init__(value)
                self.maximum_request = 0

            def readline(self, size: int = -1) -> bytes:
                """Track the caller-provided byte bound."""
                self.maximum_request = max(self.maximum_request, size)
                return super().readline(size)

            def close(self) -> None:
                """Keep the stream available for post-call assertions."""

        stream = TrackingBytesIO(b"x" * (line_reader.JSONL_SCHEMA_MAX_BYTES * 2))

        with patch.object(Path, "open", return_value=stream):
            metadata = jsonl_reader._create_jsonl_metadata(Path("unused.jsonl"))

        self.assertEqual([], metadata.columns)
        self.assertEqual(line_reader.JSONL_SCHEMA_MAX_BYTES + 1, stream.maximum_request)
        self.assertEqual(line_reader.JSONL_SCHEMA_MAX_BYTES + 1, stream.tell())

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

    def test_row_index_scan_resumes_where_it_stopped(self) -> None:
        """Index up to a row, then continue from the saved position to the end."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(f'{{"id": {index}}}\n' for index in range(1, 1_001)), encoding="utf-8")
            with patch.object(line_cursor, "LINE_SCAN_BLOCK_BYTES", 512):
                partial = line_reader.build_line_index_with_progress(path, _NoCancelControl(), until_row=300)
                saved = LineOffsetIndex(path).progress()
                with patch.object(line_indexing, "iter_line_blocks", wraps=line_cursor.iter_line_blocks) as scan:
                    full = line_reader.build_line_index_with_progress(path, _NoCancelControl())

            self.assertFalse(partial["complete"])
            self.assertGreaterEqual(partial["indexed_rows"], 300)
            self.assertLess(partial["indexed_rows"], 1_000)
            self.assertIsNotNone(saved)
            self.assertEqual((saved.next_byte, saved.next_row), (scan.call_args.kwargs["start_offset"], scan.call_args.kwargs["first_row"]))
            self.assertEqual(1_000, full["row_count"])
            self.assertTrue(full["complete"])

    def test_parquet_rows_are_addressed_by_physical_row_number(self) -> None:
        """Read any Parquet page directly, across row groups and beyond the offset fallback."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.parquet"
            pq.write_table(pa.Table.from_pylist([{"id": index} for index in range(MAX_OFFSET_FALLBACK + 50)]), path, row_group_size=1_000)

            deep = fetch_preview_page("data.parquet", path, limit=3, offset=MAX_OFFSET_FALLBACK + 10)
            after = fetch_preview_page("data.parquet", path, limit=3, page_token=deep["next_page_token"])
            columns, values = fetch_raw_row(path, MAX_OFFSET_FALLBACK + 1)

            self.assertEqual([[MAX_OFFSET_FALLBACK + 10], [MAX_OFFSET_FALLBACK + 11], [MAX_OFFSET_FALLBACK + 12]], deep["rows"])
            self.assertEqual([MAX_OFFSET_FALLBACK + 11, MAX_OFFSET_FALLBACK + 12, MAX_OFFSET_FALLBACK + 13], deep["row_ids"])
            self.assertEqual([MAX_OFFSET_FALLBACK + 14, MAX_OFFSET_FALLBACK + 15, MAX_OFFSET_FALLBACK + 16], after["row_ids"])
            self.assertEqual((["id"], [MAX_OFFSET_FALLBACK]), (columns, values))

    def test_parquet_column_named_file_row_number_is_read_like_any_column(self) -> None:
        """Read a column whose name DuckDB reserves for its virtual row number."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.parquet"
            pq.write_table(pa.Table.from_pylist([{"file_row_number": index * 10} for index in range(5)]), path, row_group_size=2)

            page = fetch_preview_page("data.parquet", path, limit=2, offset=3)

            self.assertEqual([[30], [40]], page["rows"])
            self.assertEqual([4, 5], page["row_ids"])

    def test_parquet_offset_maps_visible_rows_with_binary_search(self) -> None:
        """Map a visible offset to a physical row while skipping hidden rows."""
        self.assertEqual(750_003, parquet_reader._absolute_row_for_offset(1_000_000, 750_000, {2, 500_000}))
        self.assertEqual(1_000_001, parquet_reader._absolute_row_for_offset(1_000_000, 999_999, {7}))

    def test_parquet_page_rows_follow_the_byte_budget(self) -> None:
        """Shrink pages of wide Parquet rows to the preview byte budget."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "wide.parquet"
            pq.write_table(pa.Table.from_pylist([{"blob": os.urandom(4_096)} for _ in range(20)]), path)

            with patch.object(parquet_reader, "PREVIEW_PAGE_MAX_BYTES", 10_000):
                page = fetch_preview_page("wide.parquet", path, limit=10)

            self.assertEqual([1, 2], page["row_ids"])
            self.assertTrue(page["has_next"])

    def test_line_blocks_skip_blank_lines_and_keep_long_rows_whole(self) -> None:
        """Number only non-blank lines and never split a line across blocks."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            long_value = "y" * 50
            path.write_bytes(f'{{"id": 1}}\r\n \t\r\n\n{{"text": "{long_value}"}}\n{{"id": 3}}'.encode())

            blocks = list(line_cursor.iter_line_blocks(path, start_offset=0, first_row=1, block_bytes=16))
            rows = [(block.first_row + index, block.line(index)) for block in blocks for index in range(block.row_count)]

            self.assertEqual([1, 2, 3], [row for row, _ in rows])
            self.assertEqual(b'{"id": 1}\r\n', rows[0][1])
            self.assertEqual(f'{{"text": "{long_value}"}}\n'.encode(), rows[1][1])
            self.assertEqual(b'{"id": 3}', rows[2][1])

    def test_line_block_checkpoints_are_stride_aligned(self) -> None:
        """Report checkpoints only for row numbers that are multiples of the stride."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(f'{{"id": {index}}}\n' for index in range(1, 11)), encoding="utf-8")

            block = next(line_cursor.iter_line_blocks(path, start_offset=0, first_row=4))

            self.assertEqual([(6, block.starts[2]), (9, block.starts[5]), (12, block.starts[8])], block.checkpoints(3))

    def test_candidate_rows_match_ascii_case_and_caseless_text(self) -> None:
        """Match ASCII case-insensitively, caseless scripts exactly, and defer cased non-ASCII queries."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"name": "ADA ada"}\n{"name": "Linus"}\n{"city": "東京都"}\n{"name": "ada"}\n', encoding="utf-8")
            block = next(line_cursor.iter_line_blocks(path, start_offset=0, first_row=1))

            ascii_rows = list(line_cursor.iter_candidate_rows(block, line_cursor.line_search_needle("Ada")))
            japanese_rows = list(line_cursor.iter_candidate_rows(block, line_cursor.line_search_needle("東京")))
            all_rows = list(line_cursor.iter_candidate_rows(block, line_cursor.line_search_needle("Émile")))

        self.assertEqual([0, 3], ascii_rows)
        self.assertEqual([2], japanese_rows)
        self.assertEqual([0, 1, 2, 3], all_rows)

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

    def test_line_offset_far_from_checkpoints_is_rejected_until_indexed(self) -> None:
        """Reject long unindexed skips, then jump directly once the row index exists."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(f"{index}\n" for index in range(1, 30_001)), encoding="utf-8")

            with patch.object(line_cursor, "MAX_OFFSET_SCAN_ROWS", 5_000), patch.object(line_cursor, "MAX_OFFSET_SCAN_BYTES", 1_000):
                with self.assertRaises(HTTPException) as raised:
                    fetch_preview_page("data.jsonl", path, limit=1, offset=24_000)
                line_reader.build_line_index_with_progress(path, _NoCancelControl(), until_row=24_001)
                far = fetch_preview_page("data.jsonl", path, limit=1, offset=24_000)
                columns, raw = fetch_raw_row(path, 24_500)

            self.assertEqual(409, raised.exception.status_code)
            self.assertEqual({"code": "row_index_required", "row": 24_001}, {key: raised.exception.detail[key] for key in ("code", "row")})
            self.assertEqual([[24_001]], far["rows"])
            self.assertEqual([24_001], far["row_ids"])
            self.assertEqual((["value"], [24_500]), (columns, raw))

    def test_small_line_file_jump_past_the_end_returns_an_empty_page(self) -> None:
        """Scan a small file to its end instead of asking for a row index."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")

            page = fetch_preview_page("data.jsonl", path, limit=5, offset=999_999_998)

            self.assertEqual([], page["rows"])
            self.assertFalse(page["has_next"])
            with self.assertRaises(HTTPException) as raised:
                fetch_raw_row(path, 999_999_999)
            self.assertEqual(404, raised.exception.status_code)

    def test_large_offset_without_page_token_is_rejected_for_duckdb_fallback(self) -> None:
        """Keep the plain-offset guard for formats that only have a DuckDB OFFSET path."""
        with self.assertRaises(HTTPException) as raised:
            fetch_preview_page("data.unknown", Path("data.unknown"), limit=1, offset=MAX_OFFSET_FALLBACK + 1)

        self.assertEqual(400, raised.exception.status_code)

    def test_line_preview_stops_at_the_page_byte_budget(self) -> None:
        """Return a short page with a resume token when rows exceed the byte budget."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(json.dumps({"text": "z" * 100}) + "\n" for _ in range(10)), encoding="utf-8")

            with patch.object(jsonl_reader, "PREVIEW_PAGE_MAX_BYTES", 250):
                page = fetch_preview_page("data.jsonl", path, limit=10)

            self.assertEqual([1, 2, 3], page["row_ids"])
            self.assertTrue(page["has_next"])

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

    def test_search_pages_resume_after_the_last_match(self) -> None:
        """Continue line, delimited, and Parquet searches from the previous page token."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            names = ["Ada", "Linus", "ada lovelace", "Grace", "ADA"]
            (root / "data.jsonl").write_text("".join(json.dumps({"name": name}) + "\n\n" for name in names), encoding="utf-8")
            (root / "data.csv").write_text("name\n" + "".join(f"{name}\n" for name in names), encoding="utf-8")
            pq.write_table(pa.Table.from_pylist([{"name": name} for name in names]), root / "data.parquet", row_group_size=2)

            for file_name in ("data.jsonl", "data.csv", "data.parquet"):
                with self.subTest(file_name=file_name):
                    first = search_dataset(file_name, root / file_name, query="ada", limit=2, control=_NoCancelControl())
                    second = search_dataset(file_name, root / file_name, query="ada", limit=2, control=_NoCancelControl(), page_token=first["next_page_token"])

                    self.assertEqual([1, 3], first["row_ids"])
                    self.assertTrue(first["truncated"])
                    self.assertEqual([5], second["row_ids"])
                    self.assertIsNone(second["next_page_token"])

    def test_search_rejects_a_token_from_another_format(self) -> None:
        """Refuse to resume a search with a cursor created for a different reader."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.csv"
            path.write_text("name\nAda\nAda\n", encoding="utf-8")
            jsonl_token = encode_page_token({"kind": "search-jsonl", "byte_offset": 0, "row_number": 1})

            with self.assertRaises(HTTPException) as raised:
                search_dataset("data.csv", path, query="Ada", limit=1, control=_NoCancelControl(), page_token=jsonl_token)

            self.assertEqual(400, raised.exception.status_code)

    def test_parquet_pages_continue_from_the_previous_reader(self) -> None:
        """Reuse the positioned reader for the next page instead of decoding the row group again."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.parquet"
            pq.write_table(pa.Table.from_pylist([{"id": index} for index in range(5_000)]), path, row_group_size=5_000)

            first = fetch_preview_page("data.parquet", path, limit=10, offset=1_000)
            with patch.object(parquet_reader, "_open_cursor", side_effect=AssertionError("reader reopened")):
                second = fetch_preview_page("data.parquet", path, limit=10, page_token=first["next_page_token"])

        self.assertEqual(list(range(1_001, 1_011)), first["row_ids"])
        self.assertEqual(list(range(1_011, 1_021)), second["row_ids"])

    def test_raw_rows_of_a_previewed_page_are_served_from_memory(self) -> None:
        """Answer raw row requests for visible rows without reading the dataset again."""
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data.jsonl").write_text('{"id": 1, "text": "a"}\n{"id": 2, "text": "b"}\n', encoding="utf-8")
            (root / "data.csv").write_text("id,text\n1,a\n2,b\n", encoding="utf-8")
            pq.write_table(pa.Table.from_pylist([{"id": 1, "text": "a"}, {"id": 2, "text": "b"}]), root / "data.parquet")

            for name, expected in (("data.jsonl", [2, "b"]), ("data.csv", ["2", "b"]), ("data.parquet", [2, "b"])):
                with self.subTest(name=name):
                    fetch_preview_page(name, root / name, limit=10)
                    with (
                        patch.object(Path, "open", side_effect=AssertionError("dataset read again")),
                        patch.object(parquet_reader, "_read_rows", side_effect=AssertionError("dataset read again")),
                    ):
                        columns, values = fetch_raw_row(root / name, 2)

                    self.assertEqual((["id", "text"], expected), (columns, values))

    def test_csv_quoted_newlines_stay_inside_one_row(self) -> None:
        """Keep quoted multi-line fields in one row for preview, count, raw rows, and search."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "notes.csv"
            path.write_text('id,note\n1,"first\nline"\n2,plain\n3,"has ""quotes""\nand ""more"""\n4,"ends with\n"\n', encoding="utf-8")

            page = fetch_preview_page("notes.csv", path, limit=10)
            count = count_rows_with_progress(path, _NoCancelControl())
            RECENT_ROWS.clear()
            columns, raw = fetch_raw_row(path, 3)
            search = search_dataset("notes.csv", path, query="more", limit=5, control=_NoCancelControl())

        self.assertEqual([1, 2, 3, 4], page["row_ids"])
        self.assertEqual([["1", "first\nline"], ["2", "plain"], ["3", 'has "quotes"\nand "more"'], ["4", "ends with\n"]], page["rows"])
        self.assertNotIn("warning", page)
        self.assertEqual(4, count)
        self.assertEqual((["id", "note"], ["3", 'has "quotes"\nand "more"']), (columns, raw))
        self.assertEqual([3], search["row_ids"])

    def test_csv_with_unbalanced_quotes_splits_rows_at_line_breaks(self) -> None:
        """Fall back to one row per line, with a warning, when quotes cannot mark row ends."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "messy.csv"
            path.write_text('id,note\n1,"spans\nlines"\n2,5" display\n3,ok\n', encoding="utf-8")

            page = fetch_preview_page("messy.csv", path, limit=10)

        self.assertEqual([1, 2, 3, 4], page["row_ids"])
        self.assertIn("quote", page["warning"])

    def test_deleting_keeps_quoted_newlines_and_refuses_line_split_csv(self) -> None:
        """Delete CSV records by the preview's numbering, and refuse files whose rows are split at line breaks."""
        with TemporaryDirectory() as tmpdir:
            quoted = Path(tmpdir) / "notes.csv"
            quoted.write_text('id,note\n1,"first\nline"\n2,plain\n3,"third\nline"\n', encoding="utf-8")
            messy = Path(tmpdir) / "messy.csv"
            messy.write_text('id,note\n1,"spans\nlines"\n2,5" display\n', encoding="utf-8")

            delete_row_from_file(quoted, 2)
            page = fetch_preview_page("notes.csv", quoted, limit=10)
            with self.assertRaises(HTTPException) as raised:
                delete_row_from_file(messy, 1)

        self.assertEqual([["1", "first\nline"], ["3", "third\nline"]], page["rows"])
        self.assertEqual(400, raised.exception.status_code)


class BackgroundIndexTests(TestCase):
    """Test background row indexing and dataset status."""

    def setUp(self) -> None:
        """Stop background scans after each test."""
        self.addCleanup(line_indexing.shutdown_index_builders)

    def _wait_until_idle(self, path: Path) -> None:
        builder = line_indexing.index_builder(path)
        deadline = time.monotonic() + 10
        while builder.snapshot().running and time.monotonic() < deadline:
            time.sleep(0.01)

    def test_background_index_stops_at_its_byte_budget(self) -> None:
        """Index only the configured prefix and estimate the remaining rows."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(json.dumps({"id": index}) + "\n" for index in range(1, 5_001)), encoding="utf-8")
            with (
                patch.object(line_cursor, "LINE_SCAN_BLOCK_BYTES", 4_096),
                patch.object(line_indexing, "BACKGROUND_INDEX", True),
                patch.object(line_indexing, "BACKGROUND_INDEX_MAX_BYTES", 20_000),
            ):
                started = line_indexing.start_background_index(path)
                self._wait_until_idle(path)
                status = dataset_status("data.jsonl", path)

            self.assertTrue(started)
            self.assertFalse(status["indexing"])
            self.assertTrue(status["background"])
            self.assertFalse(status["complete"])
            self.assertIsNone(status["row_count"])
            self.assertGreaterEqual(status["indexed_bytes"], 20_000)
            self.assertLess(status["indexed_bytes"], path.stat().st_size)
            self.assertAlmostEqual(5_000, status["estimated_row_count"], delta=500)

    def test_background_index_counts_files_within_its_budget(self) -> None:
        """Report an exact row count once the background scan reaches the end."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.csv"
            path.write_text("id\n" + "".join(f"{index}\n" for index in range(1, 101)), encoding="utf-8")
            with patch.object(line_indexing, "BACKGROUND_INDEX", True), patch.object(line_indexing, "BACKGROUND_INDEX_MAX_BYTES", 0):
                line_indexing.start_background_index(path)
                self._wait_until_idle(path)
                status = dataset_status("data.csv", path, deleted_ids=[3])

            self.assertTrue(status["complete"])
            self.assertEqual(99, status["row_count"])

    def test_background_index_can_be_turned_off(self) -> None:
        """Leave datasets unindexed when the setting is off."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n', encoding="utf-8")
            with patch.object(line_indexing, "BACKGROUND_INDEX", False):
                started = line_indexing.start_background_index(path)
                status = dataset_status("data.jsonl", path)

            self.assertFalse(started)
            self.assertFalse(status["background"])
            self.assertEqual(0, status["indexed_rows"])

    def test_opening_another_dataset_releases_the_previous_background_scan(self) -> None:
        """Keep at most one background scan, for the dataset opened last."""
        with TemporaryDirectory() as tmpdir:
            first = Path(tmpdir) / "first.jsonl"
            second = Path(tmpdir) / "second.jsonl"
            for path in (first, second):
                path.write_text('{"id": 1}\n', encoding="utf-8")
            with patch.object(line_indexing, "BACKGROUND_INDEX", True):
                line_indexing.start_background_index(first)
                line_indexing.start_background_index(second)

            self.assertFalse(line_indexing.index_builder(first).snapshot().background)
            self.assertTrue(line_indexing.index_builder(second).snapshot().background)

    def test_dataset_status_reads_parquet_rows_from_the_footer(self) -> None:
        """Report Parquet row counts immediately, minus deleted rows."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.parquet"
            pq.write_table(pa.Table.from_pylist([{"id": index} for index in range(10)]), path)

            status = dataset_status("data.parquet", path, deleted_ids=[1, 2])

        self.assertEqual(8, status["row_count"])
        self.assertTrue(status["complete"])
        self.assertFalse(status["indexing"])

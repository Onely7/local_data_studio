import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from fastapi import HTTPException

from server.config import MAX_COLUMNS, MAX_JSON_PREVIEW_BYTES, MAX_OFFSET_FALLBACK
from server.readers import count_rows_with_progress, fetch_preview_page, load_dataset_metadata, search_dataset


class _NoCancelControl:
    def check_cancelled(self) -> None:
        return None

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        return None


class ReaderPreviewTests(TestCase):
    def test_jsonl_preview_uses_next_page_token(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1, "name": "Ada"}\n{"id": 2, "name": "Linus"}\n', encoding="utf-8")

            first = fetch_preview_page("data.jsonl", path, limit=1)
            second = fetch_preview_page("data.jsonl", path, limit=1, page_token=first["next_page_token"])

            self.assertEqual([[1, "Ada"]], first["rows"])
            self.assertEqual([[2, "Linus"]], second["rows"])
            self.assertIsNone(second["next_page_token"])

    def test_csv_preview_uses_header_and_next_page_token(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.csv"
            path.write_text("id,name\n1,Ada\n2,Linus\n", encoding="utf-8")

            first = fetch_preview_page("data.csv", path, limit=1)
            second = fetch_preview_page("data.csv", path, limit=1, page_token=first["next_page_token"])

            self.assertEqual(["id", "name"], first["columns"])
            self.assertEqual([["1", "Ada"]], first["rows"])
            self.assertEqual([["2", "Linus"]], second["rows"])

    def test_csv_preview_limits_wide_headers(self) -> None:
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
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n', encoding="utf-8")

            page = fetch_preview_page("data.jsonl", path, limit=1, offset=1)

            self.assertEqual([[2]], page["rows"])
            self.assertEqual([2], page["row_ids"])

    def test_jsonl_preview_offset_counts_visible_rows_after_deletes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n', encoding="utf-8")

            page = fetch_preview_page("data.jsonl", path, limit=1, offset=1, deleted_ids=[1])

            self.assertEqual([[3]], page["rows"])
            self.assertEqual([3], page["row_ids"])

    def test_jsonl_preview_limits_dynamic_columns(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "wide.jsonl"
            payload = {f"col_{index}": index for index in range(MAX_COLUMNS + 2)}
            path.write_text(f"{json.dumps(payload)}\n", encoding="utf-8")

            page = fetch_preview_page("wide.jsonl", path, limit=1)

            self.assertEqual(MAX_COLUMNS, len(page["columns"]))
            self.assertEqual(MAX_COLUMNS, len(page["rows"][0]))
            self.assertIn("warning", page)

    def test_invalid_page_token_is_rejected(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n', encoding="utf-8")

            with self.assertRaises(HTTPException) as raised:
                fetch_preview_page("data.jsonl", path, limit=1, page_token="not-a-token")

            self.assertEqual(400, raised.exception.status_code)

    def test_large_offset_without_page_token_is_rejected(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n', encoding="utf-8")

            with self.assertRaises(HTTPException) as raised:
                fetch_preview_page("data.jsonl", path, limit=1, offset=MAX_OFFSET_FALLBACK + 1)

            self.assertEqual(400, raised.exception.status_code)

    def test_json_metadata_is_bounded_and_warns(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.json"
            path.write_text('[{"id": 1, "name": "Ada"}]', encoding="utf-8")

            metadata = load_dataset_metadata(path, use_cache=False)

            self.assertEqual("json", metadata.file_format)
            self.assertEqual([{"name": "value", "type": "JSON"}], metadata.columns)
            self.assertIsNotNone(metadata.warning)

    def test_large_json_preview_returns_warning_without_parsing_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "large.json"
            path.write_text("[", encoding="utf-8")
            with path.open("ab") as file:
                file.truncate(MAX_JSON_PREVIEW_BYTES + 1)

            page = fetch_preview_page("large.json", path, limit=1)

            self.assertEqual([], page["rows"])
            self.assertIn("warning", page)

    def test_small_json_preview_is_limited(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.json"
            path.write_text('[{"id": 1}, {"id": 2}]', encoding="utf-8")

            page = fetch_preview_page("data.json", path, limit=1)

            self.assertEqual([[1]], page["rows"])
            self.assertTrue(page["has_next"])

    def test_metadata_for_jsonl_is_sampled(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1, "active": true}\n', encoding="utf-8")

            metadata = load_dataset_metadata(path, use_cache=False)

            self.assertEqual(
                [{"name": "id", "type": "BIGINT"}, {"name": "active", "type": "BOOLEAN"}],
                metadata.columns,
            )

    def test_count_rows_for_line_dataset(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n\n{"id": 2}\n', encoding="utf-8")

            count = count_rows_with_progress(path, _NoCancelControl())

            self.assertEqual(2, count)

    def test_count_rows_respects_deleted_ids_for_line_dataset(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")

            count = count_rows_with_progress(path, _NoCancelControl(), deleted_ids=[2, 999])

            self.assertEqual(1, count)

    def test_search_dataset_skips_deleted_ids_for_jsonl(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text('{"id": 1, "name": "Ada"}\n{"id": 2, "name": "Ada"}\n', encoding="utf-8")

            result = search_dataset("data.jsonl", path, query="Ada", limit=10, control=_NoCancelControl(), deleted_ids=[1])

            self.assertEqual([[2, "Ada"]], result["rows"])
            self.assertEqual([2], result["row_ids"])

import asyncio
from pathlib import Path
from time import sleep
from unittest import TestCase
from unittest.mock import patch

from app import (
    AtlasQueryRequest,
    AtlasRequest,
    CountJobRequest,
    EdaQueryRequest,
    EdaRequest,
    IndexJobRequest,
    QueryRequest,
    SearchJobRequest,
    StatsJobRequest,
    get_job,
    preview,
    start_atlas_job,
    start_atlas_query_job,
    start_count_job,
    start_eda_job,
    start_eda_query_job,
    start_index_job,
    start_query_job,
    start_search_job,
    start_stats_job,
)
from server.atlas import AtlasPreparedDataset


class ApiJobTests(TestCase):
    def test_preview_returns_page_token(self) -> None:
        payload = asyncio.run(preview(file="example.jsonl", limit=2, offset=0, page_token=None))

        self.assertEqual(2, len(payload["rows"]))
        self.assertIn("next_page_token", payload)

    def test_count_job_completes(self) -> None:
        started = asyncio.run(start_count_job(CountJobRequest(file="example.jsonl")))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(10, payload["result"]["count"])
        self.assertIn("cached", payload["result"])

    def test_search_job_returns_matching_rows(self) -> None:
        started = asyncio.run(start_search_job(SearchJobRequest(file="example.jsonl", query="horse", limit=5)))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertGreaterEqual(len(payload["result"]["rows"]), 1)
        self.assertIn("cached", payload["result"])

    def test_index_job_builds_line_index(self) -> None:
        started = asyncio.run(start_index_job(IndexJobRequest(file="example.jsonl")))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(10, payload["result"]["row_count"])
        self.assertTrue(payload["result"]["index"]["complete"])

    def test_query_job_returns_limited_rows(self) -> None:
        started = asyncio.run(start_query_job(QueryRequest(file="example.jsonl", sql="SELECT * FROM data", limit=3)))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(3, len(payload["result"]["rows"]))
        self.assertIn("columns", payload["result"])

    def test_stats_job_completes(self) -> None:
        started = asyncio.run(start_stats_job(StatsJobRequest(file="example.jsonl", sample=50)))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertIn("columns", payload["result"])

    def test_eda_query_job_completes_for_filtered_results(self) -> None:
        report_columns: list[str] = []

        class FakeReport:
            def to_file(self, path: str) -> None:
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("<html>query eda</html>")

        def fake_build_report(df, title: str, minimal: bool):  # noqa: ANN001, ARG001
            report_columns.extend(df.columns)
            return FakeReport()

        with patch("server.eda_reports.build_eda_report", side_effect=fake_build_report):
            started = asyncio.run(
                start_eda_query_job(
                    EdaQueryRequest(
                        file="example.jsonl",
                        sql=("SELECT row_number() OVER () AS rn, object, final_rating FROM data WHERE object = 'horse'"),
                        sample=100,
                        force=True,
                    )
                )
            )
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual("query", payload["result"]["source"])
        self.assertIn("/cache/", payload["result"]["url"])
        self.assertNotIn("rn", report_columns)
        self.assertIn("object", report_columns)

    def test_eda_job_completes_for_dataset_sample(self) -> None:
        started = asyncio.run(start_eda_job(EdaRequest(file="example.jsonl", sample=100, force=True, mode="minimal")))
        payload = self._wait_for_job(started["id"], attempts=80)

        self.assertEqual("succeeded", payload["status"])
        self.assertIn("/cache/", payload["result"]["url"])
        self.assertEqual(100, payload["result"]["sample"])

    def test_atlas_job_launches_for_selected_image_column(self) -> None:
        model_path = (Path.cwd() / "models" / "embedder" / "test-image-model").resolve()
        prepared_path = (Path.cwd() / "cache" / "atlas" / "datasets" / "prepared.parquet").resolve()

        def fake_launch(command, context):  # noqa: ANN001
            self.assertIn("--image", command)
            self.assertIn("image", command)
            self.assertIn("--disable-projection", command)
            self.assertIn(str(prepared_path), command)
            context.update(progress=1.0, message="ready")
            return "http://127.0.0.1:5055/", 12345

        with (
            patch("server.atlas.resolve_embedder_model", return_value=model_path),
            patch(
                "server.atlas.prepare_atlas_dataset",
                return_value=AtlasPreparedDataset(
                    path=prepared_path,
                    x="x",
                    y="y",
                    neighbors=None,
                    cache_hit=True,
                ),
            ),
            patch("server.atlas.launch_embedding_atlas", side_effect=fake_launch),
        ):
            started = asyncio.run(start_atlas_job(AtlasRequest(file="example.jsonl", column="image", model="test-image-model")))
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual("dataset", payload["result"]["source"])
        self.assertEqual("image", payload["result"]["modality"])
        self.assertEqual("test-image-model", payload["result"]["model"])
        self.assertEqual("http://127.0.0.1:5055/", payload["result"]["url"])
        self.assertTrue(payload["result"]["cache_hit"])

    def test_atlas_query_job_passes_guarded_sql(self) -> None:
        model_path = (Path.cwd() / "models" / "embedder" / "test-text-model").resolve()
        prepared_path = (Path.cwd() / "cache" / "atlas" / "datasets" / "prepared-query.parquet").resolve()
        captured_command: list[str] = []

        def fake_launch(command, context):  # noqa: ANN001
            captured_command.extend(command)
            context.update(progress=1.0, message="ready")
            return "http://127.0.0.1:5056/", 12346

        with (
            patch("server.atlas.resolve_embedder_model", return_value=model_path),
            patch(
                "server.atlas.prepare_atlas_dataset",
                return_value=AtlasPreparedDataset(
                    path=prepared_path,
                    x="x",
                    y="y",
                    neighbors=None,
                    cache_hit=False,
                ),
            ),
            patch("server.atlas.launch_embedding_atlas", side_effect=fake_launch),
        ):
            started = asyncio.run(
                start_atlas_query_job(
                    AtlasQueryRequest(
                        file="example.jsonl",
                        column="object",
                        model="test-text-model",
                        sql="SELECT object, short_description FROM data WHERE object = 'horse';",
                    )
                )
            )
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual("query", payload["result"]["source"])
        self.assertEqual("text", payload["result"]["modality"])
        self.assertIn("--disable-projection", captured_command)
        self.assertIn("--x", captured_command)
        self.assertIn("--y", captured_command)
        self.assertNotIn("--neighbors", captured_command)
        self.assertNotIn("--query", captured_command)
        self.assertNotIn("--model", captured_command)
        self.assertIn(str(prepared_path), captured_command)
        self.assertFalse(payload["result"]["cache_hit"])

    def _wait_for_job(self, job_id: str, attempts: int = 20) -> dict:
        payload = {}
        for _ in range(attempts):
            payload = asyncio.run(get_job(job_id))
            if payload["status"] in {"succeeded", "failed", "cancelled"}:
                return payload
            sleep(0.05)
        return payload

"""Tests for api jobs behavior."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from unittest import TestCase
from unittest.mock import patch

from local_data_studio.app import (
    AtlasQueryRequest,
    AtlasRequest,
    CountJobRequest,
    EdaQueryRequest,
    EdaRequest,
    IndexJobRequest,
    QueryRequest,
    RawRowRequest,
    SearchJobRequest,
    StatsJobRequest,
    get_job,
    preview,
    raw_row,
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
from local_data_studio.server.atlas import AtlasPreparedDataset
from local_data_studio.server.sql import load_query_dataframe_guarded


class ApiJobTests(TestCase):
    """Test api job behavior."""

    def test_raw_row_endpoint_returns_full_values(self) -> None:
        """Verify that raw row endpoint returns full values."""
        with self.subTest("dataset row"):
            long_text = "x" * 2_500
            with TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "data.jsonl"
                path.write_text(json.dumps({"text": long_text, "items": list(range(40))}) + "\n", encoding="utf-8")
                with patch("local_data_studio.server.api.datasets.resolve_data_file", return_value=path) as resolve_data_file:
                    payload = raw_row(RawRowRequest(file="example.jsonl", row_id=1))

            self.assertEqual(long_text, payload["row"][0])
            self.assertEqual(list(range(40)), payload["row"][1])
            resolve_data_file.assert_called_once_with("example.jsonl")

        with self.subTest("query result"):
            long_text = "x" * 2_500
            with patch("local_data_studio.server.api.datasets.fetch_raw_query_row_guarded", return_value=(["text"], [long_text])):
                payload = raw_row(RawRowRequest(file="example.jsonl", sql="SELECT text FROM data", offset=0))

            self.assertEqual(long_text, payload["row"][0])

    def test_preview_returns_page_token(self) -> None:
        """Verify that preview returns page token."""
        payload = preview(file="example.jsonl", limit=2, offset=0, page_token=None)

        self.assertEqual(2, len(payload["rows"]))
        self.assertIn("next_page_token", payload)

    def test_count_job_completes(self) -> None:
        """Verify that count job completes."""
        started = start_count_job(CountJobRequest(file="example.jsonl"))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(10, payload["result"]["count"])
        self.assertIn("cached", payload["result"])

    def test_search_job_returns_matching_rows(self) -> None:
        """Verify that search job returns matching rows."""
        started = start_search_job(SearchJobRequest(file="example.jsonl", query="horse", limit=5))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertGreaterEqual(len(payload["result"]["rows"]), 1)
        self.assertIn("cached", payload["result"])

    def test_index_job_builds_line_index(self) -> None:
        """Verify that index job builds line index."""
        started = start_index_job(IndexJobRequest(file="example.jsonl"))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(10, payload["result"]["row_count"])
        self.assertTrue(payload["result"]["index"]["complete"])

    def test_query_job_returns_limited_rows(self) -> None:
        """Verify that query job returns limited rows."""
        started = start_query_job(QueryRequest(file="example.jsonl", sql="SELECT * FROM data", limit=3))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(3, len(payload["result"]["rows"]))
        self.assertIn("columns", payload["result"])

    def test_stats_job_completes(self) -> None:
        """Verify that stats job completes."""
        started = start_stats_job(StatsJobRequest(file="example.jsonl", sample=50))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertIn("columns", payload["result"])

    def test_eda_query_job_completes_for_filtered_results(self) -> None:
        """Verify that eda query job completes for filtered results."""
        report_columns: list[str] = []

        class FakeReport:
            """Test fake report behavior."""

            def to_file(self, path: str) -> None:
                """Exercise to file behavior."""
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("<html>query eda</html>")

        def fake_build_report(df, title: str, minimal: bool):  # noqa: ANN001, ARG001
            """Exercise fake build report behavior."""
            report_columns.extend(df.columns)
            return FakeReport()

        with patch("local_data_studio.server.eda_reports.build_eda_report", side_effect=fake_build_report):
            started = start_eda_query_job(
                EdaQueryRequest(
                    file="example.jsonl",
                    sql=("SELECT row_number() OVER () AS rn, object, final_rating FROM data WHERE object = 'horse'"),
                    sample=100,
                    force=True,
                )
            )
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual("query", payload["result"]["source"])
        self.assertIn("/cache/eda/", payload["result"]["url"])
        self.assertNotIn("rn", report_columns)
        self.assertIn("object", report_columns)

    def test_query_eda_loader_returns_pandas_dataframe(self) -> None:
        """Verify that query eda loader returns pandas dataframe."""
        dataframe = load_query_dataframe_guarded(
            path=Path.cwd() / "data" / "example.jsonl",
            sql="SELECT object, final_rating FROM data WHERE object = 'horse'",
            sample=100,
            context=None,
        )

        self.assertEqual("DataFrame", type(dataframe).__name__)
        self.assertFalse(dataframe.empty)

    def test_eda_job_completes_for_dataset_sample(self) -> None:
        """Verify that eda job completes for dataset sample."""
        started = start_eda_job(EdaRequest(file="example.jsonl", sample=100, force=True, mode="minimal"))
        payload = self._wait_for_job(started["id"], attempts=80)

        self.assertEqual("succeeded", payload["status"])
        self.assertIn("/cache/eda/", payload["result"]["url"])
        self.assertEqual(100, payload["result"]["sample"])

    def test_atlas_job_launches_for_selected_image_column(self) -> None:
        """Verify that atlas job launches for selected image column."""
        model_path = (Path.cwd() / "models" / "embedder" / "test-image-model").resolve()
        prepared_path = (Path.cwd() / "cache" / "atlas" / "datasets" / "prepared.parquet").resolve()

        def fake_launch(command, context):  # noqa: ANN001
            """Exercise fake launch behavior."""
            self.assertIn("--image", command)
            self.assertIn("image", command)
            self.assertIn("--disable-projection", command)
            self.assertIn(str(prepared_path), command)
            context.update(progress=1.0, message="ready")
            return "http://127.0.0.1:5055/", 12345

        with (
            patch("local_data_studio.server.atlas_components.service.resolve_embedder_model", return_value=model_path),
            patch("local_data_studio.server.atlas_components.service._resolve_backend", return_value="transformers"),
            patch("local_data_studio.server.atlas_components.service.analyze_model_capabilities") as analyze_capabilities,
            patch(
                "local_data_studio.server.atlas_components.service.prepare_atlas_dataset",
                return_value=AtlasPreparedDataset(
                    path=prepared_path,
                    x="x",
                    y="y",
                    neighbors=None,
                    cache_hit=True,
                ),
            ),
            patch("local_data_studio.server.atlas_components.service.launch_embedding_atlas", side_effect=fake_launch),
        ):
            analyze_capabilities.return_value.fingerprint = "test-capability"
            started = start_atlas_job(AtlasRequest(file="example.jsonl", column="image", model="test-image-model"))
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual("dataset", payload["result"]["source"])
        self.assertEqual("image", payload["result"]["modality"])
        self.assertEqual("test-image-model", payload["result"]["model"])
        self.assertEqual("http://127.0.0.1:5055/", payload["result"]["url"])
        self.assertTrue(payload["result"]["cache_hit"])

    def test_atlas_query_job_passes_guarded_sql(self) -> None:
        """Verify that atlas query job passes guarded sql."""
        model_path = (Path.cwd() / "models" / "embedder" / "test-text-model").resolve()
        prepared_path = (Path.cwd() / "cache" / "atlas" / "datasets" / "prepared-query.parquet").resolve()
        captured_command: list[str] = []

        def fake_launch(command, context):  # noqa: ANN001
            """Exercise fake launch behavior."""
            captured_command.extend(command)
            context.update(progress=1.0, message="ready")
            return "http://127.0.0.1:5056/", 12346

        with (
            patch("local_data_studio.server.atlas_components.service.resolve_embedder_model", return_value=model_path),
            patch("local_data_studio.server.atlas_components.service._resolve_backend", return_value="sentence-transformers"),
            patch("local_data_studio.server.atlas_components.service.analyze_model_capabilities") as analyze_capabilities,
            patch(
                "local_data_studio.server.atlas_components.service.prepare_atlas_dataset",
                return_value=AtlasPreparedDataset(
                    path=prepared_path,
                    x="x",
                    y="y",
                    neighbors=None,
                    cache_hit=False,
                ),
            ),
            patch("local_data_studio.server.atlas_components.service.launch_embedding_atlas", side_effect=fake_launch),
        ):
            analyze_capabilities.return_value.fingerprint = "test-capability"
            started = start_atlas_query_job(
                AtlasQueryRequest(
                    file="example.jsonl",
                    column="object",
                    model="test-text-model",
                    sql="SELECT object, short_description FROM data WHERE object = 'horse';",
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
            payload = get_job(job_id)
            if payload["status"] in {"succeeded", "failed", "cancelled"}:
                return payload
            sleep(0.05)
        return payload

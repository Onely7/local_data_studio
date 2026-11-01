"""Tests for api jobs behavior."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from unittest import TestCase
from unittest.mock import patch

from fastapi import HTTPException

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
    TranslationRequest,
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
    start_translation_job,
)
from local_data_studio.server import sql as sql_module
from local_data_studio.server.atlas import AtlasPreparedDataset
from local_data_studio.server.atlas_components.process import LaunchedAtlasProcess
from local_data_studio.server.config import ATLAS_LARGE_DATASET_SAMPLE
from local_data_studio.server.sql import load_query_dataframe_guarded


class _RecordingContext:
    """Job context stand-in that records progress updates."""

    def __init__(self) -> None:
        self.updates: list[tuple[float | None, str | None]] = []

    def check_cancelled(self) -> None:
        """Never cancel."""

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        """Record one progress update."""
        self.updates.append((progress, message))


class _FakeAtlasProcess:
    """Minimal live process used by Atlas orchestration tests."""

    def __init__(self, pid: int) -> None:
        self.pid = pid

    def poll(self) -> None:
        """Report a live process through registration."""
        return None

    def wait(self, timeout=None):  # noqa: ANN001
        """Let the registry watcher finish without blocking the test suite."""
        return 0


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

    def test_full_scan_query_on_large_dataset_is_materialized_once(self) -> None:
        """Aggregate a large dataset in a job, page from the cached result, and refuse unscheduled scans."""
        context = _RecordingContext()
        sql = "SELECT label, count(*) AS n FROM data GROUP BY label ORDER BY label"
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "data.jsonl"
            path.write_text("".join(json.dumps({"label": label}) + "\n" for label in "abaca"), encoding="utf-8")
            with (
                patch.object(sql_module, "SYNC_OPERATION_MAX_BYTES", 1),
                patch.object(sql_module, "_materialize_worker", wraps=sql_module._materialize_worker) as worker,
            ):
                first = sql_module.execute_query_guarded(file_name="data.jsonl", path=path, sql=sql, limit=2, offset=0, context=context)
                second = sql_module.execute_query_guarded(file_name="data.jsonl", path=path, sql=sql, limit=2, offset=2, context=context)
                raw_columns, raw_values = sql_module.fetch_raw_query_row_guarded(path=path, sql=sql, offset=1)
                frame = sql_module.load_query_dataframe_guarded(path=path, sql=sql, sample=-1, context=context)
                with self.assertRaises(HTTPException) as raised:
                    sql_module.execute_query_guarded(file_name="data.jsonl", path=path, sql=sql, limit=2, offset=0)
                with patch.object(sql_module, "QUERY_RESULT_MAX_ROWS", 2):
                    path.write_text(path.read_text(encoding="utf-8") + '{"label": "d"}\n', encoding="utf-8")
                    truncated = sql_module.execute_query_guarded(file_name="data.jsonl", path=path, sql=sql, limit=5, offset=0, context=context)

        self.assertEqual([["a", 3], ["b", 1]], first["rows"])
        self.assertEqual([["c", 1]], second["rows"])
        self.assertEqual(3, first["total_rows"])
        self.assertTrue(first["cached_scan"])
        self.assertNotIn("warning", first)
        self.assertEqual(2, worker.call_count)
        self.assertEqual((["label", "n"], ["b", 1]), (raw_columns, raw_values))
        self.assertEqual(["a", "b", "c"], frame["label"].tolist())
        self.assertEqual(400, raised.exception.status_code)
        self.assertEqual([["a", 3], ["b", 1]], truncated["rows"])
        self.assertIn("Only the first 2 result rows", truncated["warning"])

    def test_stats_job_completes(self) -> None:
        """Verify that stats job completes."""
        started = start_stats_job(StatsJobRequest(file="example.jsonl", sample=50))
        payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertIn("columns", payload["result"])

    def test_translation_job_returns_nested_values_without_cache_fields(self) -> None:
        """Submit visible values through the shared cancellable job contract."""
        expected = {
            "model": "translator",
            "target_language": "ja",
            "items": [{"id": "row-1", "value": {"text": "こんにちは"}}],
        }
        prepared = object()
        with (
            patch("local_data_studio.server.api.jobs.TRANSLATION_SERVICE.prepare", return_value=prepared),
            patch("local_data_studio.server.api.jobs.TRANSLATION_SERVICE.translate_prepared", return_value=expected) as translate,
        ):
            started = start_translation_job(
                TranslationRequest(
                    model="translator",
                    target_language="ja",
                    column_name="content",
                    items=[{"id": "row-1", "value": {"text": "Hello"}}],
                )
            )
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual(expected, payload["result"])
        self.assertNotIn("cached", payload["result"])
        translate.assert_called_once()
        self.assertIs(prepared, translate.call_args.args[0])

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

    def test_query_eda_loader_accepts_unlimited_rows(self) -> None:
        """Verify that query EDA omits its outer limit when explicitly requested."""
        dataframe = load_query_dataframe_guarded(
            path=Path.cwd() / "data" / "example.jsonl",
            sql="SELECT object FROM data",
            sample=-1,
            context=None,
        )

        self.assertEqual(10, len(dataframe))

    def test_eda_job_completes_for_dataset_sample(self) -> None:
        """Verify that eda job completes for dataset sample."""
        started = start_eda_job(EdaRequest(file="example.jsonl", sample=100, force=True, mode="minimal"))
        payload = self._wait_for_job(started["id"], timeout=180.0)

        self.assertEqual("succeeded", payload["status"])
        self.assertIn("/cache/eda/", payload["result"]["url"])
        self.assertEqual(100, payload["result"]["sample"])

    def test_atlas_job_launches_for_selected_image_column(self) -> None:
        """Verify that atlas job launches for selected image column."""
        model_path = (Path.cwd() / "models" / "embedder" / "test-image-model").resolve()
        prepared_path = (Path.cwd() / "cache" / "atlas" / "datasets" / "prepared.parquet").resolve()

        def fake_launch(command, context, runtime):  # noqa: ANN001
            """Exercise fake launch behavior."""
            self.assertIn("--image", command)
            self.assertIn("image", command)
            self.assertIn("--disable-projection", command)
            self.assertIn(str(prepared_path), command)
            context.update(progress=1.0, message="ready")
            process = _FakeAtlasProcess(12345)
            return LaunchedAtlasProcess("http://127.0.0.1:5055/", "127.0.0.1", 5055, process)

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
                    row_count=3,
                ),
            ),
            patch("local_data_studio.server.atlas_components.service.start_embedding_atlas", side_effect=fake_launch),
        ):
            analyze_capabilities.return_value.fingerprint = "test-capability"
            started = start_atlas_job(AtlasRequest(file="example.jsonl", column="image", model="test-image-model"))
            payload = self._wait_for_job(started["id"])

        self.assertEqual("succeeded", payload["status"])
        self.assertEqual("dataset", payload["result"]["source"])
        self.assertEqual("image", payload["result"]["modality"])
        self.assertEqual("test-image-model", payload["result"]["model"])
        self.assertRegex(payload["result"]["url"], r"^/atlas/[A-Za-z0-9_-]+/$")
        self.assertIn("instance_id", payload["result"])
        self.assertTrue(payload["result"]["cache_hit"])
        self.assertEqual("umap", payload["result"]["projection_method"])
        self.assertEqual(3, payload["result"]["row_count"])
        self.assertEqual("full", payload["result"]["umap_projection_mode"])

    def test_atlas_job_samples_large_datasets_unless_a_sample_is_requested(self) -> None:
        """Embed a bounded sample of large datasets instead of loading every row."""
        model_path = (Path.cwd() / "models" / "embedder" / "test-text-model").resolve()
        prepared = AtlasPreparedDataset(path=Path("prepared.parquet"), x="x", y="y", neighbors=None, cache_hit=True, row_count=3)
        samples: list[int | None] = []

        def fake_prepare(**kwargs):  # noqa: ANN003, ANN202
            samples.append(kwargs["options"].sample)
            return prepared

        def fake_launch(command, context, runtime):  # noqa: ANN001, ARG001
            return LaunchedAtlasProcess("http://127.0.0.1:5055/", "127.0.0.1", 5055, _FakeAtlasProcess(12346))

        for large, requested, expected in ((True, None, ATLAS_LARGE_DATASET_SAMPLE), (True, 500, 500), (False, None, None)):
            with (
                self.subTest(large=large, requested=requested),
                patch("local_data_studio.server.atlas_components.service.resolve_embedder_model", return_value=model_path),
                patch("local_data_studio.server.atlas_components.service._resolve_backend", return_value="sentence-transformers"),
                patch("local_data_studio.server.atlas_components.service.analyze_model_capabilities"),
                patch("local_data_studio.server.atlas_components.service.is_large_dataset", return_value=large),
                patch("local_data_studio.server.atlas_components.service.prepare_atlas_dataset", side_effect=fake_prepare),
                patch("local_data_studio.server.atlas_components.service.start_embedding_atlas", side_effect=fake_launch),
            ):
                started = start_atlas_job(AtlasRequest(file="example.jsonl", column="short_description", model="test-text-model", sample=requested))
                payload = self._wait_for_job(started["id"])

                self.assertEqual("succeeded", payload["status"], payload.get("error"))
                self.assertEqual(expected, samples[-1])
                self.assertEqual(expected, payload["result"]["sample"])

    def test_atlas_query_job_passes_guarded_sql(self) -> None:
        """Verify that atlas query job passes guarded sql."""
        model_path = (Path.cwd() / "models" / "embedder" / "test-text-model").resolve()
        prepared_path = (Path.cwd() / "cache" / "atlas" / "datasets" / "prepared-query.parquet").resolve()
        captured_command: list[str] = []

        def fake_launch(command, context, runtime):  # noqa: ANN001
            """Exercise fake launch behavior."""
            captured_command.extend(command)
            context.update(progress=1.0, message="ready")
            process = _FakeAtlasProcess(12346)
            return LaunchedAtlasProcess("http://127.0.0.1:5056/", "127.0.0.1", 5056, process)

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
                    row_count=2,
                ),
            ),
            patch("local_data_studio.server.atlas_components.service.start_embedding_atlas", side_effect=fake_launch),
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

    def _wait_for_job(self, job_id: str, timeout: float = 30.0) -> dict:
        # Poll until the job finishes; the deadline only bounds a hung job, so
        # slow first imports on a loaded machine do not fail the test.
        deadline = monotonic() + timeout
        payload = get_job(job_id)
        while payload["status"] not in {"succeeded", "failed", "cancelled"} and monotonic() < deadline:
            sleep(0.05)
            payload = get_job(job_id)
        return payload

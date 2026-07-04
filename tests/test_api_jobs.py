import asyncio
from time import sleep
from unittest import TestCase
from unittest.mock import patch

from app import (
    CountJobRequest,
    EdaQueryRequest,
    IndexJobRequest,
    QueryRequest,
    SearchJobRequest,
    StatsJobRequest,
    get_job,
    preview,
    start_count_job,
    start_eda_query_job,
    start_index_job,
    start_query_job,
    start_search_job,
    start_stats_job,
)


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

        with patch("app.build_eda_report", side_effect=fake_build_report):
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

    def _wait_for_job(self, job_id: str) -> dict:
        payload = {}
        for _ in range(20):
            payload = asyncio.run(get_job(job_id))
            if payload["status"] in {"succeeded", "failed", "cancelled"}:
                return payload
            sleep(0.05)
        return payload

"""Tests for app contracts behavior."""

import inspect
from unittest import TestCase

from fastapi.testclient import TestClient

from local_data_studio.app import app
from local_data_studio.server.api.analysis import run_query
from local_data_studio.server.api.datasets import get_schema, preview, upload_files
from local_data_studio.server.api.jobs import get_job, start_atlas_job

EXPECTED_API_OPERATIONS = {
    ("/api/column_sample", "get"),
    ("/api/column_stats", "get"),
    ("/api/config", "get"),
    ("/api/count", "get"),
    ("/api/delete_column", "post"),
    ("/api/delete_row", "post"),
    ("/api/eda", "post"),
    ("/api/embedder_models", "get"),
    ("/api/files", "get"),
    ("/api/jobs/atlas", "post"),
    ("/api/jobs/atlas_query", "post"),
    ("/api/jobs/count", "post"),
    ("/api/jobs/eda", "post"),
    ("/api/jobs/eda_query", "post"),
    ("/api/jobs/index", "post"),
    ("/api/jobs/query", "post"),
    ("/api/jobs/search", "post"),
    ("/api/jobs/stats", "post"),
    ("/api/jobs/{job_id}", "get"),
    ("/api/jobs/{job_id}/cancel", "post"),
    ("/api/nl_query", "post"),
    ("/api/preview", "get"),
    ("/api/query", "post"),
    ("/api/raw", "get"),
    ("/api/raw_row", "post"),
    ("/api/schema", "get"),
    ("/api/search", "get"),
    ("/api/upload", "post"),
}


class ApplicationContractTests(TestCase):
    """Test application contract behavior."""

    def test_openapi_operations_remain_stable(self) -> None:
        """Verify that openapi operations remain stable."""
        schema = app.openapi()
        operations = {(path, method) for path, methods in schema["paths"].items() for method in methods if method in {"get", "post", "put", "patch", "delete"}}

        self.assertEqual(EXPECTED_API_OPERATIONS, operations)

    def test_static_mounts_follow_api_routes(self) -> None:
        """Verify that static mounts follow api routes."""
        paths = [getattr(route, "path", None) for route in app.routes]
        data_index = paths.index("/data")
        cache_index = paths.index("/cache")
        static_index = paths.index("")
        included_router_indices = [index for index, route in enumerate(app.routes) if type(route).__name__ == "_IncludedRouter"]

        self.assertTrue(included_router_indices)
        self.assertLess(max(included_router_indices), data_index)
        self.assertLess(data_index, cache_index)
        self.assertLess(cache_index, static_index)

    def test_packaged_static_response_disables_browser_cache(self) -> None:
        """Verify that packaged static response disables browser cache."""
        response = TestClient(app).get("/")

        self.assertEqual(200, response.status_code)
        self.assertEqual("no-store, max-age=0", response.headers["cache-control"])
        self.assertIn("Data Studio", response.text)

    def test_blocking_routes_run_in_fastapi_threadpool(self) -> None:
        """Verify that blocking routes run in fastapi threadpool."""
        for endpoint in (get_schema, preview, run_query, start_atlas_job, get_job):
            with self.subTest(endpoint=endpoint.__name__):
                self.assertFalse(inspect.iscoroutinefunction(endpoint))

    def test_streaming_upload_route_remains_async(self) -> None:
        """Verify that streaming upload route remains async."""
        self.assertTrue(inspect.iscoroutinefunction(upload_files))

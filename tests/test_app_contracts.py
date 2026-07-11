from unittest import TestCase

from fastapi.testclient import TestClient

from local_data_studio.app import app

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
    def test_openapi_operations_remain_stable(self) -> None:
        schema = app.openapi()
        operations = {(path, method) for path, methods in schema["paths"].items() for method in methods if method in {"get", "post", "put", "patch", "delete"}}

        self.assertEqual(EXPECTED_API_OPERATIONS, operations)

    def test_static_mounts_follow_api_routes(self) -> None:
        paths = [getattr(route, "path", None) for route in app.routes]
        data_index = paths.index("/data")
        cache_index = paths.index("/cache")
        static_index = paths.index("")
        last_api_index = max(index for index, path in enumerate(paths) if isinstance(path, str) and path.startswith("/api/"))

        self.assertLess(last_api_index, data_index)
        self.assertLess(data_index, cache_index)
        self.assertLess(cache_index, static_index)

    def test_packaged_static_response_disables_browser_cache(self) -> None:
        response = TestClient(app).get("/")

        self.assertEqual(200, response.status_code)
        self.assertEqual("no-store, max-age=0", response.headers["cache-control"])
        self.assertIn("Data Studio", response.text)

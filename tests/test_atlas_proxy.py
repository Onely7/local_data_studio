"""Tests for application-scoped Atlas registration and HTTP proxying."""

from __future__ import annotations

import gzip
from threading import Event
from unittest import TestCase

import httpx
from fastapi.testclient import TestClient

from local_data_studio.app import create_app
from local_data_studio.server.atlas_components.runtime import AtlasCapacityError, AtlasRuntime


class AsyncBytes(httpx.AsyncByteStream):
    """One-chunk raw HTTPX stream for proxy transport tests."""

    def __init__(self, content: bytes = b"") -> None:
        """Store the only chunk returned by asynchronous iteration."""
        self.content = content

    async def __aiter__(self):  # noqa: ANN201
        """Yield the configured raw chunk once."""
        yield self.content


class FakeProcess:
    """Controllable Popen-compatible process for registry lifecycle tests."""

    def __init__(self, pid: int) -> None:
        """Create a process that remains live until explicitly released."""
        self.pid = pid
        self.exited = Event()
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        """Return an exit code only after termination."""
        return 0 if self.exited.is_set() else None

    def wait(self, timeout: float | None = None) -> int:
        """Wait for the test-controlled process exit."""
        if not self.exited.wait(timeout):
            raise TimeoutError
        return 0

    def terminate(self) -> None:
        """Record graceful termination and release process waiters."""
        self.terminated = True
        self.exited.set()

    def kill(self) -> None:
        """Record forced termination and release process waiters."""
        self.killed = True
        self.exited.set()


def register_process(runtime: AtlasRuntime, process: FakeProcess, *, port: int = 5055):  # noqa: ANN201
    """Register a fake process through the same reservation contract as jobs."""
    token = runtime.reserve_slot()
    return runtime.register(token, host="127.0.0.1", port=port, process=process)


class AtlasRuntimeTests(TestCase):
    """Test bounded application ownership of Atlas child processes."""

    def test_capacity_counts_pending_and_live_instances(self) -> None:
        """Reject work once pending and registered slots reach the limit."""
        runtime = AtlasRuntime(max_instances=2)
        pending = runtime.reserve_slot()
        process = FakeProcess(100)
        register_process(runtime, process)

        with self.assertRaises(AtlasCapacityError):
            runtime.reserve_slot()

        runtime.release_slot(pending)
        replacement = runtime.reserve_slot()
        runtime.release_slot(replacement)
        runtime.terminate_all()

    def test_registry_uses_process_identity_and_removes_exited_children(self) -> None:
        """Remove a mapping when its exact Popen object exits."""
        runtime = AtlasRuntime(max_instances=1)
        process = FakeProcess(101)
        instance = register_process(runtime, process)

        self.assertIs(instance, runtime.resolve(instance.instance_id))
        process.exited.set()
        process.wait(timeout=1)
        for _ in range(100):
            if runtime.resolve(instance.instance_id) is None:
                break
            process.exited.wait(0.001)
        self.assertIsNone(runtime.resolve(instance.instance_id))


class AtlasProxyTests(TestCase):
    """Test byte-preserving same-origin forwarding to registered instances."""

    def test_proxy_preserves_raw_path_query_and_filters_connection_headers(self) -> None:
        """Forward encoded paths without leaking hop-by-hop or private headers."""
        captured: dict[str, object] = {}

        async def upstream(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path
            captured["headers"] = request.headers
            return httpx.Response(
                200,
                stream=AsyncBytes(b"ok"),
                headers={"Connection": "X-Temporary", "X-Temporary": "drop", "ETag": '"keep"'},
            )

        runtime = AtlasRuntime(max_instances=2, proxy_transport=httpx.MockTransport(upstream))
        process = FakeProcess(200)
        instance = register_process(runtime, process)
        application = create_app(atlas_runtime=runtime)

        with TestClient(application) as client:
            response = client.get(
                f"/atlas/{instance.instance_id}/a%2Fb/%25value/%E3%81%82?a=1&a=2&x=%2F",
                headers={"Connection": "X-Request-Temporary", "X-Request-Temporary": "drop", "Authorization": "secret"},
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(b"/a%2Fb/%25value/%E3%81%82?a=1&a=2&x=%2F", captured["raw_path"])
        forwarded = captured["headers"]
        self.assertNotIn("authorization", forwarded)
        self.assertNotIn("x-request-temporary", forwarded)
        self.assertNotIn("x-request-temporary", forwarded.get("connection", "").lower())
        self.assertEqual('"keep"', response.headers["etag"])
        self.assertNotIn("x-temporary", response.headers)
        self.assertEqual("no-referrer", response.headers["referrer-policy"])
        self.assertTrue(process.terminated)

    def test_proxy_preserves_range_status_and_content_headers(self) -> None:
        """Keep partial-content metadata while streaming the selected bytes."""

        async def upstream(request: httpx.Request) -> httpx.Response:
            self.assertEqual("bytes=1-3", request.headers["range"])
            return httpx.Response(
                206,
                stream=AsyncBytes(b"bcd"),
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": "bytes 1-3/5",
                    "Content-Length": "3",
                    "Content-Type": "application/octet-stream",
                },
            )

        runtime = AtlasRuntime(max_instances=1, proxy_transport=httpx.MockTransport(upstream))
        instance = register_process(runtime, FakeProcess(201))
        with TestClient(create_app(atlas_runtime=runtime)) as client:
            response = client.get(f"/atlas/{instance.instance_id}/data/dataset.parquet", headers={"Range": "bytes=1-3"})

        self.assertEqual(206, response.status_code)
        self.assertEqual(b"bcd", response.content)
        self.assertEqual("bytes 1-3/5", response.headers["content-range"])
        self.assertEqual("3", response.headers["content-length"])

    def test_proxy_rewrites_internal_redirects_and_keeps_external_redirects(self) -> None:
        """Map only redirects that resolve back to the registered Atlas origin."""
        locations = iter(["/data/file?q=1#part", "https://example.com/external"])

        async def upstream(_: httpx.Request) -> httpx.Response:
            return httpx.Response(307, stream=AsyncBytes(), headers={"Location": next(locations)})

        runtime = AtlasRuntime(max_instances=1, proxy_transport=httpx.MockTransport(upstream))
        instance = register_process(runtime, FakeProcess(202))
        with TestClient(create_app(atlas_runtime=runtime), follow_redirects=False) as client:
            internal = client.get(f"/atlas/{instance.instance_id}/redirect")
            external = client.get(f"/atlas/{instance.instance_id}/redirect")

        self.assertEqual(f"/atlas/{instance.instance_id}/data/file?q=1#part", internal.headers["location"])
        self.assertEqual("https://example.com/external", external.headers["location"])

    def test_canonical_redirect_stop_api_and_unknown_instance(self) -> None:
        """Require a trailing slash and expose explicit process cleanup."""
        runtime = AtlasRuntime(max_instances=1, proxy_transport=httpx.MockTransport(lambda _: httpx.Response(200)))
        process = FakeProcess(203)
        instance = register_process(runtime, process)
        with TestClient(create_app(atlas_runtime=runtime), follow_redirects=False) as client:
            redirect = client.get(f"/atlas/{instance.instance_id}")
            stopped = client.delete(f"/api/atlas/instances/{instance.instance_id}")
            missing = client.get(f"/atlas/{instance.instance_id}/")

        self.assertEqual(307, redirect.status_code)
        self.assertEqual(f"/atlas/{instance.instance_id}/", redirect.headers["location"])
        self.assertEqual({"instance_id": instance.instance_id, "stopped": True}, stopped.json())
        self.assertEqual(404, missing.status_code)
        self.assertTrue(process.terminated)

    def test_raw_gzip_stream_keeps_encoded_bytes_and_headers(self) -> None:
        """Use HTTPX raw iteration when encoded response headers are preserved."""
        compressed = gzip.compress(b"compressed payload")

        async def upstream(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                stream=AsyncBytes(compressed),
                headers={"Content-Encoding": "gzip", "Content-Length": str(len(compressed))},
            )

        runtime = AtlasRuntime(max_instances=1, proxy_transport=httpx.MockTransport(upstream))
        instance = register_process(runtime, FakeProcess(204))
        with TestClient(create_app(atlas_runtime=runtime)) as client:
            with client.stream("GET", f"/atlas/{instance.instance_id}/encoded") as response:
                raw = b"".join(response.iter_raw())

        self.assertEqual(compressed, raw)
        self.assertEqual("gzip", response.headers["content-encoding"])
        self.assertEqual(str(len(compressed)), response.headers["content-length"])

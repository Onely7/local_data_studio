"""Application-scoped lifecycle management for Embedding Atlas servers."""

from __future__ import annotations

import secrets
import subprocess
import threading
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

ATLAS_PROXY_TIMEOUT = httpx.Timeout(connect=2.0, pool=5.0, write=300.0, read=300.0)


class AtlasRuntimeError(RuntimeError):
    """Base error for Atlas runtime state transitions."""


class AtlasRuntimeClosingError(AtlasRuntimeError):
    """Raised when work is requested after application shutdown starts."""


class AtlasCapacityError(AtlasRuntimeError):
    """Raised when all configured Atlas process slots are reserved."""


@dataclass(frozen=True, slots=True)
class AtlasInstance:
    """A registered Atlas child process reachable only through its opaque ID."""

    instance_id: str
    host: str
    port: int
    process: subprocess.Popen[str]
    created_at: str

    @property
    def pid(self) -> int:
        """Return the child process ID for diagnostics and compatibility."""
        return self.process.pid

    @property
    def proxy_path(self) -> str:
        """Return the same-origin browser path for this instance."""
        return f"/atlas/{self.instance_id}/"


class AtlasRuntime:
    """Own Atlas process slots, live instances, and the proxy HTTP client."""

    def __init__(self, *, max_instances: int, proxy_transport: httpx.AsyncBaseTransport | None = None) -> None:
        """Create an isolated runtime with a fixed positive process limit."""
        if max_instances < 1:
            raise ValueError("max_instances must be greater than or equal to 1")
        self._max_instances = max_instances
        self._lock = threading.RLock()
        self._closing = False
        self._reservations: set[str] = set()
        self._instances: dict[str, AtlasInstance] = {}
        self._proxy_client: httpx.AsyncClient | None = None
        self._proxy_transport = proxy_transport

    @property
    def closing(self) -> bool:
        """Return whether this runtime has started permanent shutdown."""
        with self._lock:
            return self._closing

    @property
    def max_instances(self) -> int:
        """Return the configured pending-plus-live process limit."""
        return self._max_instances

    def check_open(self) -> None:
        """Reject work once shutdown has started."""
        if self.closing:
            raise AtlasRuntimeClosingError("Atlas runtime is shutting down")

    def reserve_slot(self) -> str:
        """Reserve capacity before expensive preparation or process startup."""
        with self._lock:
            if self._closing:
                raise AtlasRuntimeClosingError("Atlas runtime is shutting down")
            if len(self._reservations) + len(self._instances) >= self._max_instances:
                raise AtlasCapacityError(f"Atlas instance limit ({self._max_instances}) has been reached")
            token = secrets.token_urlsafe(24)
            self._reservations.add(token)
            return token

    def release_slot(self, token: str) -> None:
        """Release a pending reservation after failure or cancellation."""
        with self._lock:
            self._reservations.discard(token)

    def register(self, token: str, *, host: str, port: int, process: subprocess.Popen[str]) -> AtlasInstance:
        """Convert a valid reservation into a live process registration."""
        if host != "127.0.0.1" or not 1 <= port <= 65535:
            raise AtlasRuntimeError("Atlas instances must use a valid IPv4 loopback endpoint")
        with self._lock:
            if self._closing:
                raise AtlasRuntimeClosingError("Atlas runtime is shutting down")
            if token not in self._reservations:
                raise AtlasRuntimeError("Atlas process reservation is no longer valid")
            if process.poll() is not None:
                raise AtlasRuntimeError("Atlas process exited before registration")
            self._reservations.remove(token)
            instance_id = secrets.token_urlsafe(32)
            instance = AtlasInstance(
                instance_id=instance_id,
                host=host,
                port=port,
                process=process,
                created_at=datetime.now(UTC).isoformat(),
            )
            self._instances[instance_id] = instance
        threading.Thread(target=self._watch_process, args=(instance,), daemon=True).start()
        return instance

    def resolve(self, instance_id: str) -> AtlasInstance | None:
        """Return a live registered instance without exposing arbitrary targets."""
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None:
                return None
            if instance.process.poll() is None:
                return instance
            self._remove_if_same(instance)
            return None

    def stop(self, instance_id: str) -> bool:
        """Stop and unregister one instance, returning whether it existed."""
        with self._lock:
            instance = self._instances.get(instance_id)
        if instance is None:
            return False
        self._terminate_process(instance.process)
        self._remove_if_same(instance)
        return True

    def begin_shutdown(self) -> None:
        """Close the admission gate before jobs and processes are cancelled."""
        with self._lock:
            self._closing = True

    def terminate_all(self) -> None:
        """Terminate every registered child and clear pending reservations."""
        self.begin_shutdown()
        with self._lock:
            instances = list(self._instances.values())
            self._reservations.clear()
        for instance in instances:
            self._terminate_process(instance.process)
            self._remove_if_same(instance)

    def terminate_unregistered(self, process: subprocess.Popen[str]) -> None:
        """Terminate a child that failed before registry ownership transferred."""
        self._terminate_process(process)

    async def start_proxy_client(self) -> None:
        """Create the event-loop-owned HTTP client used only by proxy requests."""
        if self._proxy_client is None:
            self._proxy_client = httpx.AsyncClient(
                timeout=ATLAS_PROXY_TIMEOUT,
                trust_env=False,
                follow_redirects=False,
                transport=self._proxy_transport,
            )

    async def close_proxy_client(self) -> None:
        """Close and discard the application proxy client."""
        client = self._proxy_client
        self._proxy_client = None
        if client is not None:
            await client.aclose()

    @property
    def proxy_client(self) -> httpx.AsyncClient:
        """Return the started proxy client for request handling."""
        if self._proxy_client is None:
            raise AtlasRuntimeError("Atlas proxy client is not running")
        return self._proxy_client

    def _watch_process(self, instance: AtlasInstance) -> None:
        try:
            instance.process.wait()
        finally:
            self._remove_if_same(instance)

    def _remove_if_same(self, instance: AtlasInstance) -> None:
        with self._lock:
            current = self._instances.get(instance.instance_id)
            if current is not None and current.process is instance.process:
                self._instances.pop(instance.instance_id, None)

    @staticmethod
    def _terminate_process(process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

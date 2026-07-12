"""In-process background job registry for long-running local dataset work."""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable

JobWork = Callable[["JobContext"], dict[str, Any]]


class JobCancelledError(RuntimeError):
    """Raised when a running job is cancelled by the user."""


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class JobRecord:
    """Mutable job state guarded by JobStore's lock."""

    job_id: str
    kind: str
    status: str = "queued"
    progress: float | None = None
    message: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    cancel_requested: bool = False
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)

    def to_response(self) -> dict[str, Any]:
        """Return a new response mapping for the current lock-protected state.

        Nested ``result`` values are shared and must be treated as read-only.
        """
        return {
            "id": self.job_id,
            "kind": self.kind,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "result": self.result,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class JobContext:
    """Cooperative progress and cancellation handle passed to worker code."""

    def __init__(self, store: JobStore, job_id: str) -> None:
        """Bind worker callbacks to one job owned by ``store``."""
        self._store = store
        self._job_id = job_id

    def check_cancelled(self) -> None:
        """Raise ``JobCancelledError`` when cooperative cancellation was requested."""
        if self._store.is_cancel_requested(self._job_id):
            raise JobCancelledError("job was cancelled")

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        """Publish progress clamped to [0, 1] and an optional user-facing message."""
        self._store.update_progress(self._job_id, progress=progress, message=message)


class JobStore:
    """Thread-safe registry for short-lived background jobs."""

    def __init__(self, *, max_workers: int = 4) -> None:
        """Create a registry and a bounded worker pool owned by this store."""
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="local-data-studio-job")
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs: dict[str, JobRecord] = {}
        self._accepting = True

    def submit(self, kind: str, work: JobWork) -> JobRecord:
        """Register work and return its live mutable record immediately.

        Worker exceptions are captured in the record rather than propagated here.
        """
        job_id = uuid.uuid4().hex
        record = JobRecord(job_id=job_id, kind=kind)
        with self._lock:
            if not self._accepting:
                raise RuntimeError("job store is shutting down")
            self._jobs[job_id] = record
        self._executor.submit(self._run, job_id, work)
        return record

    def _run(self, job_id: str, work: JobWork) -> None:
        self._set_running(job_id)
        context = JobContext(self, job_id)
        try:
            context.check_cancelled()
            result = work(context)
            context.check_cancelled()
        except JobCancelledError as exc:
            self._set_cancelled(job_id, str(exc))
        except Exception as exc:
            self._set_failed(job_id, str(exc))
        else:
            self._set_succeeded(job_id, result)

    def get(self, job_id: str) -> JobRecord | None:
        """Return the store-owned live record, or ``None`` when unknown."""
        with self._lock:
            return self._jobs.get(job_id)

    def cancel(self, job_id: str) -> JobRecord | None:
        """Request cooperative cancellation and return the live record if present."""
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                return None
            if record.status in {"succeeded", "failed", "cancelled"}:
                return record
            record.cancel_requested = True
            record.message = "Cancellation requested"
            record.updated_at = _utc_now_iso()
            return record

    def begin_shutdown(self) -> None:
        """Stop accepting work and request cancellation for every active job."""
        with self._condition:
            self._accepting = False
            for record in self._jobs.values():
                if record.status not in {"succeeded", "failed", "cancelled"}:
                    record.cancel_requested = True
                    if record.status == "queued":
                        record.status = "cancelled"
                        record.message = "job was cancelled"
                    else:
                        record.message = "Cancellation requested"
                    record.updated_at = _utc_now_iso()
            self._condition.notify_all()

    def wait_for_idle(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for all running work to finish."""
        deadline = time.monotonic() + max(0.0, timeout)
        with self._condition:
            while any(record.status in {"queued", "running"} for record in self._jobs.values()):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return True

    def shutdown(self, *, wait_timeout: float = 10.0) -> None:
        """Cancel active jobs and release executor resources after a bounded wait."""
        self.begin_shutdown()
        self.wait_for_idle(wait_timeout)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def is_cancel_requested(self, job_id: str) -> bool:
        """Return whether a job is missing or has a cancellation request."""
        with self._lock:
            record = self._jobs.get(job_id)
            return record is None or record.cancel_requested

    def update_progress(self, job_id: str, *, progress: float | None = None, message: str | None = None) -> None:
        """Update a queued or running job while holding the registry lock."""
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None or record.status not in {"queued", "running"}:
                return
            if record.cancel_requested:
                return
            if progress is not None:
                record.progress = max(0.0, min(1.0, progress))
            if message is not None:
                record.message = message
            record.updated_at = _utc_now_iso()

    def _set_running(self, job_id: str) -> None:
        with self._condition:
            record = self._jobs[job_id]
            if record.cancel_requested:
                record.status = "cancelled"
                record.message = "job was cancelled"
                record.updated_at = _utc_now_iso()
                self._condition.notify_all()
                return
            record.status = "running"
            record.progress = 0.0
            record.updated_at = _utc_now_iso()

    def _set_succeeded(self, job_id: str, result: dict[str, Any]) -> None:
        with self._condition:
            record = self._jobs[job_id]
            record.status = "succeeded"
            record.progress = 1.0
            record.result = result
            record.updated_at = _utc_now_iso()
            self._condition.notify_all()

    def _set_failed(self, job_id: str, error: str) -> None:
        with self._condition:
            record = self._jobs[job_id]
            record.status = "failed"
            record.error = error
            record.updated_at = _utc_now_iso()
            self._condition.notify_all()

    def _set_cancelled(self, job_id: str, message: str) -> None:
        with self._condition:
            record = self._jobs[job_id]
            record.status = "cancelled"
            record.message = message
            record.updated_at = _utc_now_iso()
            self._condition.notify_all()


JOB_STORE = JobStore()

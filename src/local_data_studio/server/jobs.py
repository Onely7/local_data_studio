"""In-process background job registry for long-running local dataset work."""

from __future__ import annotations

import threading
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
        self._store = store
        self._job_id = job_id

    def check_cancelled(self) -> None:
        if self._store.is_cancel_requested(self._job_id):
            raise JobCancelledError("job was cancelled")

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        self._store.update_progress(self._job_id, progress=progress, message=message)


class JobStore:
    """Thread-safe registry for short-lived background jobs."""

    def __init__(self, *, max_workers: int = 4) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="local-data-studio-job")
        self._lock = threading.RLock()
        self._jobs: dict[str, JobRecord] = {}

    def submit(self, kind: str, work: JobWork) -> JobRecord:
        job_id = uuid.uuid4().hex
        record = JobRecord(job_id=job_id, kind=kind)
        with self._lock:
            self._jobs[job_id] = record
        self._executor.submit(self._run, job_id, work)
        return record

    def _run(self, job_id: str, work: JobWork) -> None:
        self._set_running(job_id)
        context = JobContext(self, job_id)
        try:
            context.check_cancelled()
            result = work(context)
        except JobCancelledError as exc:
            self._set_cancelled(job_id, str(exc))
        except Exception as exc:
            self._set_failed(job_id, str(exc))
        else:
            self._set_succeeded(job_id, result)

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def cancel(self, job_id: str) -> JobRecord | None:
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

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            record = self._jobs.get(job_id)
            return record is None or record.cancel_requested

    def update_progress(self, job_id: str, *, progress: float | None = None, message: str | None = None) -> None:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None or record.status not in {"queued", "running"}:
                return
            if progress is not None:
                record.progress = max(0.0, min(1.0, progress))
            if message is not None:
                record.message = message
            record.updated_at = _utc_now_iso()

    def _set_running(self, job_id: str) -> None:
        with self._lock:
            record = self._jobs[job_id]
            record.status = "running"
            record.progress = 0.0
            record.updated_at = _utc_now_iso()

    def _set_succeeded(self, job_id: str, result: dict[str, Any]) -> None:
        with self._lock:
            record = self._jobs[job_id]
            record.status = "succeeded"
            record.progress = 1.0
            record.result = result
            record.updated_at = _utc_now_iso()

    def _set_failed(self, job_id: str, error: str) -> None:
        with self._lock:
            record = self._jobs[job_id]
            record.status = "failed"
            record.error = error
            record.updated_at = _utc_now_iso()

    def _set_cancelled(self, job_id: str, message: str) -> None:
        with self._lock:
            record = self._jobs[job_id]
            record.status = "cancelled"
            record.message = message
            record.updated_at = _utc_now_iso()


JOB_STORE = JobStore()

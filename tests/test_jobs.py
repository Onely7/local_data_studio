"""Tests for shared background job lifecycle behavior."""

from threading import Event
from time import sleep
from unittest import TestCase

from local_data_studio.server.jobs import JobStore


class JobStoreTests(TestCase):
    """Test cancellation and progress state owned by the job store."""

    def test_cancel_message_is_not_overwritten_by_late_worker_progress(self) -> None:
        """Keep cancellation visible until cooperative work reaches a boundary."""
        store = JobStore(max_workers=1)
        worker_started = Event()
        allow_worker_update = Event()

        def work(context):  # noqa: ANN001
            worker_started.set()
            allow_worker_update.wait(timeout=2)
            context.update(progress=0.8, message="late progress")
            return {}

        record = store.submit("atlas", work)
        self.assertTrue(worker_started.wait(timeout=2))
        store.cancel(record.job_id)
        allow_worker_update.set()

        for _ in range(100):
            snapshot = store.get(record.job_id)
            if snapshot is not None and snapshot.status == "cancelled":
                break
            sleep(0.01)

        self.assertIsNotNone(snapshot)
        self.assertEqual("cancelled", snapshot.status)
        self.assertEqual("job was cancelled", snapshot.message)
        self.assertNotEqual("late progress", snapshot.message)
        store.shutdown(wait_timeout=1)

    def test_shutdown_rejects_new_jobs_and_cancels_queued_work(self) -> None:
        """Close job admission before the application releases runtime resources."""
        store = JobStore(max_workers=1)
        worker_started = Event()
        release_worker = Event()

        def blocking_work(context):  # noqa: ANN001
            worker_started.set()
            release_worker.wait(timeout=2)
            context.check_cancelled()
            return {}

        running = store.submit("atlas", blocking_work)
        self.assertTrue(worker_started.wait(timeout=2))
        queued = store.submit("atlas", lambda context: {})  # noqa: ARG005
        store.begin_shutdown()
        release_worker.set()
        self.assertTrue(store.wait_for_idle(2))

        self.assertEqual("cancelled", store.get(running.job_id).status)
        self.assertEqual("cancelled", store.get(queued.job_id).status)
        with self.assertRaisesRegex(RuntimeError, "shutting down"):
            store.submit("count", lambda context: {})  # noqa: ARG005
        store.shutdown(wait_timeout=0)

    def test_terminal_history_is_bounded_without_evicting_active_jobs(self) -> None:
        """Retain only the newest terminal records while active work remains."""
        store = JobStore(max_workers=2, max_terminal_jobs=2)
        release_active = Event()
        active_started = Event()

        def active_work(context):  # noqa: ANN001
            active_started.set()
            release_active.wait(timeout=2)
            return {"active": True}

        active = store.submit("active", active_work)
        self.assertTrue(active_started.wait(timeout=2))
        completed = [store.submit("quick", lambda context, index=index: {"index": index}) for index in range(3)]  # noqa: ARG005

        for record in completed:
            for _ in range(200):
                snapshot = store.get(record.job_id)
                if snapshot is None or snapshot.status == "succeeded":
                    break
                sleep(0.01)

        self.assertIsNotNone(store.get(active.job_id))
        self.assertIsNone(store.get(completed[0].job_id))
        self.assertIsNotNone(store.get(completed[1].job_id))
        self.assertIsNotNone(store.get(completed[2].job_id))
        release_active.set()
        store.shutdown(wait_timeout=2)

    def test_get_returns_a_detached_result_mapping(self) -> None:
        """Prevent API callers from mutating the live job result mapping."""
        store = JobStore(max_workers=1)
        record = store.submit("quick", lambda context: {"value": "original"})  # noqa: ARG005
        self.assertTrue(store.wait_for_idle(2))

        first = store.get(record.job_id)
        self.assertIsNotNone(first)
        first.result["value"] = "changed"

        second = store.get(record.job_id)
        self.assertEqual("original", second.result["value"])
        store.shutdown(wait_timeout=0)

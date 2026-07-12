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

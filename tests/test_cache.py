"""Tests for deterministic and atomic cache maintenance."""

from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from local_data_studio.server.api.services import write_cached_result
from local_data_studio.server.cache import prune_cache_dir


class CacheTests(TestCase):
    """Verify cache replacement and capacity behavior."""

    def test_write_cached_result_atomically_replaces_existing_json(self) -> None:
        """Leave one complete cache file and no temporary artifact."""
        with TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "nested" / "result.json"
            cache_path.parent.mkdir()
            cache_path.write_text('{"old": true}', encoding="utf-8")

            write_cached_result(cache_path, {"new": [1, 2, 3]})

            self.assertEqual({"new": [1, 2, 3]}, json.loads(cache_path.read_text(encoding="utf-8")))
            self.assertEqual([cache_path], list(cache_path.parent.iterdir()))

    def test_prune_cache_dir_uses_mtime_then_path_for_eviction(self) -> None:
        """Resolve equal-mtime eviction deterministically by path name."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "a.cache"
            second = root / "b.cache"
            first.write_bytes(b"a" * 5)
            second.write_bytes(b"b" * 5)
            first.touch()
            second.touch()
            timestamp = 1_700_000_000
            os.utime(first, (timestamp, timestamp))
            os.utime(second, (timestamp, timestamp))

            remaining = prune_cache_dir(root, 5)

            self.assertEqual(5, remaining)
            self.assertFalse(first.exists())
            self.assertTrue(second.exists())

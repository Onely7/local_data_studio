"""Tests for atlas behavior."""

import os
import sys
from base64 import b64decode
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from typing import Any
from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
from embedding_atlas import projection
from fastapi import HTTPException

from local_data_studio.server import atlas_cache_patch
from local_data_studio.server.atlas import (
    ATLAS_EMBED_INPUT_COLUMN,
    ATLAS_PROJECTION_NEIGHBORS,
    ATLAS_PROJECTION_X,
    ATLAS_PROJECTION_Y,
    ATLAS_TRUNCATION_SUFFIX,
    AtlasOptions,
    _embedding_atlas_env,
    _normalize_atlas_url,
    _run_full_projection,
    atlas_dataset_cache_path,
    build_atlas_command,
    discover_embedder_models,
    launch_embedding_atlas,
    prepare_atlas_dataset,
    project_atlas_frame,
    reserve_atlas_start_port,
    resolve_embedder_model,
)
from local_data_studio.server.atlas_cache import prune_cache_dir
from local_data_studio.server.atlas_components.contracts import AtlasProjectionCoordinates
from local_data_studio.server.config import BASE_DIR, PACKAGE_DIR
from local_data_studio.server.jobs import JobCancelledError

VALID_PNG_BYTES = b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=")


class AtlasModelDiscoveryTests(TestCase):
    """Test atlas model discovery behavior."""

    def test_discovers_model_roots_without_nested_internal_dirs(self) -> None:
        """Verify that discovers model roots without nested internal dirs."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            model = root / "Qwen" / "Qwen3-Embedding-0.6B"
            nested = model / "0_Transformer"
            empty = root / "empty-model"
            nested.mkdir(parents=True)
            empty.mkdir()
            (model / "config.json").write_text("{}", encoding="utf-8")
            (nested / "config.json").write_text("{}", encoding="utf-8")

            with patch("local_data_studio.server.atlas_components.service.EMBEDDER_MODELS_DIR", root):
                models = discover_embedder_models()

        self.assertEqual(1, len(models))
        self.assertEqual("Qwen/Qwen3-Embedding-0.6B", models[0]["name"])
        self.assertEqual(str(model), models[0]["path"])
        self.assertIn("backends", models[0])
        self.assertIn("capability_fingerprint", models[0])

    def test_resolve_rejects_paths_outside_model_root(self) -> None:
        """Verify that resolve rejects paths outside model root."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "models"
            root.mkdir()

            with patch("local_data_studio.server.atlas_components.service.EMBEDDER_MODELS_DIR", root):
                with self.assertRaises(HTTPException):
                    resolve_embedder_model("../outside")

    def test_normalizes_atlas_url_from_cli_output(self) -> None:
        """Verify that normalizes atlas url from cli output."""
        self.assertEqual("http://localhost:5055", _normalize_atlas_url("\x1b[32mhttp://localhost:5055\x1b[0m."))
        self.assertEqual("http://127.0.0.1:5055/", _normalize_atlas_url("http://0.0.0.0:5055/"))

    def test_atlas_command_loads_projection_cache_patch(self) -> None:
        """Verify that atlas command loads projection cache patch."""
        with patch(
            "local_data_studio.server.atlas_components.process.effective_embedder_for_modality",
            return_value="transformers",
        ):
            command = build_atlas_command(
                path=Path("data/example.jsonl"),
                column="image",
                modality="image",
                sql=None,
                model_path=Path("models/embedder/example"),
                options=AtlasOptions(
                    sample=None,
                    host="127.0.0.1",
                    port=5055,
                    batch_size=None,
                    text_embedder=None,
                    image_embedder=None,
                    trust_remote_code=False,
                ),
            )

        self.assertIn("--with", command)
        self.assertIn("local_data_studio.server.atlas_cache_patch", command)
        self.assertIn("--no-auto-port", command)
        self.assertNotIn("--auto-port", command)
        self.assertTrue(Path(command[3]).is_absolute())

    def test_launch_embedding_atlas_uses_posix_spawn_compatible_popen(self) -> None:
        """Verify that launch embedding atlas uses posix spawn compatible popen."""

        class DummyContext:
            """Test dummy context behavior."""

            def check_cancelled(self) -> None:
                """Exercise check cancelled behavior."""
                return None

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        class FakeStdout:
            """Test fake stdout behavior."""

            def __init__(self) -> None:
                self._lines = iter(["Embedding Atlas\n", "URL: http://127.0.0.1:5055\n"])

            def readline(self) -> str:
                """Exercise readline behavior."""
                return next(self._lines, "")

            def close(self) -> None:
                """Exercise close behavior."""
                return None

        class FakeProcess:
            """Test fake process behavior."""

            stdout = FakeStdout()
            pid = 12345
            returncode = None

            def poll(self) -> None:
                """Exercise poll behavior."""
                return None

        with (
            patch("local_data_studio.server.atlas_components.process.subprocess.Popen", return_value=FakeProcess()) as popen,
            patch("local_data_studio.server.atlas_components.process._atlas_http_is_ready", return_value=True) as is_ready,
        ):
            url, pid = launch_embedding_atlas(["/usr/bin/python3", "-m", "embedding_atlas.cli"], DummyContext())

        self.assertEqual("http://127.0.0.1:5055", url)
        self.assertEqual(12345, pid)
        kwargs = popen.call_args.kwargs
        self.assertFalse(kwargs["close_fds"])
        self.assertNotIn("cwd", kwargs)
        self.assertEqual("http://127.0.0.1:5055", is_ready.call_args.args[1])

    def test_launch_embedding_atlas_terminates_child_when_cancelled(self) -> None:
        """Stop the Atlas child promptly after cooperative cancellation."""

        class CancelledContext:
            """Raise cancellation at the first child supervision boundary."""

            def check_cancelled(self) -> None:
                """Simulate an already-cancelled job."""
                raise JobCancelledError("job was cancelled")

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Ignore progress from the child supervisor."""
                return None

        class FakeProcess:
            """Record whether child termination was requested."""

            stdout = None
            returncode = None
            pid = 12345

            def __init__(self) -> None:
                """Create a running fake process."""
                self.terminated = False

            def poll(self) -> None:
                """Report that the process is still running."""
                return None

            def terminate(self) -> None:
                """Record graceful termination."""
                self.terminated = True

            def wait(self, timeout=None):  # noqa: ANN001
                """Report successful child shutdown."""
                return 0

        process = FakeProcess()
        with (
            patch("local_data_studio.server.atlas_components.process.spawn_embedding_atlas", return_value=process) as spawn,
            self.assertRaises(JobCancelledError),
        ):
            launch_embedding_atlas(["/usr/bin/python3", "-m", "embedding_atlas.cli"], CancelledContext())

        spawn.assert_not_called()
        self.assertFalse(process.terminated)

    def test_embedding_atlas_env_can_import_local_server_package(self) -> None:
        """Verify that embedding atlas env can import local server package."""
        env = _embedding_atlas_env()

        python_paths = env["PYTHONPATH"].split(os.pathsep)
        self.assertIn(str(PACKAGE_DIR.parent), python_paths)
        self.assertIn(str(BASE_DIR), python_paths)
        self.assertEqual(str(BASE_DIR / "cache" / "atlas" / "projection"), env["LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR"])
        self.assertEqual(str(BASE_DIR / "cache" / "atlas"), env["LOCAL_DATA_STUDIO_ATLAS_CACHE_PRUNE_DIR"])
        self.assertEqual("false", env["TOKENIZERS_PARALLELISM"])
        self.assertEqual("1", env["VECLIB_MAXIMUM_THREADS"])

    def test_projection_cache_prunes_oldest_files_first(self) -> None:
        """Verify that projection cache prunes oldest files first."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_file = root / "old"
            new_file = root / "new"
            old_file.write_bytes(b"o" * 10)
            new_file.write_bytes(b"n" * 10)
            old_time = 1_700_000_000
            new_time = old_time + 10
            old_file.touch()
            new_file.touch()

            os.utime(old_file, (old_time, old_time))
            os.utime(new_file, (new_time, new_time))

            total = prune_cache_dir(root, 10)

            self.assertEqual(10, total)
            self.assertFalse(old_file.exists())
            self.assertTrue(new_file.exists())

    def test_projection_cache_preserves_active_artifact(self) -> None:
        """Verify that projection cache preserves active artifact."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_file = root / "old"
            active_file = root / "active"
            old_file.write_bytes(b"o" * 10)
            active_file.write_bytes(b"a" * 10)
            old_time = 1_700_000_000
            active_time = old_time + 10
            old_file.touch()
            active_file.touch()

            os.utime(old_file, (old_time, old_time))
            os.utime(active_file, (active_time, active_time))

            total = prune_cache_dir(root, 1, preserve=(active_file,))

            self.assertEqual(10, total)
            self.assertFalse(old_file.exists())
            self.assertTrue(active_file.exists())

    def test_projection_cache_reuses_same_inputs_and_settings(self) -> None:
        """Verify that projection cache reuses same inputs and settings."""
        with TemporaryDirectory() as tmp:
            os.environ["LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR"] = tmp
            os.environ["LOCAL_DATA_STUDIO_ATLAS_CACHE_MAX_BYTES"] = str(1024 * 1024)
            self.assertIsNotNone(atlas_cache_patch)

            calls = 0
            original_run_umap = projection._run_umap

            def fake_run_umap(hidden_vectors: Any, *, umap_args: dict | None = None) -> Any:
                """Exercise fake run umap behavior."""
                nonlocal calls
                calls += 1
                row_count = len(hidden_vectors)
                return projection.Projection(
                    projection=np.column_stack((np.arange(row_count), np.arange(row_count) + 10)).astype(float),
                    knn_indices=np.zeros((row_count, 2), dtype=np.int64),
                    knn_distances=np.zeros((row_count, 2), dtype=float),
                )

            projection._run_umap = fake_run_umap
            try:
                df = pd.DataFrame({"vector": [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]})
                first = projection.compute_projection(
                    df,
                    inputs="vector",
                    modality="vector",
                    x="x",
                    y="y",
                    neighbors="neighbors",
                    umap_args={"n_neighbors": 2, "random_state": 42},
                )
                second = projection.compute_projection(
                    df,
                    inputs="vector",
                    modality="vector",
                    x="x",
                    y="y",
                    neighbors="neighbors",
                    umap_args={"n_neighbors": 2, "random_state": 42},
                )
            finally:
                projection._run_umap = original_run_umap
                os.environ.pop("LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR", None)
                os.environ.pop("LOCAL_DATA_STUDIO_ATLAS_CACHE_MAX_BYTES", None)

            self.assertEqual(1, calls)
            self.assertEqual(first["x"].tolist(), second["x"].tolist())
            self.assertTrue(any(path.is_file() for path in Path(tmp).rglob("*")))

    def test_projected_dataset_cache_path_is_stable_for_same_inputs(self) -> None:
        """Verify that projected dataset cache path is stable for same inputs."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"text":"a"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "text-model"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            (model_path / "tokenizer_config.json").write_text("{}", encoding="utf-8")
            options = AtlasOptions(
                sample=100,
                host="127.0.0.1",
                port=5055,
                batch_size=16,
                text_embedder="sentence-transformers",
                image_embedder=None,
                trust_remote_code=False,
            )

            with patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root):
                first = atlas_dataset_cache_path(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data",
                    model_path=model_path,
                    options=options,
                )
                second = atlas_dataset_cache_path(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data",
                    model_path=model_path,
                    options=options,
                )
                changed = atlas_dataset_cache_path(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data WHERE text = 'a'",
                    model_path=model_path,
                    options=options,
                )
                tsne = atlas_dataset_cache_path(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data",
                    model_path=model_path,
                    options=replace(options, projection_method="tsne", umap_projection_mode=None, umap_anchor_sample=None),
                )
                tsne_with_other_umap_settings = atlas_dataset_cache_path(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data",
                    model_path=model_path,
                    options=replace(options, projection_method="tsne", umap_projection_mode="anchor_transform", umap_anchor_sample=1),
                )
                smaller_sample = atlas_dataset_cache_path(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data",
                    model_path=model_path,
                    options=replace(options, sample=10),
                )

        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)
        self.assertNotEqual(first, tsne)
        self.assertEqual(tsne, tsne_with_other_umap_settings)
        self.assertNotEqual(first, smaller_sample)

    def test_prepare_atlas_dataset_reuses_projected_parquet_cache(self) -> None:
        """Verify that prepare atlas dataset reuses projected parquet cache."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"text":"a"}\n{"text":"b"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "text-model"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            (model_path / "tokenizer_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder=None,
                trust_remote_code=False,
            )
            calls = 0

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame({"text": ["a", "b"]})

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                nonlocal calls
                calls += 1
                data_frame[ATLAS_PROJECTION_X] = [0.0, 1.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0, 0.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = ['{"ids":[1],"distances":[0.1]}', '{"ids":[0],"distances":[0.1]}']
                return data_frame

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
            ):
                first = prepare_atlas_dataset(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )
                second = prepare_atlas_dataset(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached_columns = pd.read_parquet(first.path).columns

        self.assertFalse(first.cache_hit)
        self.assertTrue(second.cache_hit)
        self.assertEqual(first.path, second.path)
        self.assertEqual(1, calls)
        self.assertEqual(ATLAS_PROJECTION_X, first.x)
        self.assertEqual(ATLAS_PROJECTION_Y, first.y)
        self.assertIsNone(first.neighbors)
        self.assertNotIn(ATLAS_PROJECTION_NEIGHBORS, cached_columns)

    def test_concurrent_atlas_cache_misses_compute_projection_once(self) -> None:
        """Verify that concurrent atlas cache misses compute projection once."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"text":"a"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "text-model"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            (model_path / "tokenizer_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(None, "127.0.0.1", 5055, None, None, None, False)
            entered = Event()
            release = Event()
            calls = 0
            results = []

            def fake_project(data_frame, **kwargs):  # noqa: ANN001, ARG001
                """Exercise fake project behavior."""
                nonlocal calls
                calls += 1
                entered.set()
                release.wait(timeout=2)
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                return data_frame

            def prepare() -> None:
                """Exercise prepare behavior."""
                results.append(
                    prepare_atlas_dataset(
                        path=data_path,
                        column="text",
                        modality="text",
                        sql=None,
                        model_path=model_path,
                        options=options,
                        context=DummyContext(),
                    )
                )

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", return_value=pd.DataFrame({"text": ["a"]})),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project),
            ):
                first_thread = Thread(target=prepare)
                second_thread = Thread(target=prepare)
                first_thread.start()
                self.assertTrue(entered.wait(timeout=2))
                second_thread.start()
                release.set()
                first_thread.join(timeout=2)
                second_thread.join(timeout=2)

        self.assertEqual(1, calls)
        self.assertEqual([False, True], sorted(result.cache_hit for result in results))

    def test_prepare_atlas_dataset_limits_embedding_and_cached_rows(self) -> None:
        """Apply ATLAS_SAMPLE before embedding and cache at most that many rows."""

        class DummyContext:
            """Ignore progress updates for cache construction."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001, ARG002
                """Accept a progress update."""

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"text":"value"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "text-model"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=4,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder="sentence-transformers",
                image_embedder=None,
                trust_remote_code=False,
                projection_method="pca",
                umap_projection_mode=None,
                umap_anchor_sample=None,
            )

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Return a filtered result while keeping sampling application-owned."""
                self.assertEqual("SELECT text FROM data WHERE text IS NOT NULL", query)
                self.assertIsNone(sample)
                return pd.DataFrame({"text": [f"row-{index}" for index in range(10)]})

            def fake_project(data_frame, **kwargs):  # noqa: ANN001, ARG001
                """Assert that sampling happened before encoder input is consumed."""
                self.assertEqual(4, len(data_frame))
                return AtlasProjectionCoordinates(np.zeros((4, 2), dtype=np.float32))

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project),
                patch("local_data_studio.server.atlas_components.dataset.effective_embedder_for_modality", return_value="sentence-transformers"),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql="SELECT text FROM data WHERE text IS NOT NULL",
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            self.assertEqual(4, prepared.row_count)
            self.assertEqual(4, len(pd.read_parquet(prepared.path)))

    def test_prepare_atlas_dataset_converts_image_urls_to_bytes_for_embedding(self) -> None:
        """Verify that prepare atlas dataset converts image urls to bytes for embedding."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"image":"https://example.test/image.jpg","label":"sample"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "image-model"
            model_path.mkdir(parents=True)
            (model_path / "preprocessor_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder="transformers",
                trust_remote_code=False,
            )

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame({"image": ["https://example.test/image.jpg"], "label": ["sample"]})

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                self.assertEqual(ATLAS_EMBED_INPUT_COLUMN, kwargs["input_column"])
                self.assertEqual(b"\xff\xd8\xfftest", data_frame[ATLAS_EMBED_INPUT_COLUMN].iloc[0]["bytes"])
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = [{"ids": [], "distances": []}]
                return data_frame

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
                patch("local_data_studio.server.atlas_components.dataset.effective_embedder_for_modality", return_value="transformers"),
                patch("local_data_studio.server.atlas_components.images.read_url_bytes", return_value=b"\xff\xd8\xfftest"),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="image",
                    modality="image",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached = pd.read_parquet(prepared.path)
            self.assertIn("image", cached.columns)
            self.assertNotIn(ATLAS_EMBED_INPUT_COLUMN, cached.columns)
            self.assertNotIn(ATLAS_PROJECTION_NEIGHBORS, cached.columns)
            self.assertEqual("https://example.test/image.jpg", cached["image"].iloc[0])

    def test_prepare_atlas_dataset_skips_unreadable_image_rows(self) -> None:
        """Verify that prepare atlas dataset skips unreadable image rows."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"image":"https://example.test/good.jpg"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "image-model"
            model_path.mkdir(parents=True)
            (model_path / "preprocessor_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder="transformers",
                trust_remote_code=False,
            )

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame({"image": ["https://example.test/good.jpg", "https://example.test/bad.jpg"], "label": ["good", "bad"]})

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                self.assertEqual(1, len(data_frame))
                self.assertEqual(b"\xff\xd8\xffgood", data_frame[ATLAS_EMBED_INPUT_COLUMN].iloc[0]["bytes"])
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = [{"ids": [], "distances": []}]
                return data_frame

            def fake_read_url_bytes(url: str) -> bytes:
                """Exercise fake read url bytes behavior."""
                if url.endswith("bad.jpg"):
                    raise ValueError("connection reset")
                return b"\xff\xd8\xffgood"

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
                patch("local_data_studio.server.atlas_components.dataset.effective_embedder_for_modality", return_value="transformers"),
                patch("local_data_studio.server.atlas_components.images.read_url_bytes", side_effect=fake_read_url_bytes),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="image",
                    modality="image",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached = pd.read_parquet(prepared.path)
            self.assertEqual(["good"], cached["label"].tolist())
            self.assertEqual("https://example.test/good.jpg", cached["image"].iloc[0])

    def test_prepare_atlas_dataset_preserves_dict_image_bytes_for_display(self) -> None:
        """Verify that prepare atlas dataset preserves dict image bytes for display."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"image":{"bytes":"89504e470d0a1a0a","path":"scenes/scene_003444.png"}}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "image-model"
            model_path.mkdir(parents=True)
            (model_path / "preprocessor_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder="transformers",
                trust_remote_code=False,
            )

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame({"image": [{"bytes": "89504e470d0a1a0a", "path": "scenes/scene_003444.png"}]})

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                self.assertEqual(b"\x89PNG\r\n\x1a\n", data_frame[ATLAS_EMBED_INPUT_COLUMN].iloc[0]["bytes"])
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = [{"ids": [], "distances": []}]
                return data_frame

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
                patch("local_data_studio.server.atlas_components.dataset.effective_embedder_for_modality", return_value="transformers"),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="image",
                    modality="image",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached = pd.read_parquet(prepared.path)
            image = cached["image"].iloc[0]
            self.assertEqual(b"\x89PNG\r\n\x1a\n", image["bytes"])
            self.assertEqual("scenes/scene_003444.png", image["path"])

    def test_prepare_atlas_dataset_preserves_python_bytes_in_image_objects(self) -> None:
        """Verify that prepare atlas dataset preserves python bytes in image objects."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"image":"placeholder"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "image-model"
            model_path.mkdir(parents=True)
            (model_path / "preprocessor_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder="transformers",
                trust_remote_code=False,
            )

            image_bytes = b"\x89PNG\r\n\x1a\n"

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame({"image": [{"bytes": image_bytes, "path": "scenes/scene_003444.png"}]})

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                self.assertEqual(image_bytes, data_frame[ATLAS_EMBED_INPUT_COLUMN].iloc[0]["bytes"])
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = [{"ids": [], "distances": []}]
                return data_frame

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
                patch("local_data_studio.server.atlas_components.dataset.effective_embedder_for_modality", return_value="transformers"),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="image",
                    modality="image",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached = pd.read_parquet(prepared.path)
            image = cached["image"].iloc[0]
            self.assertEqual(image_bytes, image["bytes"])
            self.assertEqual("scenes/scene_003444.png", image["path"])

    def test_prepare_atlas_dataset_preserves_other_image_columns(self) -> None:
        """Verify that prepare atlas dataset preserves other image columns."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"image":"https://example.test/image.jpg"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "image-model"
            model_path.mkdir(parents=True)
            (model_path / "preprocessor_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder="transformers",
                trust_remote_code=False,
            )

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame(
                    {
                        "image": ["https://example.test/image.jpg"],
                        "part_mask": [{"bytes": "89504e470d0a1a0a", "path": "masks/part/scene.png"}],
                        "metadata": [{"nested": "not an image", "payload": b"binary"}],
                    }
                )

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                self.assertEqual(b"\xff\xd8\xfftest", data_frame[ATLAS_EMBED_INPUT_COLUMN].iloc[0]["bytes"])
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = [{"ids": [], "distances": []}]
                return data_frame

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
                patch("local_data_studio.server.atlas_components.dataset.effective_embedder_for_modality", return_value="transformers"),
                patch("local_data_studio.server.atlas_components.images.read_url_bytes", return_value=b"\xff\xd8\xfftest"),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="image",
                    modality="image",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached = pd.read_parquet(prepared.path)
            self.assertEqual("https://example.test/image.jpg", cached["image"].iloc[0])
            self.assertEqual(b"\x89PNG\r\n\x1a\n", cached["part_mask"].iloc[0]["bytes"])
            self.assertEqual("masks/part/scene.png", cached["part_mask"].iloc[0]["path"])
            self.assertIsInstance(cached["metadata"].iloc[0], str)
            self.assertIn("<binary 6 bytes>", cached["metadata"].iloc[0])

    def test_prepare_atlas_dataset_truncates_long_text_for_embedding_and_cache(self) -> None:
        """Verify that prepare atlas dataset truncates long text for embedding and cache."""

        class DummyContext:
            """Test dummy context behavior."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Exercise update behavior."""
                return None

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data.jsonl"
            data_path.write_text('{"text":"placeholder"}\n', encoding="utf-8")
            model_root = root / "models"
            model_path = model_root / "text-model"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            (model_path / "tokenizer_config.json").write_text("{}", encoding="utf-8")
            data_cache = root / "cache" / "atlas" / "datasets"
            cache_root = root / "cache" / "atlas"
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder=None,
                trust_remote_code=False,
            )

            def fake_load_datasets(inputs, query=None, sample=None, splits=None):  # noqa: ANN001, ARG001
                """Exercise fake load datasets behavior."""
                return pd.DataFrame({"text": ["x" * 100], "notes": ["y" * 100]})

            def fake_project_atlas_frame(data_frame, **kwargs):  # noqa: ANN001
                """Exercise fake project atlas frame behavior."""
                self.assertEqual(ATLAS_EMBED_INPUT_COLUMN, kwargs["input_column"])
                self.assertEqual("x" * 8, data_frame[ATLAS_EMBED_INPUT_COLUMN].iloc[0])
                data_frame[ATLAS_PROJECTION_X] = [0.0]
                data_frame[ATLAS_PROJECTION_Y] = [1.0]
                data_frame[ATLAS_PROJECTION_NEIGHBORS] = [{"ids": [], "distances": []}]
                return data_frame

            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", model_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", data_cache),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", cache_root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_TEXT_MAX_CHARS", 8),
                patch("local_data_studio.server.atlas_components.images.ATLAS_TEXT_MAX_CHARS", 8),
                patch("local_data_studio.server.atlas_components.dataset.load_datasets", side_effect=fake_load_datasets),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame", side_effect=fake_project_atlas_frame),
            ):
                prepared = prepare_atlas_dataset(
                    path=data_path,
                    column="text",
                    modality="text",
                    sql=None,
                    model_path=model_path,
                    options=options,
                    context=DummyContext(),
                )

            cached = pd.read_parquet(prepared.path)
            self.assertNotIn(ATLAS_EMBED_INPUT_COLUMN, cached.columns)
            self.assertEqual(f"{'x' * 8}{ATLAS_TRUNCATION_SUFFIX}", cached["text"].iloc[0])
            self.assertEqual(f"{'y' * 8}{ATLAS_TRUNCATION_SUFFIX}", cached["notes"].iloc[0])

    def test_project_atlas_frame_honors_float16_for_vector_embeddings(self) -> None:
        """Verify that project atlas frame honors float16 for vector embeddings."""
        options = AtlasOptions(
            sample=None,
            host="127.0.0.1",
            port=5055,
            batch_size=None,
            text_embedder=None,
            image_embedder=None,
            trust_remote_code=False,
            embedding_dtype="float16",
            umap_projection_mode="full",
            umap_anchor_sample=None,
        )
        seen_dtype = None

        def fake_run_full_projection(embeddings):  # noqa: ANN001
            """Exercise fake run full projection behavior."""
            nonlocal seen_dtype
            seen_dtype = embeddings.dtype
            return projection.Projection(
                projection=np.array([[0.0, 1.0], [1.0, 0.0]], dtype=np.float32),
                knn_indices=np.zeros((2, 1), dtype=np.int64),
                knn_distances=np.zeros((2, 1), dtype=np.float32),
            )

        with patch("local_data_studio.server.atlas_components.reducers.run_umap_projection", side_effect=fake_run_full_projection):
            projected = project_atlas_frame(
                pd.DataFrame({"vector": [[1.0, 2.0], [3.0, 4.0]]}),
                input_column="vector",
                modality="vector",
                model_path=Path("models/embedder/example"),
                options=options,
            )

        self.assertEqual(np.float16, seen_dtype)
        self.assertEqual((2, 2), projected.values.shape)
        self.assertTrue(np.array_equal(np.array([[0.0, 1.0], [1.0, 0.0]], dtype=np.float32), projected.values))

    def test_full_projection_sets_single_threaded_umap_with_seed(self) -> None:
        """Verify that full projection sets single threaded umap with seed."""
        seen_args: dict[str, Any] | None = None

        def fake_run_umap(embeddings, *, umap_args=None):  # noqa: ANN001
            """Exercise fake run umap behavior."""
            nonlocal seen_args
            seen_args = umap_args
            row_count = len(embeddings)
            return projection.Projection(
                projection=np.zeros((row_count, 2), dtype=np.float32),
                knn_indices=np.zeros((row_count, 0), dtype=np.int64),
                knn_distances=np.zeros((row_count, 0), dtype=np.float32),
            )

        with patch("local_data_studio.server.atlas_components.reducers._run_umap", side_effect=fake_run_umap):
            _run_full_projection(np.ones((3, 2), dtype=np.float32))

        self.assertIsNotNone(seen_args)
        self.assertEqual(42, seen_args["random_state"])
        self.assertEqual(1, seen_args["n_jobs"])

    def test_anchor_transform_projects_remainder_without_neighbors(self) -> None:
        """Verify that anchor transform projects remainder without neighbors."""
        options = AtlasOptions(
            sample=None,
            host="127.0.0.1",
            port=5055,
            batch_size=1,
            text_embedder=None,
            image_embedder=None,
            trust_remote_code=False,
            embedding_dtype="float32",
            umap_projection_mode="anchor_transform",
            umap_anchor_sample=2,
        )
        calls: list[int] = []

        class FakeReducer:
            """Test fake reducer behavior."""

            def fit_transform(self, embeddings):  # noqa: ANN001
                """Exercise fit transform behavior."""
                return np.column_stack((np.arange(len(embeddings)), np.arange(len(embeddings)) + 10)).astype(np.float32)

            def transform(self, embeddings):  # noqa: ANN001
                """Exercise transform behavior."""
                return np.column_stack((np.arange(len(embeddings)) + 100, np.arange(len(embeddings)) + 200)).astype(np.float32)

        class FakeSession:
            """Test fake session behavior."""

            def embed(self, items):  # noqa: ANN001
                """Exercise embed behavior."""
                calls.append(len(items))
                return np.ones((len(items), 3), dtype=np.float32)

        class _Indexer:
            def __init__(self, values: list[str]) -> None:
                self.values = values

            def __getitem__(self, index: int) -> str:
                return self.values[index]

        class _Column:
            def __init__(self, values: list[str]) -> None:
                self.values = values
                self.iloc = _Indexer(values)

            def __len__(self) -> int:
                return len(self.values)

            def tolist(self) -> list[str]:
                raise AssertionError("anchor transform must not materialize the full column")

        class _Frame:
            def __init__(self, values: list[str]) -> None:
                self.column = _Column(values)

            def __getitem__(self, column: str) -> _Column:
                self.requested_column = column
                return self.column

        with (
            patch("local_data_studio.server.atlas_components.projection.create_embedding_session", return_value=FakeSession()) as factory,
            patch("umap.UMAP", return_value=FakeReducer()),
        ):
            projected = project_atlas_frame(
                _Frame(["a", "b", "c", "d"]),
                input_column="text",
                modality="text",
                model_path=Path("models/embedder/example"),
                options=options,
            )

        self.assertEqual([1, 1, 1, 1], calls)
        self.assertEqual((4, 2), projected.values.shape)
        factory.assert_called_once()

    def test_atlas_command_omits_neighbors_when_projection_has_none(self) -> None:
        """Verify that atlas command omits neighbors when projection has none."""
        command = build_atlas_command(
            path=Path("cache/atlas/datasets/example.parquet"),
            column="text",
            modality="text",
            sql=None,
            model_path=Path("models/embedder/example"),
            options=AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder=None,
                trust_remote_code=False,
            ),
            projection_columns=(ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, None),
        )

        self.assertIn("--disable-projection", command)
        self.assertNotIn("--neighbors", command)
        self.assertNotIn("--with", command)

    def test_atlas_command_uses_current_python_module(self) -> None:
        """Verify that atlas command uses current python module."""
        command = build_atlas_command(
            path=Path("cache/atlas/datasets/example.parquet"),
            column="text",
            modality="text",
            sql=None,
            model_path=Path("models/embedder/example"),
            options=AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder=None,
                trust_remote_code=False,
            ),
            projection_columns=(ATLAS_PROJECTION_X, ATLAS_PROJECTION_Y, None),
        )

        self.assertEqual([sys.executable, "-m", "embedding_atlas.cli"], command[:3])

    def test_reserve_atlas_start_port_advances_preferred_ports(self) -> None:
        """Verify that reserve atlas start port advances preferred ports."""
        options = AtlasOptions(
            sample=None,
            host="127.0.0.1",
            port=5055,
            batch_size=None,
            text_embedder=None,
            image_embedder=None,
            trust_remote_code=False,
        )
        with (
            patch("local_data_studio.server.atlas_components.ports.ATLAS_PORT_STATE", {}),
            patch("local_data_studio.server.atlas_components.ports._port_is_available", return_value=True),
        ):
            first = reserve_atlas_start_port(options)
            second = reserve_atlas_start_port(options)

        self.assertEqual(5055, first.port)
        self.assertEqual(5056, second.port)

    def test_reserve_atlas_start_port_skips_browser_restricted_ports(self) -> None:
        """Never expose an Atlas link on Chromium's blocked SIP ports."""
        options = AtlasOptions(
            sample=None,
            host="127.0.0.1",
            port=5059,
            batch_size=None,
            text_embedder=None,
            image_embedder=None,
            trust_remote_code=False,
        )
        with (
            patch("local_data_studio.server.atlas_components.ports.ATLAS_PORT_STATE", {}),
            patch("local_data_studio.server.atlas_components.ports._port_is_available", return_value=True),
        ):
            first = reserve_atlas_start_port(options)
            second = reserve_atlas_start_port(options)

        self.assertEqual(5059, first.port)
        self.assertEqual(5062, second.port)

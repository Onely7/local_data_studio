"""Tests for cli behavior."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import TestCase

from local_data_studio.cli import build_parser, configure_runtime_environment

PATH_ENV_NAMES = {
    "LOCAL_DATA_STUDIO_WORKSPACE_DIR",
    "LOCAL_DATA_STUDIO_ENV_FILE",
    "LOCAL_DATA_STUDIO_CONFIG_FILE",
    "DATA_DIR",
    "DATA_FILE",
    "CACHE_DIR",
    "EMBEDDER_MODELS_DIR",
    "FILE_SERVE_ROOTS",
    "VIS_EXCLUDE_FILES",
    "LOCAL_DATA_STUDIO_HOST",
    "LOCAL_DATA_STUDIO_PORT",
    "LOCAL_DATA_STUDIO_RELOAD",
}
SETTINGS_ENV_NAMES = {
    "ALLOW_DELETE_DATA",
    "ATLAS_BATCH_SIZE",
    "ATLAS_CACHE_MAX_BYTES",
    "ATLAS_EMBEDDING_DTYPE",
    "ATLAS_HOST",
    "ATLAS_PORT",
    "ATLAS_SAMPLE",
    "ATLAS_TEXT_MAX_CHARS",
    "ATLAS_TRUST_REMOTE_CODE",
    "ATLAS_UMAP_ANCHOR_SAMPLE",
    "ATLAS_UMAP_PROJECTION_MODE",
    "EDA_CACHE_MAX_BYTES",
    "EDA_CELL_MAX_CHARS",
    "EDA_NESTED_POLICY",
    "EDA_ROW_LIMIT",
}


class CliConfigTests(TestCase):
    """Test cli config behavior."""

    def setUp(self) -> None:
        """Exercise set up behavior."""
        environment_names = PATH_ENV_NAMES | SETTINGS_ENV_NAMES
        self.original_env = {name: os.environ.get(name) for name in environment_names}
        for name in environment_names:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        """Exercise tear down behavior."""
        for name, original_value in self.original_env.items():
            if original_value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = original_value

    def test_config_file_populates_runtime_environment(self) -> None:
        """Verify that config file populates runtime environment."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "local_data_studio.toml"
            config.write_text(
                """
[paths]
workspace_dir = "workspace"
env_file = ".env.local"
data_dir = "datasets"
cache_dir = "cache-store"
models_dir = "models/custom"
file_serve_roots = ["datasets", "images"]
vis_exclude_files = ["datasets/ignore.csv", "datasets/archive/old.parquet"]

[server]
host = "0.0.0.0"
port = 8765
reload = true

[settings]
eda_row_limit = 25000
eda_cell_max_chars = 4096
eda_nested_policy = "drop"
eda_cache_max_bytes = 2147483648
allow_delete_data = false
atlas_host = "0.0.0.0"
atlas_port = 6060
atlas_sample = 5000
atlas_batch_size = 32
atlas_cache_max_bytes = 4294967296
atlas_text_max_chars = 2048
atlas_embedding_dtype = "float16"
atlas_umap_projection_mode = "anchor_transform"
atlas_umap_anchor_sample = 2000
atlas_trust_remote_code = true
""",
                encoding="utf-8",
            )

            args = build_parser().parse_args(["--config", str(config)])
            host, port, reload = configure_runtime_environment(args)

            workspace = root / "workspace"
            self.assertEqual("0.0.0.0", host)
            self.assertEqual(8765, port)
            self.assertTrue(reload)
            self.assertEqual(str(workspace.resolve()), os.environ["LOCAL_DATA_STUDIO_WORKSPACE_DIR"])
            self.assertEqual(str(config.resolve()), os.environ["LOCAL_DATA_STUDIO_CONFIG_FILE"])
            self.assertEqual(str((workspace / ".env.local").resolve()), os.environ["LOCAL_DATA_STUDIO_ENV_FILE"])
            self.assertEqual(str((workspace / "datasets").resolve()), os.environ["DATA_DIR"])
            self.assertEqual(str((workspace / "cache-store").resolve()), os.environ["CACHE_DIR"])
            self.assertEqual(str((workspace / "models/custom").resolve()), os.environ["EMBEDDER_MODELS_DIR"])
            self.assertEqual(
                ",".join([str((workspace / "datasets").resolve()), str((workspace / "images").resolve())]),
                os.environ["FILE_SERVE_ROOTS"],
            )
            self.assertEqual(
                ",".join(
                    [
                        str((workspace / "datasets/ignore.csv").resolve()),
                        str((workspace / "datasets/archive/old.parquet").resolve()),
                    ]
                ),
                os.environ["VIS_EXCLUDE_FILES"],
            )
            self.assertEqual("25000", os.environ["EDA_ROW_LIMIT"])
            self.assertEqual("drop", os.environ["EDA_NESTED_POLICY"])
            self.assertEqual("false", os.environ["ALLOW_DELETE_DATA"])
            self.assertEqual("6060", os.environ["ATLAS_PORT"])
            self.assertEqual("float16", os.environ["ATLAS_EMBEDDING_DTYPE"])
            self.assertEqual("anchor_transform", os.environ["ATLAS_UMAP_PROJECTION_MODE"])
            self.assertEqual("true", os.environ["ATLAS_TRUST_REMOTE_CODE"])

    def test_environment_overrides_config_and_cli_overrides_environment(self) -> None:
        """Verify that environment overrides config and cli overrides environment."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "local_data_studio.toml"
            config.write_text(
                """
[paths]
workspace_dir = "workspace"
data_dir = "config-data"
cache_dir = "config-cache"

[settings]
eda_row_limit = 25000
atlas_sample = 1000
""",
                encoding="utf-8",
            )
            env_data_dir = root / "env-data"
            cli_cache_dir = root / "cli-cache"
            os.environ["DATA_DIR"] = str(env_data_dir)
            os.environ["CACHE_DIR"] = str(root / "env-cache")
            os.environ["EDA_ROW_LIMIT"] = "1000"

            args = build_parser().parse_args(["--config", str(config), "--cache-dir", str(cli_cache_dir)])
            configure_runtime_environment(args)

            self.assertEqual(str(env_data_dir), os.environ["DATA_DIR"])
            self.assertEqual(str(cli_cache_dir), os.environ["CACHE_DIR"])
            self.assertEqual("1000", os.environ["EDA_ROW_LIMIT"])
            self.assertEqual("1000", os.environ["ATLAS_SAMPLE"])

    def test_direct_asgi_startup_applies_toml_settings(self) -> None:
        """Apply `[settings]` when Uvicorn imports the application directly."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "local_data_studio.toml"
            config.write_text(
                """
[settings]
eda_row_limit = 123
allow_delete_data = false
atlas_sample = 456
""",
                encoding="utf-8",
            )
            repository_root = Path(__file__).resolve().parents[1]
            environment = os.environ.copy()
            environment.update(
                {
                    "PYTHONPATH": str(repository_root / "src"),
                    "LOCAL_DATA_STUDIO_CONFIG_FILE": str(config),
                }
            )
            for name in SETTINGS_ENV_NAMES:
                environment.pop(name, None)
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "from local_data_studio.server.config import "
                        "ALLOW_DELETE_DATA, ATLAS_SAMPLE, DEFAULT_EDA_SAMPLE; "
                        "assert (DEFAULT_EDA_SAMPLE, ALLOW_DELETE_DATA, ATLAS_SAMPLE) == (123, False, 456)"
                    ),
                ],
                cwd=repository_root,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, result.returncode, result.stderr)

    def test_workspace_cli_sets_default_base(self) -> None:
        """Verify that workspace cli sets default base."""
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"

            args = build_parser().parse_args(["--workspace-dir", str(workspace), "--data-dir", "data"])
            configure_runtime_environment(args)

            self.assertEqual(str(workspace.resolve()), os.environ["LOCAL_DATA_STUDIO_WORKSPACE_DIR"])
            self.assertEqual("data", os.environ["DATA_DIR"])

    def test_importing_app_defers_heavy_feature_modules(self) -> None:
        """Verify that importing app defers heavy feature modules."""
        repository_root = Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(repository_root / "src")
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; import local_data_studio.app; "
                    "assert 'local_data_studio.server.atlas' not in sys.modules; "
                    "assert 'local_data_studio.server.eda_reports' not in sys.modules; "
                    "assert 'litellm' not in sys.modules"
                ),
            ],
            cwd=repository_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)

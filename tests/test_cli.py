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
    "DATA_DIR",
    "DATA_FILE",
    "CACHE_DIR",
    "EMBEDDER_MODELS_DIR",
    "FILE_SERVE_ROOTS",
    "LOCAL_DATA_STUDIO_HOST",
    "LOCAL_DATA_STUDIO_PORT",
    "LOCAL_DATA_STUDIO_RELOAD",
}


class CliConfigTests(TestCase):
    def setUp(self) -> None:
        self.original_env = {name: os.environ.get(name) for name in PATH_ENV_NAMES}
        for name in PATH_ENV_NAMES:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name in PATH_ENV_NAMES:
            if self.original_env[name] is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = self.original_env[name]

    def test_config_file_populates_runtime_environment(self) -> None:
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

[server]
host = "0.0.0.0"
port = 8765
reload = true
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
            self.assertEqual(str((workspace / ".env.local").resolve()), os.environ["LOCAL_DATA_STUDIO_ENV_FILE"])
            self.assertEqual(str((workspace / "datasets").resolve()), os.environ["DATA_DIR"])
            self.assertEqual(str((workspace / "cache-store").resolve()), os.environ["CACHE_DIR"])
            self.assertEqual(str((workspace / "models/custom").resolve()), os.environ["EMBEDDER_MODELS_DIR"])
            self.assertEqual(
                ",".join([str((workspace / "datasets").resolve()), str((workspace / "images").resolve())]),
                os.environ["FILE_SERVE_ROOTS"],
            )

    def test_environment_overrides_config_and_cli_overrides_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "local_data_studio.toml"
            config.write_text(
                """
[paths]
workspace_dir = "workspace"
data_dir = "config-data"
cache_dir = "config-cache"
""",
                encoding="utf-8",
            )
            env_data_dir = root / "env-data"
            cli_cache_dir = root / "cli-cache"
            os.environ["DATA_DIR"] = str(env_data_dir)
            os.environ["CACHE_DIR"] = str(root / "env-cache")

            args = build_parser().parse_args(["--config", str(config), "--cache-dir", str(cli_cache_dir)])
            configure_runtime_environment(args)

            self.assertEqual(str(env_data_dir), os.environ["DATA_DIR"])
            self.assertEqual(str(cli_cache_dir), os.environ["CACHE_DIR"])

    def test_workspace_cli_sets_default_base(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"

            args = build_parser().parse_args(["--workspace-dir", str(workspace), "--data-dir", "data"])
            configure_runtime_environment(args)

            self.assertEqual(str(workspace.resolve()), os.environ["LOCAL_DATA_STUDIO_WORKSPACE_DIR"])
            self.assertEqual("data", os.environ["DATA_DIR"])

    def test_importing_app_defers_heavy_feature_modules(self) -> None:
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
                    "assert 'local_data_studio.server.eda_reports' not in sys.modules"
                ),
            ],
            cwd=repository_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)

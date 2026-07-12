"""Command-line entrypoint for Local Data Studio."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import uvicorn

from .runtime_config import CONFIG_FILE_ENV, apply_settings_environment, config_section, read_runtime_config

PATH_ENV_KEYS: dict[str, str] = {
    "workspace_dir": "LOCAL_DATA_STUDIO_WORKSPACE_DIR",
    "env_file": "LOCAL_DATA_STUDIO_ENV_FILE",
    "data_dir": "DATA_DIR",
    "data_file": "DATA_FILE",
    "cache_dir": "CACHE_DIR",
    "models_dir": "EMBEDDER_MODELS_DIR",
    "file_serve_roots": "FILE_SERVE_ROOTS",
    "vis_exclude_dirs": "VIS_EXCLUDE_DIRS",
    "vis_exclude_files": "VIS_EXCLUDE_FILES",
}


def _set_path_env(name: str, value: str | None, *, overwrite: bool = True) -> None:
    if value:
        if overwrite or name not in os.environ:
            os.environ[name] = str(Path(value).expanduser())


def _resolve_path(value: str, base_dir: Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return str(path.resolve())


def _coerce_path_setting(value: Any, base_dir: Path) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return _resolve_path(stripped, base_dir) if stripped else None
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        resolved = [_resolve_path(str(item), base_dir) for item in value if str(item).strip()]
        return ",".join(resolved) if resolved else None
    return str(value)


def _workspace_from(args: argparse.Namespace, config: Mapping[str, Any], config_path: Path | None) -> Path:
    paths = config_section(config, "paths")
    raw_workspace = args.workspace_dir or os.environ.get("LOCAL_DATA_STUDIO_WORKSPACE_DIR") or paths.get("workspace_dir")
    if raw_workspace:
        base_dir = config_path.parent if config_path else Path.cwd()
        return Path(_resolve_path(str(raw_workspace), base_dir))
    return Path.cwd().resolve()


def _apply_config_environment(config: Mapping[str, Any], workspace_dir: Path) -> None:
    paths = config_section(config, "paths")
    _set_path_env("LOCAL_DATA_STUDIO_WORKSPACE_DIR", str(workspace_dir), overwrite=False)
    for key, env_name in PATH_ENV_KEYS.items():
        if key == "workspace_dir":
            continue
        coerced = _coerce_path_setting(paths.get(key), workspace_dir)
        _set_path_env(env_name, coerced, overwrite=False)


def configure_runtime_environment(args: argparse.Namespace) -> tuple[str, int, bool]:
    """Resolve CLI, environment, and TOML settings before importing the app.

    Explicit CLI paths take precedence over environment and config values. Relative
    config paths are resolved from the config file; other defaults use the workspace.

    Returns:
        The host, port, and reload values passed to Uvicorn.

    Raises:
        OSError: The requested config file cannot be read.
        tomllib.TOMLDecodeError: The config file is not valid TOML.
    """
    config, config_path = read_runtime_config(args.config)
    if config_path:
        os.environ[CONFIG_FILE_ENV] = str(config_path)
    workspace_dir = _workspace_from(args, config, config_path)
    _apply_config_environment(config, workspace_dir)
    apply_settings_environment(config)

    _set_path_env("LOCAL_DATA_STUDIO_WORKSPACE_DIR", str(workspace_dir), overwrite=bool(args.workspace_dir))
    _set_path_env("LOCAL_DATA_STUDIO_ENV_FILE", args.env_file)
    _set_path_env("DATA_DIR", args.data_dir)
    _set_path_env("DATA_FILE", args.data_file)
    _set_path_env("CACHE_DIR", args.cache_dir)
    _set_path_env("EMBEDDER_MODELS_DIR", args.models_dir)
    if args.file_serve_roots:
        os.environ["FILE_SERVE_ROOTS"] = ",".join(str(Path(item).expanduser()) for item in args.file_serve_roots)

    server = config_section(config, "server")
    host = args.host or os.environ.get("LOCAL_DATA_STUDIO_HOST") or str(server.get("host") or "127.0.0.1")
    port = args.port or int(os.environ.get("LOCAL_DATA_STUDIO_PORT") or server.get("port") or 8000)
    reload = bool(args.reload if args.reload is not None else _coerce_bool(os.environ.get("LOCAL_DATA_STUDIO_RELOAD"), server.get("reload", False)))
    return host, port, reload


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser without reading runtime configuration."""
    parser = argparse.ArgumentParser(description="Run the Local Data Studio web app.")
    parser.add_argument("--config", help="Path to a Local Data Studio TOML config file.")
    parser.add_argument("--workspace-dir", help="Base directory for .env, data, cache, and models/embedder defaults.")
    parser.add_argument("--env-file", help="Path to the .env file to load.")
    parser.add_argument("--host", help="Host interface for the local web server.")
    parser.add_argument("--port", type=int, help="Port for the local web server.")
    parser.add_argument("--cache-dir", help="Directory for Local Data Studio cache files.")
    parser.add_argument("--models-dir", help="Directory containing HuggingFace encoder model directories.")
    parser.add_argument("--file-serve-roots", nargs="+", help="Directories from which local image files may be served.")
    parser.add_argument("--reload", action=argparse.BooleanOptionalAction, default=None, help="Enable uvicorn reload for local development.")

    data_source = parser.add_mutually_exclusive_group()
    data_source.add_argument("--data-dir", help="Directory to search for datasets.")
    data_source.add_argument("--data-file", help="Single dataset file to open.")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Configure the workspace and run the Uvicorn web server.

    Args:
        argv: Arguments without the executable name. ``None`` reads ``sys.argv``.
    """
    args = build_parser().parse_args(argv)
    host, port, reload = configure_runtime_environment(args)

    uvicorn.run(
        "local_data_studio.app:app",
        host=host,
        port=port,
        reload=reload,
    )

"""Command-line entrypoint for Local Data Studio."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path

import uvicorn


def _set_path_env(name: str, value: str | None) -> None:
    if value:
        os.environ[name] = str(Path(value).expanduser())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Local Data Studio web app.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface for the local web server.")
    parser.add_argument("--port", type=int, default=8000, help="Port for the local web server.")
    parser.add_argument("--cache-dir", help="Directory for Local Data Studio cache files.")
    parser.add_argument("--reload", action="store_true", help="Enable uvicorn reload for local development.")

    data_source = parser.add_mutually_exclusive_group()
    data_source.add_argument("--data-dir", help="Directory to search for datasets.")
    data_source.add_argument("--data-file", help="Single dataset file to open.")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    _set_path_env("DATA_DIR", args.data_dir)
    _set_path_env("DATA_FILE", args.data_file)
    _set_path_env("CACHE_DIR", args.cache_dir)

    uvicorn.run(
        "local_data_studio.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )

"""Embedding Atlas command and child-environment construction."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from ..config import ATLAS_CACHE_DIR, ATLAS_CACHE_MAX_BYTES, ATLAS_CACHE_ROOT, BASE_DIR, PACKAGE_DIR
from .contracts import AtlasModality, AtlasOptions
from .embedding_backends import effective_embedder_for_modality


def embedding_atlas_executable() -> list[str]:
    """Return the installed Atlas module invocation using the active interpreter."""
    return [sys.executable, "-m", "embedding_atlas.cli"]


def build_atlas_command(
    *,
    path: Path,
    column: str,
    modality: AtlasModality,
    sql: str | None,
    model_path: Path,
    options: AtlasOptions,
    projection_columns: tuple[str, str, str | None] | None = None,
) -> list[str]:
    """Build an argument vector without shell interpolation."""
    command = [
        *embedding_atlas_executable(),
        str(path.resolve()),
        f"--{modality}",
        column,
        "--host",
        options.host,
        "--port",
        str(options.port),
        "--no-auto-port",
    ]
    if projection_columns is not None:
        x_column, y_column, neighbors_column = projection_columns
        command.extend(["--disable-projection", "--x", x_column, "--y", y_column])
        if neighbors_column is not None:
            command.extend(["--neighbors", neighbors_column])
        return command
    if sql:
        command.extend(["--query", sql])
    if options.sample is not None:
        command.extend(["--sample", str(options.sample)])
    if options.batch_size is not None:
        command.extend(["--batch-size", str(options.batch_size)])
    command.extend(["--with", "local_data_studio.server.atlas_cache_patch", "--model", str(model_path)])
    embedder = effective_embedder_for_modality(modality, model_path, options)
    if embedder:
        command.extend(["--embedder", embedder])
    if options.trust_remote_code:
        command.append("--trust-remote-code")
    return command


def embedding_atlas_env() -> dict[str, str]:
    """Return a child environment with bounded native threads and local caches."""
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    python_paths = [str(PACKAGE_DIR.parent), str(BASE_DIR)]
    if existing_python_path := env.get("PYTHONPATH"):
        python_paths.append(existing_python_path)
    env["PYTHONPATH"] = os.pathsep.join(python_paths)
    env["LOCAL_DATA_STUDIO_ATLAS_CACHE_DIR"] = str(ATLAS_CACHE_DIR)
    env["LOCAL_DATA_STUDIO_ATLAS_CACHE_PRUNE_DIR"] = str(ATLAS_CACHE_ROOT)
    env["LOCAL_DATA_STUDIO_ATLAS_CACHE_MAX_BYTES"] = str(ATLAS_CACHE_MAX_BYTES)
    env.setdefault("TOKENIZERS_PARALLELISM", "false")
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("OPENBLAS_NUM_THREADS", "1")
    env.setdefault("MKL_NUM_THREADS", "1")
    env.setdefault("VECLIB_MAXIMUM_THREADS", "1")
    env.setdefault("NUMEXPR_NUM_THREADS", "1")
    return env

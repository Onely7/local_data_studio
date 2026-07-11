"""Embedding Atlas command construction and subprocess supervision."""

from __future__ import annotations

import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from ..atlas_cache import prune_cache_dir
from ..config import (
    ATLAS_CACHE_DIR,
    ATLAS_CACHE_MAX_BYTES,
    ATLAS_CACHE_ROOT,
    BASE_DIR,
    PACKAGE_DIR,
)
from ..jobs import JobContext
from .contracts import AtlasModality, AtlasOptions
from .projection import effective_embedder_for_modality

ATLAS_URL_PATTERN = re.compile(
    r"https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\])(?::\d+)?"
    r"(?:/[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)?"
)
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
RUNNING_ATLAS_PROCESSES: list[subprocess.Popen[str]] = []


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
        "--auto-port",
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


def _reader_thread(stream: Any, output: queue.Queue[str]) -> None:
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            output.put(line.rstrip())
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def normalize_atlas_url(url: str) -> str:
    """Normalize wildcard hosts to a browser-reachable loopback URL."""
    cleaned = ANSI_ESCAPE_PATTERN.sub("", url).strip().rstrip(".,);")
    return cleaned.replace("://0.0.0.0", "://127.0.0.1")


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


def format_process_returncode(returncode: int | None) -> str:
    """Format normal exits and POSIX signals for user-facing errors."""
    if returncode is None:
        return "still running"
    if returncode >= 0:
        return f"exit code {returncode}"
    try:
        signal_name = signal.Signals(-returncode).name
    except ValueError:
        signal_name = f"signal {-returncode}"
    return f"{signal_name} ({returncode})"


def spawn_embedding_atlas(command: list[str], env: dict[str, str]) -> subprocess.Popen[str]:
    # Keep this eligible for posix_spawn on macOS. Do not add cwd,
    # preexec_fn, pass_fds, start_new_session, or shell=True.
    """Spawn Atlas through the macOS-safe ``posix_spawn``-eligible path.

    The child uses no shell and ``close_fds=False`` to avoid the known SIGSEGV
    regression. The returned process is owned by the runtime process registry.
    """
    return subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        close_fds=False,
    )


def launch_embedding_atlas(command: list[str], context: JobContext) -> tuple[str, int]:
    """Start Embedding Atlas and return its browser URL once available."""
    prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
    try:
        process = spawn_embedding_atlas(command, embedding_atlas_env())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail="embedding-atlas is not installed; run uv sync") from exc

    output: queue.Queue[str] = queue.Queue()
    if process.stdout is not None:
        threading.Thread(target=_reader_thread, args=(process.stdout, output), daemon=True).start()
    started = time.monotonic()
    recent_lines: list[str] = []
    try:
        while True:
            context.check_cancelled()
            while True:
                try:
                    line = output.get_nowait()
                except queue.Empty:
                    break
                if line:
                    recent_lines.append(line)
                    recent_lines = recent_lines[-12:]
                    if match := ATLAS_URL_PATTERN.search(line):
                        url = normalize_atlas_url(match.group(0))
                        prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
                        RUNNING_ATLAS_PROCESSES.append(process)
                        context.update(progress=1.0, message="Embedding Atlas is ready")
                        return url, process.pid
                    context.update(progress=None, message=line[-180:])
            if process.poll() is not None:
                details = "\n".join(recent_lines[-6:]) or format_process_returncode(process.returncode)
                raise HTTPException(status_code=500, detail=f"embedding-atlas exited before producing a URL: {details}")
            elapsed = time.monotonic() - started
            context.update(
                progress=min(0.95, 0.05 + elapsed / 300),
                message=f"Running Embedding Atlas for {int(elapsed)}s",
            )
            time.sleep(0.5)
    except Exception:
        _terminate_process(process)
        raise

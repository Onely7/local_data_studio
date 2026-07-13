"""Embedding Atlas command construction and subprocess supervision."""

from __future__ import annotations

import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx
from fastapi import HTTPException

from ..atlas_cache import prune_cache_dir
from ..config import ATLAS_CACHE_MAX_BYTES, ATLAS_CACHE_ROOT
from ..jobs import JobContext
from .command import build_atlas_command, embedding_atlas_env, embedding_atlas_executable
from .ports import is_browser_safe_port
from .readiness import (
    ATLAS_READINESS_TIMEOUT,
    ATLAS_SERVER_READY_TIMEOUT_SECONDS,
    ATLAS_URL_PATTERN,
    format_process_returncode,
    normalize_atlas_url,
)
from .readiness import (
    atlas_http_is_ready as _atlas_http_is_ready,
)
from .readiness import (
    atlas_url_target as _atlas_url_target,
)
from .runtime import AtlasRuntime

RUNNING_ATLAS_PROCESSES: list[subprocess.Popen[str]] = []


@dataclass(frozen=True, slots=True)
class LaunchedAtlasProcess:
    """A ready Atlas child process that has not yet been publicly registered."""

    url: str
    host: str
    port: int
    process: subprocess.Popen[str]


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


def start_embedding_atlas(
    command: list[str],
    context: JobContext,
    runtime: AtlasRuntime | None = None,
) -> LaunchedAtlasProcess:
    """Start Embedding Atlas and return the ready, unregistered child process."""
    prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
    if runtime is not None:
        runtime.check_open()
    context.check_cancelled()
    try:
        process = spawn_embedding_atlas(command, embedding_atlas_env())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail="embedding-atlas is not installed; run uv sync") from exc

    output: queue.Queue[str] = queue.Queue()
    if process.stdout is not None:
        threading.Thread(target=_reader_thread, args=(process.stdout, output), daemon=True).start()
    started = time.monotonic()
    recent_lines: list[str] = []
    pending_url: str | None = None
    pending_url_started: float | None = None
    try:
        readiness_client = httpx.Client(
            timeout=ATLAS_READINESS_TIMEOUT,
            trust_env=False,
            follow_redirects=False,
        )
        with readiness_client:
            while True:
                if runtime is not None:
                    runtime.check_open()
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
                            pending_url = normalize_atlas_url(match.group(0))
                            pending_url_started = time.monotonic()
                            parsed_port = urlsplit(pending_url).port
                            if parsed_port is None or not is_browser_safe_port(parsed_port):
                                raise HTTPException(
                                    status_code=500,
                                    detail=f"Embedding Atlas selected browser-restricted port {parsed_port}",
                                )
                            context.update(progress=0.98, message="Waiting for the Atlas page to accept connections")
                            continue
                        context.update(progress=None, message=line[-180:])
                if pending_url and _atlas_http_is_ready(readiness_client, pending_url):
                    if runtime is not None:
                        runtime.check_open()
                    context.check_cancelled()
                    prune_cache_dir(ATLAS_CACHE_ROOT, ATLAS_CACHE_MAX_BYTES)
                    context.update(progress=1.0, message="Embedding Atlas is ready")
                    target = _atlas_url_target(pending_url)
                    if target is None:
                        raise HTTPException(status_code=500, detail="Embedding Atlas returned a non-loopback URL")
                    host, port = target
                    return LaunchedAtlasProcess(url=pending_url, host=host, port=port, process=process)
                if process.poll() is not None:
                    details = "\n".join(recent_lines[-6:]) or format_process_returncode(process.returncode)
                    raise HTTPException(status_code=500, detail=f"embedding-atlas exited before producing a URL: {details}")
                elapsed = time.monotonic() - started
                if pending_url_started is not None and time.monotonic() - pending_url_started >= ATLAS_SERVER_READY_TIMEOUT_SECONDS:
                    raise HTTPException(status_code=504, detail="Embedding Atlas did not start accepting connections within 60 seconds")
                context.update(
                    progress=min(0.99, 0.96 + elapsed / 3000),
                    message=(f"Waiting for the Atlas page ({int(elapsed)}s)" if pending_url else f"Starting the Atlas server ({int(elapsed)}s)"),
                )
                time.sleep(0.5)
    except Exception:
        _terminate_process(process)
        raise


def launch_embedding_atlas(command: list[str], context: JobContext) -> tuple[str, int]:
    """Start Atlas and preserve the legacy raw URL/PID helper contract."""
    launched = start_embedding_atlas(command, context)
    RUNNING_ATLAS_PROCESSES.append(launched.process)
    return launched.url, launched.process.pid

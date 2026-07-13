"""Embedding Atlas stdout parsing and synchronous readiness probes."""

from __future__ import annotations

import re
import signal
from urllib.parse import urlsplit

import httpx

ATLAS_URL_PATTERN = re.compile(
    r"https?://(?:localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\])(?::\d+)?"
    r"(?:/[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)?"
)
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
ATLAS_SERVER_READY_TIMEOUT_SECONDS = 60
ATLAS_READINESS_TIMEOUT = httpx.Timeout(connect=0.5, pool=0.5, write=2.0, read=2.0)


def normalize_atlas_url(url: str) -> str:
    """Normalize wildcard hosts to a browser-reachable loopback URL."""
    cleaned = ANSI_ESCAPE_PATTERN.sub("", url).strip().rstrip(".,);")
    return cleaned.replace("://0.0.0.0", "://127.0.0.1")


def atlas_url_target(url: str) -> tuple[str, int] | None:
    """Return the normalized IPv4 loopback host and port for an Atlas URL."""
    parsed = urlsplit(url)
    host = parsed.hostname
    port = parsed.port
    if host not in {"localhost", "127.0.0.1", "0.0.0.0"} or port is None:
        return None
    return "127.0.0.1", port


def atlas_http_is_ready(client: httpx.Client, url: str) -> bool:
    """Return whether the Atlas page and metadata endpoint both respond."""
    if atlas_url_target(url) is None:
        return False
    try:
        root_response = client.get(f"{url.rstrip('/')}/")
        if root_response.status_code != 200:
            return False
        metadata_response = client.get(f"{url.rstrip('/')}/data/metadata.json")
        return metadata_response.status_code == 200
    except httpx.HTTPError:
        return False


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

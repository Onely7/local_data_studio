"""Browser-safe port allocation for local Embedding Atlas servers."""

from __future__ import annotations

import socket
import threading
from dataclasses import replace

from fastapi import HTTPException

from .contracts import AtlasOptions

# Chromium blocks these ports for HTTP(S), even on loopback. Keep the Atlas
# link browser-reachable instead of asking users to weaken browser security.
BROWSER_RESTRICTED_PORTS = frozenset(
    {
        0,
        1,
        7,
        9,
        11,
        13,
        15,
        17,
        19,
        20,
        21,
        22,
        23,
        25,
        37,
        42,
        43,
        53,
        69,
        77,
        79,
        87,
        95,
        101,
        102,
        103,
        104,
        109,
        110,
        111,
        113,
        115,
        117,
        119,
        123,
        135,
        137,
        139,
        143,
        161,
        179,
        389,
        427,
        465,
        512,
        513,
        514,
        515,
        526,
        530,
        531,
        532,
        540,
        548,
        554,
        556,
        563,
        587,
        601,
        636,
        989,
        990,
        993,
        995,
        1719,
        1720,
        1723,
        2049,
        3659,
        4045,
        4190,
        5060,
        5061,
        6000,
        6566,
        6665,
        6666,
        6667,
        6668,
        6669,
        6697,
        10080,
    }
)
ATLAS_PORT_LOCK = threading.Lock()
ATLAS_PORT_STATE: dict[str, int] = {}


def is_browser_safe_port(port: int) -> bool:
    """Return whether Chromium-family browsers permit HTTP on ``port``."""
    return 1 <= port <= 65535 and port not in BROWSER_RESTRICTED_PORTS


def _port_is_available(host: str, port: int) -> bool:
    bind_host = host.strip("[]")
    if bind_host == "localhost":
        bind_host = "127.0.0.1"
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind((bind_host, port))
    except OSError:
        return False
    return True


def reserve_atlas_start_port(options: AtlasOptions) -> AtlasOptions:
    """Reserve the next browser-safe, currently available Atlas port.

    The reservation is process-local and closes its probe socket immediately.
    Embedding Atlas is launched with auto-port disabled directly afterwards.

    Raises:
        HTTPException: No usable port exists at or above the configured port.
    """
    with ATLAS_PORT_LOCK:
        state_key = f"{options.host}:{options.port}"
        candidate = max(options.port, ATLAS_PORT_STATE.get(state_key, options.port))
        while candidate <= 65535:
            if is_browser_safe_port(candidate) and _port_is_available(options.host, candidate):
                ATLAS_PORT_STATE[state_key] = candidate + 1
                return replace(options, port=candidate)
            candidate += 1
    raise HTTPException(status_code=500, detail=f"no browser-safe Atlas port is available from {options.port}")

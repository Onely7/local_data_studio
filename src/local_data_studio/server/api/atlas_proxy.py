"""Same-origin streaming reverse proxy for registered Atlas instances."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, HTTPException, Request
from starlette.background import BackgroundTask
from starlette.responses import RedirectResponse, StreamingResponse

from ..atlas_components.runtime import AtlasInstance, AtlasRuntime, AtlasRuntimeError

router = APIRouter()

INSTANCE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{32,64}$")
HOP_BY_HOP_HEADERS = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}
PRIVATE_REQUEST_HEADERS = {b"authorization", b"cookie", b"host", b"content-length"}
PRIVATE_RESPONSE_HEADERS = {b"set-cookie"}
PROXY_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]


def _runtime(request: Request) -> AtlasRuntime:
    return request.app.state.atlas_runtime


def _connection_tokens(headers: Iterable[tuple[bytes, bytes]]) -> set[bytes]:
    tokens: set[bytes] = set()
    for name, value in headers:
        if name.lower() == b"connection":
            tokens.update(token.strip().lower() for token in value.split(b",") if token.strip())
    return tokens


def _filtered_headers(
    headers: Iterable[tuple[bytes, bytes]],
    *,
    private: set[bytes],
) -> list[tuple[bytes, bytes]]:
    raw_headers = list(headers)
    blocked = HOP_BY_HOP_HEADERS | private | _connection_tokens(raw_headers)
    return [(name, value) for name, value in raw_headers if name.lower() not in blocked]


def _resolve_instance(runtime: AtlasRuntime, instance_id: str) -> AtlasInstance:
    if INSTANCE_ID_PATTERN.fullmatch(instance_id) is None:
        raise HTTPException(status_code=404, detail="Atlas instance not found")
    instance = runtime.resolve(instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Atlas instance not found")
    return instance


def _upstream_url(request: Request, instance: AtlasInstance) -> httpx.URL:
    raw_path = request.scope.get("raw_path")
    if not isinstance(raw_path, bytes):
        raw_path = request.url.path.encode("utf-8")
    prefix = f"/atlas/{instance.instance_id}".encode()
    if not raw_path.startswith(prefix):
        raise HTTPException(status_code=404, detail="Atlas instance not found")
    target_path = raw_path[len(prefix) :] or b"/"
    if not target_path.startswith(b"/"):
        raise HTTPException(status_code=400, detail="Invalid Atlas proxy path")
    query = request.scope.get("query_string", b"")
    raw_target = target_path + (b"?" + query if query else b"")
    # The host is intentionally constant: request data and child output cannot
    # turn this route into a general-purpose server-side request proxy.
    return httpx.URL(scheme="http", host="127.0.0.1", port=instance.port, raw_path=raw_target)


def _rewrite_location(value: bytes, *, upstream_url: httpx.URL, instance: AtlasInstance) -> bytes:
    location = value.decode("latin-1")
    resolved = urljoin(str(upstream_url), location)
    parsed = urlsplit(resolved)
    upstream = urlsplit(str(upstream_url))
    if (parsed.scheme, parsed.hostname, parsed.port) != (upstream.scheme, upstream.hostname, upstream.port):
        return value
    path = parsed.path.lstrip("/")
    proxy_path = instance.proxy_path + path
    rewritten = urlunsplit(("", "", proxy_path, parsed.query, parsed.fragment))
    return rewritten.encode("latin-1")


async def _response_body(upstream: httpx.Response) -> AsyncIterator[bytes]:
    try:
        async for chunk in upstream.aiter_raw():
            yield chunk
    finally:
        await upstream.aclose()


@router.delete("/api/atlas/instances/{instance_id}")
def stop_atlas_instance(instance_id: str, request: Request) -> dict[str, object]:
    """Stop one live Atlas child selected by its opaque instance identifier."""
    runtime = _runtime(request)
    _resolve_instance(runtime, instance_id)
    if not runtime.stop(instance_id):
        raise HTTPException(status_code=404, detail="Atlas instance not found")
    return {"instance_id": instance_id, "stopped": True}


@router.api_route("/atlas/{instance_id}", methods=PROXY_METHODS, include_in_schema=False)
async def redirect_atlas_root(instance_id: str, request: Request) -> RedirectResponse:
    """Canonicalize Atlas roots so relative frontend assets resolve correctly."""
    _resolve_instance(_runtime(request), instance_id)
    query = request.scope.get("query_string", b"")
    suffix = f"?{query.decode('latin-1')}" if query else ""
    return RedirectResponse(f"/atlas/{instance_id}/{suffix}", status_code=307)


@router.api_route("/atlas/{instance_id}/{path:path}", methods=PROXY_METHODS, include_in_schema=False)
async def proxy_atlas(instance_id: str, path: str, request: Request) -> StreamingResponse:
    """Stream one HTTP request to a registered loopback Atlas process."""
    del path  # Routing uses the decoded value; forwarding uses ASGI raw_path.
    runtime = _runtime(request)
    instance = _resolve_instance(runtime, instance_id)
    upstream_url = _upstream_url(request, instance)
    request_headers = _filtered_headers(request.headers.raw, private=PRIVATE_REQUEST_HEADERS)
    content = request.stream() if request.method not in {"GET", "HEAD"} else None
    upstream_request = runtime.proxy_client.build_request(
        request.method,
        upstream_url,
        headers=request_headers,
        content=content,
    )
    try:
        upstream = await runtime.proxy_client.send(upstream_request, stream=True)
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Atlas did not respond before the proxy timeout") from exc
    except (httpx.RequestError, AtlasRuntimeError) as exc:
        raise HTTPException(status_code=502, detail="Atlas is not reachable through the local proxy") from exc

    response_headers = _filtered_headers(upstream.headers.raw, private=PRIVATE_RESPONSE_HEADERS)
    response_headers = [
        (name, _rewrite_location(value, upstream_url=upstream_url, instance=instance) if name.lower() == b"location" else value)
        for name, value in response_headers
    ]
    response_headers.append((b"referrer-policy", b"no-referrer"))
    response = StreamingResponse(
        _response_body(upstream),
        status_code=upstream.status_code,
        background=BackgroundTask(upstream.aclose),
    )
    response.raw_headers = response_headers
    return response

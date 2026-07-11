"""Static file mounting for the assembled FastAPI application."""

import os
from os import PathLike

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response
from starlette.types import Scope

from ..config import CACHE_DIR, DATA_SERVE_ROOT, PACKAGE_DIR


class NoCacheStaticFiles(StaticFiles):
    """Static file handler that prevents stale packaged UI assets."""

    def file_response(
        self,
        full_path: str | PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        """Build a response and force browser revalidation on every request."""
        response = super().file_response(full_path, stat_result, scope, status_code)
        response.headers["Cache-Control"] = "no-store, max-age=0"
        return response


def mount_static_files(app: FastAPI) -> None:
    """Mount data/cache before the root static catch-all."""
    app.mount("/data", StaticFiles(directory=str(DATA_SERVE_ROOT), check_dir=False), name="data")
    app.mount("/cache", StaticFiles(directory=str(CACHE_DIR), check_dir=False), name="cache")
    app.mount("/", NoCacheStaticFiles(directory=str(PACKAGE_DIR / "static"), html=True), name="static")

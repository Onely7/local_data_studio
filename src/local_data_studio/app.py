"""FastAPI application assembly for Local Data Studio."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .server.api.analysis import column_stats, count_rows, llm_models, nl_query, run_eda, run_query, search, translation_languages
from .server.api.analysis import router as analysis_router
from .server.api.atlas_proxy import router as atlas_proxy_router
from .server.api.atlas_proxy import stop_atlas_instance
from .server.api.datasets import (
    column_sample,
    embedder_models,
    get_config,
    get_schema,
    list_files,
    preview,
    raw_file,
    raw_row,
    upload_files,
)
from .server.api.datasets import (
    router as datasets_router,
)
from .server.api.jobs import (
    cancel_job,
    get_job,
    start_atlas_job,
    start_atlas_query_job,
    start_count_job,
    start_eda_job,
    start_eda_query_job,
    start_index_job,
    start_query_job,
    start_search_job,
    start_stats_job,
    start_translation_job,
)
from .server.api.jobs import (
    router as jobs_router,
)
from .server.api.mutations import delete_column, delete_row
from .server.api.mutations import router as mutations_router
from .server.api.schemas import (
    AtlasQueryRequest,
    AtlasRequest,
    CountJobRequest,
    DeleteColumnRequest,
    DeleteRowRequest,
    EdaQueryRequest,
    EdaRequest,
    IndexJobRequest,
    NLQueryRequest,
    QueryRequest,
    RawRowRequest,
    SearchJobRequest,
    StatsJobRequest,
    TranslationRequest,
)
from .server.api.static import NoCacheStaticFiles, mount_static_files
from .server.atlas_components.runtime import AtlasRuntime
from .server.config import ATLAS_MAX_INSTANCES
from .server.jobs import JobStore


def create_app(*, job_store: JobStore | None = None, atlas_runtime: AtlasRuntime | None = None) -> FastAPI:
    """Assemble the application with API routes before static catch-all mounts.

    Returns:
        A new application instance. Callers own the instance and may add routes.
    """
    owned_job_store = job_store or JobStore()
    owned_atlas_runtime = atlas_runtime or AtlasRuntime(max_instances=ATLAS_MAX_INSTANCES)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await owned_atlas_runtime.start_proxy_client()
        try:
            yield
        finally:
            owned_atlas_runtime.begin_shutdown()
            owned_job_store.begin_shutdown()
            await asyncio.to_thread(owned_job_store.wait_for_idle, 10.0)
            await asyncio.to_thread(owned_atlas_runtime.terminate_all)
            await owned_atlas_runtime.close_proxy_client()
            owned_job_store.shutdown(wait_timeout=0.0)

    application = FastAPI(title="Data Viewer", lifespan=lifespan)
    application.state.job_store = owned_job_store
    application.state.atlas_runtime = owned_atlas_runtime
    application.include_router(datasets_router)
    application.include_router(analysis_router)
    application.include_router(jobs_router)
    application.include_router(mutations_router)
    application.include_router(atlas_proxy_router)
    mount_static_files(application)
    return application


app = create_app()


__all__ = [
    "AtlasQueryRequest",
    "AtlasRequest",
    "CountJobRequest",
    "DeleteColumnRequest",
    "DeleteRowRequest",
    "EdaQueryRequest",
    "EdaRequest",
    "IndexJobRequest",
    "NLQueryRequest",
    "NoCacheStaticFiles",
    "QueryRequest",
    "RawRowRequest",
    "SearchJobRequest",
    "StatsJobRequest",
    "TranslationRequest",
    "app",
    "cancel_job",
    "column_sample",
    "column_stats",
    "count_rows",
    "create_app",
    "delete_column",
    "delete_row",
    "embedder_models",
    "get_config",
    "get_job",
    "get_schema",
    "list_files",
    "llm_models",
    "nl_query",
    "preview",
    "raw_file",
    "raw_row",
    "run_eda",
    "run_query",
    "search",
    "start_atlas_job",
    "start_atlas_query_job",
    "start_count_job",
    "start_eda_job",
    "start_eda_query_job",
    "start_index_job",
    "start_query_job",
    "start_search_job",
    "start_stats_job",
    "start_translation_job",
    "stop_atlas_instance",
    "translation_languages",
    "upload_files",
]

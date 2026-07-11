"""FastAPI application assembly for Local Data Studio."""

from fastapi import FastAPI

from .server.api.analysis import column_stats, count_rows, nl_query, run_eda, run_query, search
from .server.api.analysis import router as analysis_router
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
)
from .server.api.static import NoCacheStaticFiles, mount_static_files


def create_app() -> FastAPI:
    application = FastAPI(title="Data Viewer")
    application.include_router(datasets_router)
    application.include_router(analysis_router)
    application.include_router(jobs_router)
    application.include_router(mutations_router)
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
    "upload_files",
]

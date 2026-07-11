"""Background job API endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException

from ..cache import count_cache_path, search_cache_path
from ..db import normalize_pagination
from ..deleted_rows import deleted_row_ids_for
from ..files import resolve_data_file
from ..jobs import JOB_STORE, JobContext
from ..readers import build_line_index_with_progress, count_rows_with_progress, search_dataset
from ..sql import execute_query_guarded
from .schemas import (
    AtlasQueryRequest,
    AtlasRequest,
    CountJobRequest,
    EdaQueryRequest,
    EdaRequest,
    IndexJobRequest,
    QueryRequest,
    SearchJobRequest,
    StatsJobRequest,
)
from .services import atlas_service, compute_cached_column_stats, eda_reports_service, load_cached_result, write_cached_result

router = APIRouter()


@router.post("/api/jobs/count")
def start_count_job(payload: CountJobRequest) -> dict[str, Any]:
    """Submit a cancellable row-count job with fingerprinted cache reuse."""
    path = resolve_data_file(payload.file)
    deleted_ids = deleted_row_ids_for(path)

    def _work(context: JobContext) -> dict[str, Any]:
        cache_path = count_cache_path(path, deleted_ids=deleted_ids)
        cached = load_cached_result(cache_path)
        if cached is not None and isinstance(cached.get("count"), int):
            context.update(progress=1.0, message="Count loaded from cache")
            return {**cached, "cached": True}
        result = {"file": payload.file, "count": count_rows_with_progress(path, context, deleted_ids=deleted_ids)}
        write_cached_result(cache_path, result)
        return {**result, "cached": False}

    return JOB_STORE.submit("count", _work).to_response()


@router.post("/api/jobs/index")
def start_index_job(payload: IndexJobRequest) -> dict[str, Any]:
    """Submit incremental sparse-index construction for a line dataset."""
    path = resolve_data_file(payload.file)

    def _work(context: JobContext) -> dict[str, Any]:
        return {"file": payload.file, **build_line_index_with_progress(path, context)}

    return JOB_STORE.submit("index", _work).to_response()


@router.post("/api/jobs/search")
def start_search_job(payload: SearchJobRequest) -> dict[str, Any]:
    """Submit a cancellable bounded search with fingerprinted cache reuse."""
    path = resolve_data_file(payload.file)
    deleted_ids = deleted_row_ids_for(path)
    search_term = payload.query.strip()
    limit_value, _ = normalize_pagination(payload.limit, 0)

    def _work(context: JobContext) -> dict[str, Any]:
        cache_path = search_cache_path(path, query=search_term, limit=limit_value, deleted_ids=deleted_ids)
        cached = load_cached_result(cache_path)
        if cached is not None and cached.get("query") == search_term:
            context.update(progress=1.0, message="Search loaded from cache")
            return {**cached, "cached": True}
        result = search_dataset(payload.file, path, query=search_term, limit=limit_value, control=context, deleted_ids=deleted_ids)
        write_cached_result(cache_path, result)
        return {**result, "cached": False}

    return JOB_STORE.submit("search", _work).to_response()


@router.post("/api/jobs/query")
def start_query_job(payload: QueryRequest) -> dict[str, Any]:
    """Submit validated read-only SQL under query resource limits."""
    path = resolve_data_file(payload.file)

    def _work(context: JobContext) -> dict[str, Any]:
        return execute_query_guarded(
            file_name=payload.file,
            path=path,
            sql=payload.sql,
            limit=payload.limit,
            offset=payload.offset,
            context=context,
        )

    return JOB_STORE.submit("query", _work).to_response()


@router.post("/api/jobs/stats")
def start_stats_job(payload: StatsJobRequest) -> dict[str, Any]:
    """Submit bounded column sampling with optional cache refresh."""
    path = resolve_data_file(payload.file)

    def _work(context: JobContext) -> dict[str, Any]:
        context.update(progress=0.15, message="Sampling rows")
        result = compute_cached_column_stats(payload.file, path, payload.sample, bool(payload.force))
        context.update(progress=1.0, message="Stats ready")
        return result

    return JOB_STORE.submit("stats", _work).to_response()


@router.post("/api/jobs/eda")
def start_eda_job(payload: EdaRequest) -> dict[str, Any]:
    """Submit EDA generation for a bounded dataset sample."""
    path = resolve_data_file(payload.file)
    reports = eda_reports_service()

    def _work(context: JobContext) -> dict[str, Any]:
        return reports.generate_dataset_eda_report(
            file_name=payload.file,
            path=path,
            sample=payload.sample,
            mode=payload.mode,
            force=payload.force,
            context=context,
        )

    return JOB_STORE.submit("eda", _work).to_response()


@router.post("/api/jobs/eda_query")
def start_eda_query_job(payload: EdaQueryRequest) -> dict[str, Any]:
    """Submit EDA generation for validated SQL query results."""
    path = resolve_data_file(payload.file)
    reports = eda_reports_service()

    def _work(context: JobContext) -> dict[str, Any]:
        return reports.generate_query_eda_report(
            file_name=payload.file,
            path=path,
            sql=payload.sql,
            sample=payload.sample,
            mode=payload.mode,
            force=payload.force,
            context=context,
        )

    return JOB_STORE.submit("eda_query", _work).to_response()


@router.post("/api/jobs/atlas")
def start_atlas_job(payload: AtlasRequest) -> dict[str, Any]:
    """Submit embedding and Atlas startup for a dataset column."""
    path = resolve_data_file(payload.file)
    atlas = atlas_service()

    def _work(context: JobContext) -> dict[str, Any]:
        return atlas.run_atlas_visualization(
            file_name=payload.file,
            path=path,
            column=payload.column,
            model=payload.model,
            sql=None,
            sample=payload.sample,
            context=context,
        )

    return JOB_STORE.submit("atlas", _work).to_response()


@router.post("/api/jobs/atlas_query")
def start_atlas_query_job(payload: AtlasQueryRequest) -> dict[str, Any]:
    """Submit embedding and Atlas startup for validated SQL results."""
    path = resolve_data_file(payload.file)
    atlas = atlas_service()

    def _work(context: JobContext) -> dict[str, Any]:
        return atlas.run_atlas_visualization(
            file_name=payload.file,
            path=path,
            column=payload.column,
            model=payload.model,
            sql=payload.sql,
            sample=payload.sample,
            context=context,
        )

    return JOB_STORE.submit("atlas_query", _work).to_response()


@router.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    """Return a detached snapshot of a job, or HTTP 404 when unknown."""
    record = JOB_STORE.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    return record.to_response()


@router.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> dict[str, Any]:
    """Request cooperative cancellation, or return HTTP 404 when unknown."""
    record = JOB_STORE.cancel(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    return record.to_response()

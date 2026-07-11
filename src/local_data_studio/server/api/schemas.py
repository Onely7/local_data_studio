"""Request schemas shared by Local Data Studio API routers."""

from typing import Any

from pydantic import BaseModel


class QueryRequest(BaseModel):
    """Input for a guarded read-only SQL query."""

    file: str
    sql: str
    limit: int | None = None
    offset: int | None = None


class RawRowRequest(BaseModel):
    """Input selecting either a one-based dataset row or SQL result offset."""

    file: str
    row_id: int | None = None
    sql: str | None = None
    offset: int | None = None


class EdaRequest(BaseModel):
    """Input controlling bounded EDA generation and report cache reuse."""

    file: str
    sample: int | None = None
    force: bool | None = None
    mode: str | None = None


class EdaQueryRequest(EdaRequest):
    """EDA input extended with the guarded SQL that supplies report rows."""

    sql: str


class AtlasRequest(BaseModel):
    """Input selecting a dataset column and local model for Atlas embedding."""

    file: str
    column: str
    model: str
    sample: int | None = None


class AtlasQueryRequest(AtlasRequest):
    """Atlas input extended with the guarded SQL that supplies embedding rows."""

    sql: str


class CountJobRequest(BaseModel):
    """Input for a background row-count operation."""

    file: str


class IndexJobRequest(BaseModel):
    """Input for incremental sparse-index construction."""

    file: str


class SearchJobRequest(BaseModel):
    """Input for a bounded background dataset search."""

    file: str
    query: str
    limit: int | None = None


class StatsJobRequest(BaseModel):
    """Input controlling sampled statistics and cache refresh."""

    file: str
    sample: int | None = None
    force: bool | None = None


class DeleteRowRequest(BaseModel):
    """Input for one-based soft or persistent row deletion."""

    file: str
    row_id: int
    persist: bool | None = None


class DeleteColumnRequest(BaseModel):
    """Input for soft or persistent column deletion."""

    file: str
    column: str
    persist: bool | None = None


class NLQueryRequest(BaseModel):
    """Input for natural-language SQL generation with optional sample context."""

    file: str
    prompt: str
    sample: dict[str, Any] | None = None

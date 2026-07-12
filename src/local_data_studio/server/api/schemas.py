"""Request schemas shared by Local Data Studio API routers."""

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


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
    mode: Literal["minimal", "maximal"] | None = None

    @field_validator("sample")
    @classmethod
    def validate_sample(cls, value: int | None) -> int | None:
        """Accept the unlimited marker or a positive row limit."""
        if value is None or value == -1 or value >= 1:
            return value
        raise ValueError("sample must be -1 or an integer greater than or equal to 1")


class EdaQueryRequest(EdaRequest):
    """EDA input extended with the guarded SQL that supplies report rows."""

    sql: str


class AtlasRequest(BaseModel):
    """Input selecting a dataset column and local model for Atlas embedding."""

    file: str
    column: str
    model: str
    sample: int | None = Field(default=None, ge=0)
    backend: Literal["transformers", "sentence-transformers"] | None = None
    prompt: str | None = Field(default=None, max_length=16_384)
    projection_method: Literal["umap", "tsne", "pca"] = "umap"


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
    prompt: str = Field(max_length=16_384)
    sample: dict[str, Any] | None = None
    model: str | None = Field(default=None, max_length=64)

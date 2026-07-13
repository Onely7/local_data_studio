"""Request schemas shared by Local Data Studio API routers."""

from typing import Any, Literal, Self

from pydantic import BaseModel, Field, JsonValue, field_validator, model_validator


class QueryRequest(BaseModel):
    """Input for a guarded read-only SQL query."""

    file: str = Field(min_length=1)
    sql: str = Field(min_length=1)
    limit: int | None = Field(default=None, ge=1)
    offset: int | None = Field(default=None, ge=0)


class RawRowRequest(BaseModel):
    """Input selecting either a one-based dataset row or SQL result offset."""

    file: str = Field(min_length=1)
    row_id: int | None = Field(default=None, ge=1)
    sql: str | None = None
    offset: int | None = Field(default=None, ge=0)


class EdaRequest(BaseModel):
    """Input controlling bounded EDA generation and report cache reuse."""

    file: str = Field(min_length=1)
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

    sql: str = Field(min_length=1)


class AtlasRequest(BaseModel):
    """Input selecting a dataset column and local model for Atlas embedding."""

    file: str = Field(min_length=1)
    column: str = Field(min_length=1)
    model: str = Field(min_length=1)
    sample: int | None = Field(default=None, ge=0)
    backend: Literal["transformers", "sentence-transformers"] | None = None
    prompt: str | None = Field(default=None, max_length=16_384)
    projection_method: Literal["umap", "tsne", "pca"] = "umap"


class AtlasQueryRequest(AtlasRequest):
    """Atlas input extended with the guarded SQL that supplies embedding rows."""

    sql: str = Field(min_length=1)


class CountJobRequest(BaseModel):
    """Input for a background row-count operation."""

    file: str = Field(min_length=1)


class IndexJobRequest(BaseModel):
    """Input for incremental sparse-index construction."""

    file: str = Field(min_length=1)


class SearchJobRequest(BaseModel):
    """Input for a bounded background dataset search."""

    file: str = Field(min_length=1)
    query: str = Field(min_length=1)
    limit: int | None = Field(default=None, ge=1)


class StatsJobRequest(BaseModel):
    """Input controlling sampled statistics and cache refresh."""

    file: str = Field(min_length=1)
    sample: int | None = None
    force: bool | None = None


class DeleteRowRequest(BaseModel):
    """Input for one-based soft or persistent row deletion."""

    file: str = Field(min_length=1)
    row_id: int = Field(ge=1)
    persist: bool | None = None


class DeleteColumnRequest(BaseModel):
    """Input for soft or persistent column deletion."""

    file: str = Field(min_length=1)
    column: str = Field(min_length=1)
    persist: bool | None = None


class NLQueryRequest(BaseModel):
    """Input for natural-language SQL generation with optional sample context."""

    file: str = Field(min_length=1)
    prompt: str = Field(min_length=1, max_length=16_384)
    sample: dict[str, Any] | None = None
    model: str | None = Field(default=None, max_length=64)


class TranslationItem(BaseModel):
    """One browser-loaded JSON value identified for translation."""

    id: str = Field(min_length=1, max_length=128)
    value: JsonValue


class TranslationRequest(BaseModel):
    """Input for a cancellable translation of visible preview values."""

    model: str | None = Field(default=None, max_length=64)
    target_language: str = Field(min_length=2, max_length=16)
    column_name: str = Field(min_length=1, max_length=512)
    items: list[TranslationItem] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_item_ids(self) -> Self:
        """Require stable one-to-one IDs for translated result mapping."""
        ids = [item.id for item in self.items]
        if len(ids) != len(set(ids)):
            raise ValueError("translation item ids must be unique")
        return self

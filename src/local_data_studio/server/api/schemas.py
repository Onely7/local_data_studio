"""Request schemas shared by Local Data Studio API routers."""

from typing import Any

from pydantic import BaseModel


class QueryRequest(BaseModel):
    file: str
    sql: str
    limit: int | None = None
    offset: int | None = None


class RawRowRequest(BaseModel):
    file: str
    row_id: int | None = None
    sql: str | None = None
    offset: int | None = None


class EdaRequest(BaseModel):
    file: str
    sample: int | None = None
    force: bool | None = None
    mode: str | None = None


class EdaQueryRequest(EdaRequest):
    sql: str


class AtlasRequest(BaseModel):
    file: str
    column: str
    model: str
    sample: int | None = None


class AtlasQueryRequest(AtlasRequest):
    sql: str


class CountJobRequest(BaseModel):
    file: str


class IndexJobRequest(BaseModel):
    file: str


class SearchJobRequest(BaseModel):
    file: str
    query: str
    limit: int | None = None


class StatsJobRequest(BaseModel):
    file: str
    sample: int | None = None
    force: bool | None = None


class DeleteRowRequest(BaseModel):
    file: str
    row_id: int
    persist: bool | None = None


class DeleteColumnRequest(BaseModel):
    file: str
    column: str
    persist: bool | None = None


class NLQueryRequest(BaseModel):
    file: str
    prompt: str
    sample: dict[str, Any] | None = None

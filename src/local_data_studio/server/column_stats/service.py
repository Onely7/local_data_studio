"""Dataset sampling and response construction for column statistics."""

from pathlib import Path
from typing import Any

from ..config import DEFAULT_SAMPLE, MAX_SAMPLE
from ..db import describe_relation, open_connection, relation_sql
from .accumulator import ColumnSampleAccumulator

STATS_FETCH_BATCH_SIZE = 1_024


def compute_column_stats(file: str, path: Path, sample: int | None) -> dict[str, Any]:
    """Compute column summaries without retaining the sampled row matrix.

    At most the configured sample size is read. Values are fetched from DuckDB in
    fixed batches and transferred into per-column accumulators; the connection and
    cursor remain owned by this function and are closed before the response returns.

    Args:
        file: User-facing allowlisted dataset name copied into the response.
        path: Resolved dataset path accepted by the DuckDB relation layer.
        sample: Requested row bound; values are clamped between 50 and ``MAX_SAMPLE``.

    Returns:
        A new JSON-compatible mapping containing ordered column summaries.

    Raises:
        OSError: The dataset cannot be opened.
        duckdb.Error: DuckDB cannot describe or sample the dataset.
    """
    relation, params = relation_sql(path)
    sample_size = max(50, min(MAX_SAMPLE, sample or DEFAULT_SAMPLE))

    with open_connection() as connection:
        column_info = describe_relation(connection, relation, params)
        column_types = {item["name"]: item["type"] for item in column_info}
        result = connection.execute(f"SELECT * FROM {relation} LIMIT {sample_size}", params)
        description = result.description or []
        column_names = [item[0] for item in description]
        accumulators = [ColumnSampleAccumulator() for _ in column_names]
        sampled_rows = 0
        while rows := result.fetchmany(STATS_FETCH_BATCH_SIZE):
            sampled_rows += len(rows)
            for row in rows:
                for accumulator, value in zip(accumulators, row):
                    accumulator.add(value)

    if sampled_rows == 0:
        return {"file": file, "columns": [], "sample": 0}
    stats = [accumulator.to_response(name, column_types.get(name, "")) for name, accumulator in zip(column_names, accumulators)]
    return {"file": file, "columns": stats, "sample": sampled_rows}

"""Tests for eda behavior."""

import os
from importlib.metadata import requires
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pydantic import ValidationError

from local_data_studio.server import eda, eda_reports
from local_data_studio.server.config import Settings
from local_data_studio.server.eda import build_eda_report, eda_cache_key, load_eda_dataframe, sanitize_eda_dataframe
from local_data_studio.server.eda_reports import EdaReportOptions, generate_dataset_eda_report


def test_runtime_dependencies_keep_pkg_resources_available_for_eda() -> None:
    """Verify that runtime dependencies keep pkg resources available for eda."""
    project_requirements = requires("local-data-studio") or []
    setuptools_requirement = next(requirement for requirement in project_requirements if requirement.lower().startswith("setuptools"))

    assert ">=80" in setuptools_requirement
    assert "<81" in setuptools_requirement


def test_sanitize_eda_dataframe_normalizes_nested_and_binary_values() -> None:
    """Verify that sanitize eda dataframe normalizes nested and binary values."""
    frame = pd.DataFrame(
        {
            "number": [1, 2],
            "image": [b"\x89PNG", b"\x00\x01"],
            "metadata": [{"label": "cat"}, {"label": "dog"}],
        }
    )

    sanitized = sanitize_eda_dataframe(frame)

    assert sanitized["number"].tolist() == [1, 2]
    assert sanitized["image"].tolist() == ["89504e47", "0001"]
    assert sanitized["metadata"].tolist() == ['{"label": "cat"}', '{"label": "dog"}']


def test_eda_cache_key_changes_with_ydata_backend_version(tmp_path: Path) -> None:
    """Verify that eda cache key changes with ydata backend version."""
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("value\n1\n", encoding="utf-8")

    assert eda_cache_key(dataset, 100, "minimal") != eda_cache_key(dataset, 200, "minimal")


def test_build_eda_report_uses_ydata_minimal_option() -> None:
    """Verify that build eda report uses ydata minimal option."""
    frame = pd.DataFrame({"value": [1, 2]})

    with patch("local_data_studio.server.eda.ProfileReport") as profile_report:
        build_eda_report(frame, title="Example", minimal=True)

    profile_report.assert_called_once_with(frame, title="Example", minimal=True)


@pytest.mark.parametrize("row_limit", [1, 50_001, -1])
def test_eda_row_limit_accepts_positive_values_and_unlimited(row_limit: int) -> None:
    """Accept any positive environment limit and the explicit unlimited marker."""
    settings = Settings(_env_file=None, EDA_ROW_LIMIT=row_limit)

    assert settings.default_eda_sample == row_limit
    assert EdaReportOptions.from_request(sample=row_limit, mode="minimal", force=False).sample == row_limit


@pytest.mark.parametrize("row_limit", [0, -2])
def test_eda_row_limit_rejects_other_non_positive_values(row_limit: int) -> None:
    """Reject ambiguous or unsupported non-positive EDA row limits."""
    with pytest.raises(ValidationError, match="EDA_ROW_LIMIT must be -1"):
        Settings(_env_file=None, EDA_ROW_LIMIT=row_limit)


def test_unlimited_eda_loads_the_complete_dataset(tmp_path: Path) -> None:
    """Omit the SQL limit when EDA_ROW_LIMIT uses the unlimited marker."""
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("value\n1\n2\n3\n", encoding="utf-8")

    frame = load_eda_dataframe(dataset, -1, [])

    assert frame["value"].tolist() == [1, 2, 3]


def test_bounded_eda_load_applies_limit_before_row_numbering(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid a full source scan for normal EDA samples without hidden rows."""
    dataset = tmp_path / "dataset.jsonl"
    dataset.write_text('{"value": 1}\n', encoding="utf-8")
    queries: list[str] = []

    class FakeResult:
        """Return a minimal DataFrame from the recorded query."""

        def df(self) -> pd.DataFrame:
            """Return the test result without invoking DuckDB."""
            return pd.DataFrame({"value": [1]})

    class FakeConnection:
        """Record generated EDA SQL for its query-shape contract."""

        def __enter__(self) -> "FakeConnection":
            """Provide the connection inside the EDA context manager."""
            return self

        def __exit__(self, *_: object) -> None:
            """Close the no-op test connection."""

        def execute(self, query: str, _: list[object]) -> FakeResult:
            """Record the query and return a bounded fake result."""
            queries.append(query)
            return FakeResult()

    def fake_relation_sql(_: Path) -> tuple[str, list[str]]:
        """Return the synthetic source relation used by this contract test."""
        return "read_json_auto(?)", [str(dataset)]

    def fail_rowid_relation(*_: object) -> tuple[str, list[object]]:
        """Fail if the bounded no-delete path tries to number every row."""
        pytest.fail("row IDs are unnecessary without hidden rows")

    monkeypatch.setattr(eda, "open_connection", FakeConnection)
    monkeypatch.setattr(eda, "relation_sql", fake_relation_sql)
    monkeypatch.setattr(eda, "relation_with_rowid_sql", fail_rowid_relation)

    frame = load_eda_dataframe(dataset, 100_000, [])

    assert frame["value"].tolist() == [1]
    assert queries == ["SELECT * FROM read_json_auto(?) LIMIT 100000"]


def test_eda_reports_use_dedicated_cache_and_prune_oldest(tmp_path: Path) -> None:
    """Store report files under cache/eda and retain the newest artifact within capacity."""
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("value\n1\n", encoding="utf-8")
    cache_dir = tmp_path / "cache" / "eda"

    class FakeReport:
        """Write a deterministic report body without invoking YData Profiling."""

        def to_file(self, target: str) -> None:
            """Create a small report artifact for pruning assertions."""
            Path(target).write_bytes(b"report-data")

    with (
        patch.object(eda, "EDA_CACHE_DIR", cache_dir),
        patch.object(eda_reports, "CACHE_DIR", cache_dir.parent),
        patch.object(eda_reports, "EDA_CACHE_DIR", cache_dir),
        patch.object(eda_reports, "EDA_CACHE_MAX_BYTES", 15),
        patch.object(eda_reports, "load_eda_dataframe", return_value=pd.DataFrame({"value": [1]})),
        patch.object(eda_reports, "build_eda_report", return_value=FakeReport()),
    ):
        first = generate_dataset_eda_report(
            file_name="dataset.csv",
            path=dataset,
            sample=100,
            mode="minimal",
            force=True,
        )
        first_path = cache_dir / Path(first["url"]).name
        os.utime(first_path, (1_700_000_000, 1_700_000_000))
        second = generate_dataset_eda_report(
            file_name="dataset.csv",
            path=dataset,
            sample=200,
            mode="minimal",
            force=True,
        )

    second_path = cache_dir / Path(second["url"]).name
    assert first["url"].startswith("/cache/eda/")
    assert second["url"].startswith("/cache/eda/")
    assert not first_path.exists()
    assert second_path.exists()
    assert sum(path.stat().st_size for path in cache_dir.glob("*.html")) <= 15

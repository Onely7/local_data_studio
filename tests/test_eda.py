"""Tests for eda behavior."""

import os
from importlib.metadata import requires
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from local_data_studio.server import eda, eda_reports
from local_data_studio.server.eda import build_eda_report, eda_cache_key, sanitize_eda_dataframe
from local_data_studio.server.eda_reports import generate_dataset_eda_report


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

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from local_data_studio.server.eda import build_eda_report, eda_cache_key, sanitize_eda_dataframe


def test_sanitize_eda_dataframe_normalizes_nested_and_binary_values() -> None:
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
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("value\n1\n", encoding="utf-8")

    assert eda_cache_key(dataset, 100, "minimal") != eda_cache_key(dataset, 200, "minimal")


def test_build_eda_report_uses_ydata_minimal_option() -> None:
    frame = pd.DataFrame({"value": [1, 2]})

    with patch("local_data_studio.server.eda.ProfileReport") as profile_report:
        build_eda_report(frame, title="Example", minimal=True)

    profile_report.assert_called_once_with(frame, title="Example", minimal=True)

"""Deterministic row limiting for Atlas projection inputs."""

from __future__ import annotations

from typing import Any

ATLAS_SAMPLE_RANDOM_STATE = 42


def sample_atlas_frame(data_frame: Any, sample_limit: int | None) -> Any:
    """Return at most ``sample_limit`` rows using a deterministic sample.

    The input frame is returned unchanged when the limit is absent or no
    smaller than the available row count. Sampling therefore behaves as a
    maximum rather than requiring the dataset to contain exactly that many
    rows.
    """
    if not sample_limit or len(data_frame) <= sample_limit:
        return data_frame
    return data_frame.sample(n=sample_limit, axis=0, random_state=ATLAS_SAMPLE_RANDOM_STATE).reset_index(drop=True)

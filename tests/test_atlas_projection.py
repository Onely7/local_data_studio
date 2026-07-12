"""Tests for Atlas sampling and projection method contracts."""

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
from pydantic import ValidationError

from local_data_studio.server.api.schemas import AtlasRequest
from local_data_studio.server.atlas_components.contracts import AtlasOptions
from local_data_studio.server.atlas_components.projection import project_atlas_frame
from local_data_studio.server.atlas_components.reducers import reduce_embeddings
from local_data_studio.server.atlas_components.sampling import sample_atlas_frame
from local_data_studio.server.config import Settings


class AtlasSamplingTests(TestCase):
    """Test the strict Atlas projection-row limit."""

    def test_positive_limit_is_a_deterministic_maximum(self) -> None:
        """Sample at most the configured rows with a stable seed."""
        frame = pd.DataFrame({"value": range(20)})

        first = sample_atlas_frame(frame, 5)
        second = sample_atlas_frame(frame, 5)

        self.assertEqual(5, len(first))
        self.assertEqual(first["value"].tolist(), second["value"].tolist())
        self.assertEqual(list(range(5)), first.index.tolist())

    def test_limit_larger_than_input_keeps_every_row(self) -> None:
        """Keep a short query result instead of requiring the requested count."""
        frame = pd.DataFrame({"value": range(3)})

        sampled = sample_atlas_frame(frame, 10)

        self.assertIs(frame, sampled)
        self.assertEqual(3, len(sampled))

    def test_zero_or_missing_limit_is_unbounded(self) -> None:
        """Treat zero and an omitted limit as no sampling restriction."""
        frame = pd.DataFrame({"value": range(3)})

        self.assertIs(frame, sample_atlas_frame(frame, 0))
        self.assertIs(frame, sample_atlas_frame(frame, None))


class AtlasProjectionSettingsTests(TestCase):
    """Test projection request and environment validation."""

    def test_api_accepts_all_projection_methods_and_defaults_to_umap(self) -> None:
        """Expose the three supported methods without breaking old clients."""
        base = {"file": "example.jsonl", "column": "text", "model": "model"}

        self.assertEqual("umap", AtlasRequest(**base).projection_method)
        for method in ("umap", "tsne", "pca"):
            self.assertEqual(method, AtlasRequest(**base, projection_method=method).projection_method)
        with self.assertRaises(ValidationError):
            AtlasRequest(**base, projection_method="invalid")

    def test_negative_sample_is_rejected_by_settings_and_api(self) -> None:
        """Reject invalid limits before loading a dataset or model."""
        with self.assertRaises(ValidationError):
            Settings(_env_file=None, ATLAS_SAMPLE=-1)
        with self.assertRaises(ValidationError):
            AtlasRequest(file="example.jsonl", column="text", model="model", sample=-1)

    def test_removed_umap_environment_names_report_replacements(self) -> None:
        """Fail fast when a removed UMAP setting name is still configured."""
        for old_name, new_name in (
            ("ATLAS_PROJECTION_MODE", "ATLAS_UMAP_PROJECTION_MODE"),
            ("ATLAS_ANCHOR_SAMPLE", "ATLAS_UMAP_ANCHOR_SAMPLE"),
        ):
            with self.subTest(old_name=old_name), self.assertRaisesRegex(ValidationError, new_name):
                Settings(_env_file=None, **{old_name: "1"})


class AtlasReducerTests(TestCase):
    """Test reducer output and orchestration contracts."""

    def test_reducers_return_finite_float32_coordinates(self) -> None:
        """Return a stable two-column representation for every method."""
        embeddings = np.random.default_rng(42).normal(size=(8, 4)).astype(np.float32)

        for method in ("umap", "tsne", "pca"):
            with self.subTest(method=method):
                first = reduce_embeddings(embeddings, method)
                second = reduce_embeddings(embeddings, method)
                self.assertEqual((8, 2), first.shape)
                self.assertEqual(np.float32, first.dtype)
                self.assertTrue(np.isfinite(first).all())
                self.assertTrue(np.allclose(first, second))

    def test_small_inputs_return_two_coordinate_columns(self) -> None:
        """Handle empty and one-row embedding matrices without reducer errors."""
        for method in ("umap", "tsne", "pca"):
            for row_count in (0, 1):
                with self.subTest(method=method, row_count=row_count):
                    result = reduce_embeddings(np.ones((row_count, 3), dtype=np.float32), method)
                    self.assertTrue(np.array_equal(np.zeros((row_count, 2), dtype=np.float32), result))

    def test_anchor_transform_is_used_only_for_umap(self) -> None:
        """Keep t-SNE and PCA on full projection even with UMAP anchor settings."""
        frame = pd.DataFrame({"vector": [[1.0, 2.0], [2.0, 3.0]]})

        for method in ("tsne", "pca"):
            options = AtlasOptions(
                sample=None,
                host="127.0.0.1",
                port=5055,
                batch_size=None,
                text_embedder=None,
                image_embedder=None,
                trust_remote_code=False,
                projection_method=method,
                umap_projection_mode="anchor_transform",
                umap_anchor_sample=1,
            )
            with (
                self.subTest(method=method),
                patch("local_data_studio.server.atlas_components.projection.create_embedding_session") as create_session,
                patch("local_data_studio.server.atlas_components.projection.compute_full_projection") as full_projection,
                patch("local_data_studio.server.atlas_components.projection.compute_anchor_transform_projection") as anchor_projection,
            ):
                full_projection.return_value.values = np.zeros((2, 2), dtype=np.float32)
                project_atlas_frame(
                    frame,
                    input_column="vector",
                    modality="vector",
                    model_path=Path(__file__),
                    options=options,
                )
                create_session.assert_called_once()
                full_projection.assert_called_once()
                anchor_projection.assert_not_called()

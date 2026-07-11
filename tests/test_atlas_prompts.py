"""Tests for safe Atlas prompt templates and capability-driven adapters."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, cast
from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
import torch
from fastapi import HTTPException
from PIL import Image

from local_data_studio.server.atlas_components.contracts import AtlasOptions
from local_data_studio.server.atlas_components.dataset import atlas_dataset_cache_path, prepare_atlas_dataset
from local_data_studio.server.atlas_components.embedding_backends import (
    create_sentence_transformer_embedder,
    create_transformers_image_pooler_embedder,
    create_transformers_pooling_embedder,
)
from local_data_studio.server.atlas_components.images import prepare_projection_input
from local_data_studio.server.atlas_components.prompts import (
    PromptedEmbeddingValue,
    PromptTemplateError,
    compile_prompt_template,
)
from local_data_studio.server.jobs import JobContext


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _sentence_model(path: Path) -> None:
    _write_json(path / "config.json", {"model_type": "bert"})
    _write_json(path / "tokenizer_config.json", {})
    _write_json(
        path / "modules.json",
        [
            {"idx": 0, "name": "0", "path": "", "type": "sentence_transformers.models.Transformer"},
            {"idx": 1, "name": "1", "path": "1_Pooling", "type": "sentence_transformers.models.Pooling"},
        ],
    )
    _write_json(path / "1_Pooling" / "config.json", {"pooling_mode": "mean", "include_prompt": True})


def _options(
    *,
    backend: str = "sentence-transformers",
    prompt: str | None = None,
    capability_fingerprint: str | None = None,
) -> AtlasOptions:
    return AtlasOptions(
        sample=None,
        host="127.0.0.1",
        port=5055,
        batch_size=8,
        text_embedder=None,
        image_embedder=None,
        trust_remote_code=False,
        backend=backend,
        prompt=prompt,
        capability_fingerprint=capability_fingerprint,
    )


class PromptTemplateTests(TestCase):
    """Verify row substitution without Python format-string evaluation."""

    def test_renders_multiple_columns_and_escaped_braces(self) -> None:
        """Treat exact columns as fields and doubled braces as literals."""
        template = compile_prompt_template("{{task}} {title}: {body}", ["title", "body"], 100)

        rendered = template.render({"title": "News", "body": "Details"})

        self.assertEqual("{task} News: Details", rendered)
        self.assertEqual(("title", "body"), template.columns)

    def test_rejects_unknown_columns_and_format_syntax(self) -> None:
        """Report available columns before model loading for unsafe fields."""
        with self.assertRaisesRegex(PromptTemplateError, r"available columns: title, body"):
            compile_prompt_template("{missing}", ["title", "body"], 100)
        with self.assertRaisesRegex(PromptTemplateError, "format specifiers"):
            compile_prompt_template("{title!r}", ["title"], 100)
        with self.assertRaisesRegex(PromptTemplateError, "format specifiers"):
            compile_prompt_template("{title:>20}", ["title"], 100)

    def test_text_prefix_and_complete_templates_have_distinct_inputs(self) -> None:
        """Pass plain prompts as prefixes and rendered placeholders as complete text."""
        frame = pd.DataFrame({"title": ["first"], "body": ["content"]})
        prefix = compile_prompt_template("query: ", list(frame.columns), 100)
        complete = compile_prompt_template("{title}: {body}", list(frame.columns), 100)

        prefix_frame, _, _ = prepare_projection_input(
            frame,
            column="body",
            modality="text",
            dataset_path=Path("dataset.jsonl"),
            prompt_template=prefix,
        )
        complete_frame, _, _ = prepare_projection_input(
            frame,
            column="body",
            modality="text",
            dataset_path=Path("dataset.jsonl"),
            prompt_template=complete,
        )

        prefix_value = prefix_frame.iloc[0, 0]
        complete_value = complete_frame.iloc[0, 0]
        self.assertEqual(PromptedEmbeddingValue("content", "query: "), prefix_value)
        self.assertEqual(PromptedEmbeddingValue("first: content", ""), complete_value)

    def test_image_template_preserves_binary_input(self) -> None:
        """Attach an instruction without replacing the decoded image payload."""
        frame = pd.DataFrame({"image": [{"bytes": "89504e470d0a1a0a"}], "label": ["star"]})
        template = compile_prompt_template("Represent {label}", list(frame.columns), 100)

        projection, _, _ = prepare_projection_input(
            frame,
            column="image",
            modality="image",
            dataset_path=Path("dataset.jsonl"),
            prompt_template=template,
        )

        value = projection.iloc[0, 0]
        self.assertIsInstance(value, PromptedEmbeddingValue)
        self.assertEqual(b"\x89PNG\r\n\x1a\n", value.value["bytes"])
        self.assertEqual("Represent star", value.prompt)


class SentenceTransformerAdapterTests(TestCase):
    """Verify prompt grouping and one-time model ownership."""

    def test_uses_model_default_only_when_prompt_is_absent(self) -> None:
        """Omit the prompt keyword for defaults and preserve explicit overrides."""

        class FakeModel:
            """Record encode calls and return deterministic vectors."""

            def __init__(self) -> None:
                self.calls: list[tuple[list[object], dict[str, object]]] = []

            def encode(self, values: list[object], **kwargs: object) -> np.ndarray:
                """Record one grouped call."""
                self.calls.append((values, kwargs))
                return np.ones((len(values), 2), dtype=np.float32) * len(self.calls)

        fake = FakeModel()
        with patch(
            "local_data_studio.server.atlas_components.embedding_backends.load_sentence_transformer_model",
            return_value=fake,
        ) as loader:
            embed = create_sentence_transformer_embedder("text", Path("model"), _options())
            result = asyncio.run(
                embed(
                    ["default", PromptedEmbeddingValue("first", "query: "), PromptedEmbeddingValue("second", "query: ")],
                    model=None,
                    embedder_args={},
                )
            )

        loader.assert_called_once()
        self.assertEqual((3, 2), result.shape)
        self.assertEqual(2, len(fake.calls))
        calls_by_prompt = {call[1].get("prompt"): call for call in fake.calls}
        self.assertIn(None, calls_by_prompt)
        self.assertNotIn("prompt", calls_by_prompt[None][1])
        self.assertEqual(["first", "second"], calls_by_prompt["query: "][0])


class TransformersAdapterTests(TestCase):
    """Verify statically selected multimodal pooling without repository code."""

    def test_multimodal_adapter_uses_last_token_and_normalizes(self) -> None:
        """Build a structured image message and reproduce saved post-processing."""

        class FakeProcessor:
            """Record structured messages and return a minimal tensor batch."""

            def __init__(self) -> None:
                self.messages: object = None

            def apply_chat_template(self, messages: object, **kwargs: object) -> list[str]:
                """Record messages formatted for a multimodal AutoProcessor."""
                self.messages = messages
                return ["formatted"]

            def __call__(self, **kwargs: object) -> dict[str, torch.Tensor]:
                """Return tensors accepted by the fake base model."""
                return {
                    "input_ids": torch.tensor([[1, 2, 3]]),
                    "attention_mask": torch.tensor([[1, 1, 1]]),
                }

        class FakeModel:
            """Expose token states whose final vector has a known norm."""

            def __call__(self, **inputs: torch.Tensor) -> SimpleNamespace:
                """Return three token embeddings for one input row."""
                return SimpleNamespace(last_hidden_state=torch.tensor([[[1.0, 0.0], [0.0, 2.0], [3.0, 4.0]]]))

        with TemporaryDirectory() as tmp:
            model = Path(tmp) / "renamed-multimodal-model"
            _sentence_model(model)
            _write_json(
                model / "config.json",
                {
                    "model_type": "qwen3_vl",
                    "text_config": {"model_type": "qwen3_vl_text", "vocab_size": 100},
                    "vision_config": {"model_type": "qwen3_vl"},
                },
            )
            _write_json(model / "preprocessor_config.json", {"processor_class": "Qwen3VLProcessor"})
            _write_json(
                model / "sentence_bert_config.json",
                {"modality_config": {"text": {"method": "forward"}, "image": {"method": "forward"}}},
            )
            modules = json.loads((model / "modules.json").read_text(encoding="utf-8"))
            modules.append({"idx": 2, "name": "2", "path": "2_Normalize", "type": "sentence_transformers.models.Normalize"})
            _write_json(model / "modules.json", modules)
            _write_json(model / "1_Pooling" / "config.json", {"pooling_mode": "lasttoken", "include_prompt": True})
            processor = FakeProcessor()
            with patch(
                "local_data_studio.server.atlas_components.embedding_backends.load_transformers_components",
                return_value=(FakeModel(), processor, torch.device("cpu")),
            ) as loader:
                embed = create_transformers_pooling_embedder("image", model, _options(backend="transformers"), multimodal=True)
                result = asyncio.run(
                    embed(
                        [Image.new("RGB", (1, 1))],
                        model=None,
                        embedder_args={},
                    )
                )

        loader.assert_called_once()
        np.testing.assert_allclose([[0.6, 0.8]], result, atol=1e-6)
        self.assertIsNotNone(processor.messages)
        messages = cast(list[list[dict[str, Any]]], processor.messages)
        self.assertEqual("image", messages[0][1]["content"][0]["type"])

    def test_image_adapter_uses_pooler_output(self) -> None:
        """Return the author-defined pooled image vector instead of averaging tokens."""

        class FakeProcessor:
            """Return a minimal image tensor batch."""

            def __call__(self, **kwargs: object) -> dict[str, torch.Tensor]:
                """Create one processor output tensor."""
                return {"pixel_values": torch.ones((1, 3, 1, 1))}

        class FakeModel:
            """Expose distinct pooled and token outputs."""

            def __call__(self, **inputs: torch.Tensor) -> SimpleNamespace:
                """Return a deterministic pooled vector."""
                return SimpleNamespace(
                    pooler_output=torch.tensor([[5.0, 6.0]]),
                    last_hidden_state=torch.tensor([[[100.0, 200.0]]]),
                )

        with patch(
            "local_data_studio.server.atlas_components.embedding_backends.load_transformers_image_components",
            return_value=(FakeModel(), FakeProcessor(), torch.device("cpu")),
        ) as loader:
            embed = create_transformers_image_pooler_embedder(Path("model"), _options(backend="transformers"))
            result = asyncio.run(embed([Image.new("RGB", (1, 1))], model=None, embedder_args={}))

        loader.assert_called_once()
        np.testing.assert_array_equal([[5.0, 6.0]], result)


class PromptCacheContractTests(TestCase):
    """Verify validation order and cache separation for prompt settings."""

    def test_backend_prompt_and_capability_change_cache_identity(self) -> None:
        """Prevent projected parquet reuse across materially different encoders."""
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "data.jsonl"
            dataset.write_text('{"text":"value"}\n', encoding="utf-8")
            model = root / "model"
            _sentence_model(model)
            base = _options(prompt=None, capability_fingerprint="one")
            prompt = _options(prompt="query: ", capability_fingerprint="one")
            changed = _options(prompt="query: ", capability_fingerprint="two")

            with patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", root):
                base_path = atlas_dataset_cache_path(path=dataset, column="text", modality="text", sql=None, model_path=model, options=base)
                prompt_path = atlas_dataset_cache_path(path=dataset, column="text", modality="text", sql=None, model_path=model, options=prompt)
                changed_path = atlas_dataset_cache_path(path=dataset, column="text", modality="text", sql=None, model_path=model, options=changed)

        self.assertNotEqual(base_path, prompt_path)
        self.assertNotEqual(prompt_path, changed_path)

    def test_unknown_query_column_stops_before_projection(self) -> None:
        """Validate placeholders against the loaded SQL result columns first."""

        class DummyContext:
            """Accept progress updates used during cache preparation."""

            def update(self, *, progress=None, message=None):  # noqa: ANN001
                """Ignore progress updates."""

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "data.jsonl"
            dataset.write_text('{"text":"value"}\n', encoding="utf-8")
            model = root / "model"
            _sentence_model(model)
            with (
                patch("local_data_studio.server.atlas_components.dataset.EMBEDDER_MODELS_DIR", root),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_DATA_CACHE_DIR", root / "cache"),
                patch("local_data_studio.server.atlas_components.dataset.ATLAS_CACHE_ROOT", root / "cache-root"),
                patch(
                    "local_data_studio.server.atlas_components.dataset.load_datasets",
                    return_value=pd.DataFrame({"text": ["value"], "selected": [True]}),
                ),
                patch("local_data_studio.server.atlas_components.dataset.project_atlas_frame") as project,
            ):
                with self.assertRaises(HTTPException) as raised:
                    prepare_atlas_dataset(
                        path=dataset,
                        column="text",
                        modality="text",
                        sql="SELECT text, selected FROM data",
                        model_path=model,
                        options=_options(prompt="{not_selected}", capability_fingerprint="test"),
                        context=cast(JobContext, DummyContext()),
                    )

        self.assertEqual(400, raised.exception.status_code)
        self.assertIn("text, selected", raised.exception.detail)
        project.assert_not_called()

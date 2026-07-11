"""Tests for static local embedding-model capability detection."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from local_data_studio.server.embedder_capabilities import analyze_model_capabilities


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_transformer_pipeline(path: Path, *, pooling: str = "mean", normalize: bool = True) -> None:
    modules = [
        {"idx": 0, "name": "0", "path": "", "type": "sentence_transformers.models.Transformer"},
        {"idx": 1, "name": "1", "path": "1_Pooling", "type": "sentence_transformers.models.Pooling"},
    ]
    if normalize:
        modules.append({"idx": 2, "name": "2", "path": "2_Normalize", "type": "sentence_transformers.models.Normalize"})
    _write_json(path / "modules.json", modules)
    _write_json(path / "1_Pooling" / "config.json", {"pooling_mode": pooling, "include_prompt": True})


class EmbedderCapabilityTests(TestCase):
    """Verify backend status without loading model weight files."""

    def test_native_sentence_pipeline_is_available_in_both_backends(self) -> None:
        """Recognize a reproducible Transformer, Pooling, Normalize pipeline."""
        with TemporaryDirectory() as tmp:
            model = Path(tmp) / "renamed-model"
            _write_json(model / "config.json", {"model_type": "bert"})
            _write_json(model / "tokenizer_config.json", {})
            _write_transformer_pipeline(model)

            capabilities = analyze_model_capabilities(model)

        self.assertEqual("native", capabilities.sentence_transformers.status)
        self.assertTrue(capabilities.sentence_transformers.available)
        self.assertEqual("direct", capabilities.transformers.status)
        self.assertEqual("auto-pooling", capabilities.transformers.adapter)
        self.assertEqual("sentence-transformers", capabilities.default_backend)

    def test_plain_transformers_model_uses_sentence_generic_fallback(self) -> None:
        """Prefer Sentence Transformers when both generic execution paths exist."""
        with TemporaryDirectory() as tmp:
            model = Path(tmp) / "plain-model"
            _write_json(model / "config.json", {"model_type": "bert"})
            _write_json(model / "tokenizer_config.json", {})

            capabilities = analyze_model_capabilities(model)

        self.assertEqual("generic_fallback", capabilities.sentence_transformers.status)
        self.assertEqual("direct", capabilities.transformers.status)
        self.assertEqual("sentence-transformers", capabilities.default_backend)

    def test_dense_pipeline_is_transformers_backbone_only(self) -> None:
        """Disable Transformers when saved post-processing cannot be reproduced."""
        with TemporaryDirectory() as tmp:
            model = Path(tmp) / "dense-model"
            _write_json(model / "config.json", {"model_type": "bert"})
            _write_json(model / "tokenizer_config.json", {})
            _write_transformer_pipeline(model, normalize=False)
            modules = json.loads((model / "modules.json").read_text(encoding="utf-8"))
            modules.append({"idx": 2, "name": "2", "path": "2_Dense", "type": "sentence_transformers.models.Dense"})
            _write_json(model / "modules.json", modules)
            _write_json(model / "2_Dense" / "config.json", {"in_features": 768, "out_features": 384})

            capabilities = analyze_model_capabilities(model)

        self.assertTrue(capabilities.sentence_transformers.available)
        self.assertEqual("backbone_only", capabilities.transformers.status)
        self.assertFalse(capabilities.transformers.available)

    def test_custom_or_malformed_modules_are_not_executed_during_discovery(self) -> None:
        """Classify unsafe and invalid module metadata without importing its code."""
        with TemporaryDirectory() as tmp:
            custom = Path(tmp) / "custom"
            malformed = Path(tmp) / "malformed"
            _write_json(custom / "config.json", {"model_type": "bert"})
            _write_json(custom / "modules.json", [{"idx": 0, "name": "0", "path": "", "type": "custom.module.Encoder"}])
            _write_json(malformed / "config.json", {"model_type": "bert"})
            _write_json(malformed / "modules.json", [{"idx": 2, "type": 3}])

            custom_capabilities = analyze_model_capabilities(custom)
            malformed_capabilities = analyze_model_capabilities(malformed)

        self.assertEqual("unknown", custom_capabilities.sentence_transformers.status)
        self.assertFalse(custom_capabilities.sentence_transformers.available)
        self.assertEqual("unknown", malformed_capabilities.sentence_transformers.status)

    def test_auto_map_is_reported_as_remote_code_when_config_cannot_load_safely(self) -> None:
        """Keep repository code disabled during the discovery request."""
        with TemporaryDirectory() as tmp:
            model = Path(tmp) / "remote-model"
            _write_json(
                model / "config.json",
                {"model_type": "unregistered_model", "auto_map": {"AutoConfig": "configuration.CustomConfig"}},
            )

            capabilities = analyze_model_capabilities(model)
            trusted_capabilities = analyze_model_capabilities(model, allow_remote_code=True)

        self.assertEqual("remote_code", capabilities.transformers.status)
        self.assertFalse(capabilities.transformers.available)
        self.assertFalse(capabilities.sentence_transformers.available)
        self.assertTrue(trusted_capabilities.transformers.available)
        self.assertTrue(trusted_capabilities.sentence_transformers.available)

    def test_multimodal_auto_mapping_is_detected_without_model_name_rules(self) -> None:
        """Resolve a renamed Qwen3-VL config through AutoClass and module metadata."""
        with TemporaryDirectory() as tmp:
            model = Path(tmp) / "arbitrary-local-name"
            _write_json(
                model / "config.json",
                {
                    "model_type": "qwen3_vl",
                    "text_config": {"model_type": "qwen3_vl_text", "vocab_size": 100},
                    "vision_config": {"model_type": "qwen3_vl"},
                },
            )
            _write_json(model / "tokenizer_config.json", {})
            _write_json(model / "preprocessor_config.json", {"processor_class": "Qwen3VLProcessor"})
            _write_json(
                model / "sentence_bert_config.json",
                {"modality_config": {"text": {"method": "forward"}, "image": {"method": "forward"}}},
            )
            _write_transformer_pipeline(model, pooling="lasttoken")

            capabilities = analyze_model_capabilities(model)

        self.assertEqual(("text", "image"), capabilities.transformers.modalities)
        self.assertEqual("auto-pooling-multimodal", capabilities.transformers.adapter)
        self.assertTrue(capabilities.transformers.available)

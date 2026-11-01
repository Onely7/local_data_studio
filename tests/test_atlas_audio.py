"""Tests for embedding audio columns in Embedding Atlas."""

from __future__ import annotations

import asyncio
import io
import json
import math
import struct
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from local_data_studio.server.atlas_components.contracts import AtlasOptions
from local_data_studio.server.atlas_components.embedding_backends import audio_sampling_rate, create_sentence_transformer_embedder
from local_data_studio.server.atlas_components.images import prepare_projection_input
from local_data_studio.server.atlas_components.service import infer_atlas_modality


def wav_bytes(seconds: float, rate: int = 16_000) -> bytes:
    """Return a mono 16-bit PCM WAV tone."""
    frames = b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * 440 * index / rate))) for index in range(int(seconds * rate)))
    output = io.BytesIO()
    with wave.open(output, "wb") as writer:
        writer.setnchannels(1)
        writer.setsampwidth(2)
        writer.setframerate(rate)
        writer.writeframes(frames)
    return output.getvalue()


def _options(prompt: str | None = None) -> AtlasOptions:
    return AtlasOptions(
        sample=None,
        host="127.0.0.1",
        port=5055,
        batch_size=8,
        text_embedder=None,
        image_embedder=None,
        trust_remote_code=False,
        backend="sentence-transformers",
        prompt=prompt,
    )


class _RecordingModel:
    def __init__(self) -> None:
        self.calls: list[tuple[list[object], dict[str, object]]] = []

    def encode(self, values: list[object], **kwargs: object) -> np.ndarray:
        self.calls.append((values, kwargs))
        return np.ones((len(values), 4), dtype=np.float32)


def test_audio_projection_input_spools_readable_rows_and_labels_them() -> None:
    """Keep readable clips, skip unreadable ones, and label the display column."""
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "clips").mkdir()
        (root / "clips" / "c.wav").write_bytes(wav_bytes(0.25))
        frame = pd.DataFrame(
            {
                "audio": [{"bytes": wav_bytes(0.5), "path": "speech/a.wav"}, {"bytes": b"RIFF0000WAVEjunk", "path": "b.wav"}, "clips/c.wav"],
                "text": ["first", "broken", "third"],
            }
        )
        with patch("local_data_studio.server.atlas_components.projection_inputs.FILE_SERVE_ROOTS", [root]):
            projection, input_column, output = prepare_projection_input(frame, column="audio", modality="audio", dataset_path=root / "data.parquet")

        items = projection[input_column].tolist()
        spool_path = Path(projection._values._stream.name)
        projection.close()

    assert list(output["text"]) == ["first", "third"]
    assert list(output["audio"]) == ["a.wav · 0.5 s", "c.wav · 0.2 s"]
    assert [len(item["audio_bytes"]) for item in items] == [len(wav_bytes(0.5)), len(wav_bytes(0.25))]
    assert not spool_path.exists()


def test_audio_rows_are_decoded_at_the_model_rate_without_prompts() -> None:
    """Resample clips to the model's rate, drop text prompts, and lift the 30 s cut."""
    fake = _RecordingModel()
    capabilities = SimpleNamespace(sentence_transformers=SimpleNamespace(modalities=("text", "audio")))
    with TemporaryDirectory() as tmpdir:
        model_dir = Path(tmpdir)
        (model_dir / "processor_config.json").write_text(json.dumps({"feature_extractor": {"sampling_rate": 24_000}}), encoding="utf-8")
        with (
            patch("local_data_studio.server.atlas_components.embedding_backends.load_sentence_transformer_model", return_value=fake),
            patch("local_data_studio.server.atlas_components.embedding_backends.analyze_model_capabilities", return_value=capabilities),
        ):
            embed = create_sentence_transformer_embedder("audio", model_dir, _options(prompt="task: clustering | query: "))
            vectors = asyncio.run(embed([{"audio_bytes": wav_bytes(0.5)}, {"audio_bytes": wav_bytes(1.0)}], model=None, embedder_args={}))

    values, kwargs = fake.calls[0]
    assert vectors.shape == (2, 4)
    assert "prompt" not in kwargs
    assert kwargs["batch_size"] == 2
    assert all(value["sampling_rate"] == 24_000 and value["array"].dtype == np.float32 for value in values)
    assert abs(values[1]["array"].size - 24_000) <= 64
    assert kwargs["processing_kwargs"] == {"audio": {"max_length": values[1]["array"].size}}


def test_audio_sampling_rate_falls_back_to_speech_rate() -> None:
    """Read the feature extractor rate and default to 16 kHz without one."""
    with TemporaryDirectory() as tmpdir:
        model_dir = Path(tmpdir)
        assert audio_sampling_rate(model_dir) == 16_000
        (model_dir / "preprocessor_config.json").write_text(json.dumps({"sampling_rate": 48_000}), encoding="utf-8")
        assert audio_sampling_rate(model_dir) == 48_000


def test_infer_atlas_modality_detects_audio_columns() -> None:
    """Treat audio structs and audio paths as audio and leave text alone."""
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "speech.parquet"
        pq.write_table(
            pa.table(
                {
                    "audio": [{"bytes": wav_bytes(0.1), "path": "a.wav"}],
                    "clip": ["clips/a.flac"],
                    "text": ["hello world"],
                }
            ),
            path,
        )

        assert infer_atlas_modality(path, "audio") == "audio"
        assert infer_atlas_modality(path, "clip") == "audio"
        assert infer_atlas_modality(path, "text") == "text"

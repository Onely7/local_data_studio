"""Tests for audio decoding, transcription settings, and transcription jobs."""

import io
import math
import struct
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from local_data_studio.app import app
from local_data_studio.server import transcription_service
from local_data_studio.server.audio_decode import AudioDecodeError, audio_source_bytes, decode_audio_waveform
from local_data_studio.server.transcription_config import TranscriptionModelProfile, TranscriptionSettings, public_transcription_config
from local_data_studio.server.transcription_service import (
    AudioCellReference,
    TranscriptionResult,
    TranscriptionService,
    join_transcripts,
    load_cell_audio,
    split_at_quiet_points,
    transcribe_cells,
)


def wav_bytes(seconds: float = 1.0, rate: int = 22_050, channels: int = 2) -> bytes:
    """Return a 16-bit PCM WAV tone at an arbitrary rate and channel count."""
    frames = bytearray()
    for index in range(int(seconds * rate)):
        sample = int(8000 * math.sin(2 * math.pi * 440 * index / rate))
        frames += struct.pack("<h", sample) * channels
    output = io.BytesIO()
    with wave.open(output, "wb") as writer:
        writer.setnchannels(channels)
        writer.setsampwidth(2)
        writer.setframerate(rate)
        writer.writeframes(bytes(frames))
    return output.getvalue()


class _Context:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def check_cancelled(self) -> None:
        return None

    def update(self, *, progress: float | None = None, message: str | None = None) -> None:
        if message:
            self.messages.append(message)


class _FakeRecognizer:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str | None, int]] = []

    def transcribe(self, waveform: np.ndarray, *, language: str | None, max_new_tokens: int) -> TranscriptionResult:
        self.calls.append((waveform.size, language, max_new_tokens))
        return TranscriptionResult(text=f"segment {len(self.calls)}", language="English")


def _service(fake: _FakeRecognizer, **settings: object) -> tuple[TranscriptionService, TranscriptionModelProfile]:
    profile = TranscriptionModelProfile(id="fake", label="Fake ASR", model="models/asr/fake")
    service = TranscriptionService(TranscriptionSettings(models=(profile,), idle_unload_seconds=0, **settings), loader=lambda _: fake)
    return service, profile


def test_decode_audio_waveform_resamples_to_mono_16k() -> None:
    """Decode stereo 22.05 kHz WAV bytes into a mono float32 waveform at 16 kHz."""
    waveform = decode_audio_waveform(wav_bytes(1.0))

    assert waveform.dtype == np.float32
    assert waveform.ndim == 1
    assert abs(waveform.size - 16_000) <= 64
    assert 0.1 < float(np.abs(waveform).max()) <= 1.0


def test_decode_audio_waveform_limits_length_or_truncates() -> None:
    """Reject audio past the limit unless the caller asks for truncation."""
    data = wav_bytes(3.0, rate=16_000, channels=1)

    with pytest.raises(AudioDecodeError, match="longer than 1 seconds"):
        decode_audio_waveform(data, max_seconds=1.0)
    assert decode_audio_waveform(data, max_seconds=1.0, truncate=True).size == 16_000
    with pytest.raises(AudioDecodeError, match="failed to decode"):
        decode_audio_waveform(b"not audio at all")


def test_audio_source_bytes_reads_embedded_and_referenced_audio() -> None:
    """Read struct bytes and dataset-relative paths, and refuse non-audio values."""
    data = wav_bytes(0.1, rate=16_000, channels=1)
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "clip.wav").write_bytes(data)

        assert audio_source_bytes({"bytes": data, "path": "x.wav"}, dataset_path=root / "d.parquet", allowed_roots=[root]) == data
        assert audio_source_bytes("clip.wav", dataset_path=root / "d.parquet", allowed_roots=[root]) == data
        with pytest.raises(AudioDecodeError, match="does not contain audio"):
            audio_source_bytes("plain text", dataset_path=None, allowed_roots=[root])
        with pytest.raises(AudioDecodeError, match="missing or outside"):
            audio_source_bytes("missing.wav", dataset_path=root / "d.parquet", allowed_roots=[root])


def test_split_at_quiet_points_cuts_in_silence() -> None:
    """Split long audio at the quietest window before each limit."""
    rate = 1000
    loud = np.ones(rate * 4, dtype=np.float32)
    waveform = np.concatenate([loud, np.zeros(rate // 2, dtype=np.float32), loud, np.zeros(rate // 2, dtype=np.float32), loud])

    segments = split_at_quiet_points(waveform, sampling_rate=rate, max_seconds=6, search_seconds=3)

    assert sum(segment.size for segment in segments) == waveform.size
    assert all(segment.size <= 6 * rate for segment in segments)
    assert 4000 <= segments[0].size <= 4500
    assert split_at_quiet_points(loud, sampling_rate=rate, max_seconds=6) == [loud]


def test_join_transcripts_keeps_japanese_and_chinese_unspaced() -> None:
    """Join segments with spaces except between unspaced scripts."""
    assert join_transcripts(["Hello there.", " General Kenobi.", ""]) == "Hello there. General Kenobi."
    assert join_transcripts(["今日は", "いい天気です。"]) == "今日はいい天気です。"
    assert join_transcripts(["음성", "인식"]) == "음성 인식"


def test_settings_validate_profiles_and_defaults() -> None:
    """Reject duplicate IDs, unknown defaults, and invalid devices."""
    profile = {"id": "a", "model": "models/asr/a"}
    with pytest.raises(ValidationError, match="unique"):
        TranscriptionSettings.model_validate({"models": [profile, profile]})
    with pytest.raises(ValidationError, match="default_model"):
        TranscriptionSettings.model_validate({"default_model": "b", "models": [profile]})
    with pytest.raises(ValidationError, match="device"):
        TranscriptionModelProfile.model_validate({"id": "a", "model": "x", "device": "tpu"})
    assert TranscriptionModelProfile.model_validate({"id": "a", "model": "x", "language": "auto"}).language is None


def test_profile_availability_distinguishes_directories_and_hub_ids() -> None:
    """Load local directories, allow Hub IDs, and flag missing paths."""
    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        (base / "models" / "asr").mkdir(parents=True)

        local = TranscriptionModelProfile(id="local", model="models/asr")
        hub = TranscriptionModelProfile(id="hub", model="Qwen/Qwen3-ASR-1.7B-hf")
        missing = TranscriptionModelProfile(id="missing", model="models/asr/missing/model")

        assert local.model_location(base) == (base / "models" / "asr").resolve()
        assert local.availability(base) == (True, "")
        assert hub.availability(base) == (True, "downloads from Hugging Face on first use")
        assert missing.availability(base)[0] is False


def test_public_config_hides_model_locations() -> None:
    """Expose IDs, labels, and availability without filesystem paths."""
    settings = TranscriptionSettings(models=(TranscriptionModelProfile(id="hub", label="Hub ASR", model="org/model"),))

    payload = public_transcription_config(settings)

    assert payload["models"] == [{"id": "hub", "label": "Hub ASR", "available": True, "reason": "downloads from Hugging Face on first use"}]
    assert payload["default_model"] == "hub"
    assert "org/model" not in str(payload)


def test_service_transcribes_segments_and_reuses_the_loaded_model() -> None:
    """Split long waveforms, budget tokens per segment, and load the model once."""
    fake = _FakeRecognizer()
    service, profile = _service(fake, chunk_seconds=10)
    rate = 16_000
    waveform = np.ones(rate * 25, dtype=np.float32)
    for gap in (9.5, 19.0):
        waveform[int(gap * rate) : int((gap + 0.2) * rate)] = 0

    first = service.transcribe(profile, waveform)
    second = service.transcribe(profile, np.zeros(rate, dtype=np.float32))

    sizes = [call[0] / rate for call in fake.calls]
    tokens = [call[2] for call in fake.calls]
    assert first == TranscriptionResult(text="segment 1 segment 2 segment 3", language="English")
    assert second.text == "segment 4"
    assert 9.4 < sizes[0] < 9.8 and 9.2 < sizes[1] < 9.8 and sum(sizes[:3]) == pytest.approx(25)
    assert all(tokens[index] == int(sizes[index] * 15) for index in range(3))
    assert tokens[3] == 64
    assert service.loaded_model == "fake"
    service.unload()
    assert service.loaded_model is None


def test_transcribe_cells_caches_results_and_reports_item_errors(tmp_path: Path) -> None:
    """Reuse cached transcripts by content and keep failures per item."""
    fake = _FakeRecognizer()
    service, _ = _service(fake)
    clip = wav_bytes(0.5, rate=16_000, channels=1)
    references = {
        "good": AudioCellReference(file="d.parquet", column="audio", row_id=1),
        "same": AudioCellReference(file="d.parquet", column="audio", row_id=2),
        "bad": AudioCellReference(file="d.parquet", column="audio", row_id=3),
    }

    def read_audio(reference: AudioCellReference) -> bytes:
        if reference.row_id == 3:
            raise AudioDecodeError("audio file is missing")
        return clip

    with patch.object(transcription_service, "TRANSCRIPT_CACHE_DIR", tmp_path / "transcripts"):
        result = transcribe_cells(list(references.items()), model="fake", context=_Context(), service=service, read_audio=read_audio)

    items = {item["id"]: item for item in result["items"]}
    assert result["model"] == "fake"
    assert items["good"] == {"id": "good", "text": "segment 1", "language": "English", "duration": 0.5, "cached": False}
    assert items["same"]["cached"] is True
    assert items["same"]["text"] == "segment 1"
    assert items["bad"] == {"id": "bad", "error": "audio file is missing"}
    assert len(fake.calls) == 1


def test_load_cell_audio_follows_pointers_inside_rows() -> None:
    """Read the audio a pointer selects from a Parquet row."""
    clip = wav_bytes(0.1, rate=16_000, channels=1)
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "speech.parquet"
        pq.write_table(pa.table({"clips": [[{"bytes": clip, "path": "a.wav"}]]}), path)
        with patch("local_data_studio.server.files.resolve_data_file", return_value=path):
            assert load_cell_audio(AudioCellReference(file="speech.parquet", column="clips", pointer=(0,), row_id=1)) == clip
            with pytest.raises(AudioDecodeError, match="not found"):
                load_cell_audio(AudioCellReference(file="speech.parquet", column="clips", pointer=(4,), row_id=1))
            with pytest.raises(AudioDecodeError, match="column not found"):
                load_cell_audio(AudioCellReference(file="speech.parquet", column="missing", row_id=1))


def test_transcription_api_validates_models_and_batch_size() -> None:
    """Reject unknown models and oversized batches before starting a job."""
    profile = TranscriptionModelProfile(id="hub", model="org/model")
    service = TranscriptionService(TranscriptionSettings(models=(profile,), max_batch_items=1, idle_unload_seconds=0))
    item = {"id": "a", "file": "d.parquet", "row_id": 1, "column": "audio"}
    client = TestClient(app)
    with patch("local_data_studio.server.api.jobs.TRANSCRIPTION_SERVICE", service):
        unknown = client.post("/api/jobs/transcription", json={"model": "other", "items": [item]})
        oversized = client.post("/api/jobs/transcription", json={"model": "hub", "items": [item, {**item, "id": "b"}]})
        duplicate = client.post("/api/jobs/transcription", json={"model": "hub", "items": [item, item]})

    assert unknown.status_code == 400
    assert "unknown transcription model" in unknown.json()["detail"]
    assert oversized.status_code == 400
    assert "at most 1" in oversized.json()["detail"]
    assert duplicate.status_code == 422


def test_transcription_models_endpoint_returns_public_config() -> None:
    """Serve the browser-safe model list."""
    response = TestClient(app).get("/api/transcription_models")

    assert response.status_code == 200
    assert set(response.json()) == {"models", "default_model", "limits"}

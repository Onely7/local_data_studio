"""Tests for audio detection, preview markers, and audio serving."""

import base64
import io
import math
import struct
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from local_data_studio.app import app
from local_data_studio.server.audio_values import (
    audio_bytes_from_value,
    audio_duration_seconds,
    audio_mime_from_bytes,
    audio_mime_from_path,
    audio_preview_marker,
    find_audio_source,
    is_audio_like_value,
    is_audio_preview_marker,
    resolve_audio_path,
    value_at_pointer,
)
from local_data_studio.server.files import resolve_raw_media_file
from local_data_studio.server.serialization import serialize_raw_value, serialize_value


def wav_bytes(seconds: float = 1.0, rate: int = 8000) -> bytes:
    """Return a mono 16-bit PCM WAV file holding a 440 Hz tone."""
    frames = b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * 440 * index / rate))) for index in range(int(seconds * rate)))
    output = io.BytesIO()
    with wave.open(output, "wb") as writer:
        writer.setnchannels(1)
        writer.setsampwidth(2)
        writer.setframerate(rate)
        writer.writeframes(frames)
    return output.getvalue()


def flac_header(sample_rate: int = 16_000, total_samples: int = 32_000) -> bytes:
    """Return the FLAC signature and a STREAMINFO block without audio frames."""
    packed = (sample_rate << 44) | (0 << 41) | (15 << 36) | total_samples
    stream_info = b"\x00\x10\x00\x10" + b"\x00" * 6 + packed.to_bytes(8, "big") + b"\x00" * 16
    return b"fLaC" + bytes([0x80, 0x00, 0x00, len(stream_info)]) + stream_info


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (wav_bytes(0.01), "audio/wav"),
        (flac_header(), "audio/flac"),
        (b"OggS\x00\x02" + b"\x00" * 20, "audio/ogg"),
        (b"ID3\x04\x00" + b"\x00" * 20, "audio/mpeg"),
        (b"\xff\xfb\x90\x64" + b"\x00" * 20, "audio/mpeg"),
        (b"\xff\xf1\x50\x80" + b"\x00" * 20, "audio/aac"),
        (b"\x00\x00\x00\x20ftypM4A \x00\x00\x00\x00", "audio/mp4"),
        (b"FORM\x00\x00\x00\x10AIFF" + b"\x00" * 8, "audio/aiff"),
        (b"\x89PNG\r\n\x1a\n" + b"\x00" * 8, None),
        (b"\xff\xd8\xff\xe0" + b"\x00" * 8, None),
        (b"RIFF\x00\x00\x00\x00WEBPVP8 ", None),
        (b"\x00\x00\x00\x20ftypisom\x00\x00\x00\x00", None),
        (b"hello world", None),
    ],
)
def test_audio_mime_from_bytes_recognizes_container_signatures(data: bytes, expected: str | None) -> None:
    """Detect common audio containers and leave images, video, and text alone."""
    assert audio_mime_from_bytes(data) == expected


def test_audio_duration_reads_wav_and_flac_headers() -> None:
    """Read durations from WAV data chunks and FLAC STREAMINFO without decoding."""
    assert audio_duration_seconds(wav_bytes(1.5), "audio/wav") == pytest.approx(1.5)
    assert audio_duration_seconds(flac_header(16_000, 40_000), "audio/flac") == pytest.approx(2.5)
    assert audio_duration_seconds(b"ID3" + b"\x00" * 30, "audio/mpeg") is None


def test_preview_marker_carries_size_and_duration_without_payload() -> None:
    """Replace audio bytes with a payload-free marker the browser recognizes."""
    data = wav_bytes(2.0)
    marker = audio_preview_marker(data, "audio/wav")

    assert marker == f"data:audio/wav;preview=1;size={len(data)};duration=2.000;base64,"
    assert is_audio_preview_marker(marker)
    assert not is_audio_preview_marker(f"data:audio/wav;base64,{base64.b64encode(data).decode()}")
    assert audio_bytes_from_value(marker) is None


def test_serialize_value_replaces_audio_bytes_with_preview_markers() -> None:
    """Keep preview pages small for bare bytes, Hugging Face structs, and data URLs."""
    data = wav_bytes(0.5)
    encoded = base64.b64encode(data).decode()

    bare = serialize_value(data)
    struct_value = serialize_value({"bytes": b"\x01\x02\x03\x04" * 8, "path": "clip.mp3"})
    data_url = serialize_value(f"data:audio/wav;base64,{encoded}")
    base64_bytes = serialize_value({"bytes": encoded, "path": None})

    assert bare.startswith("data:audio/wav;preview=1;size=")
    assert struct_value == {"bytes": "data:audio/mpeg;preview=1;size=32;base64,", "path": "clip.mp3"}
    assert data_url.startswith("data:audio/wav;preview=1;")
    assert base64_bytes["bytes"].startswith("data:audio/wav;preview=1;")


def test_serialize_value_keeps_webp_images_that_share_the_riff_signature() -> None:
    """Treat RIFF/WEBP payloads under ``bytes`` as images, not audio."""
    from PIL import Image  # noqa: PLC0415

    output = io.BytesIO()
    Image.new("RGB", (8, 8), "red").save(output, format="WEBP")
    encoded = base64.b64encode(output.getvalue()).decode()

    assert serialize_value({"bytes": encoded, "path": None})["bytes"].startswith("data:image/webp")


def test_serialize_raw_value_returns_playable_audio_data_urls() -> None:
    """Return full audio as data URLs when the user asks for the raw row."""
    data = wav_bytes(0.2)

    raw = serialize_raw_value({"bytes": data, "path": "clip.wav"})

    assert raw["bytes"] == f"data:audio/wav;base64,{base64.b64encode(data).decode()}"
    assert serialize_raw_value({"bytes": b"\x00\x01\x02\x03", "path": "clip.flac"})["bytes"].startswith("data:audio/flac;base64,")


def test_audio_like_values_cover_structs_paths_and_lists() -> None:
    """Recognize the shapes Atlas and transcription accept as audio."""
    assert is_audio_like_value({"bytes": wav_bytes(0.01), "path": None})
    assert is_audio_like_value({"bytes": None, "path": "audio/clip.flac"})
    assert is_audio_like_value("https://example.com/a.mp3?download=1")
    assert is_audio_like_value([None, "clip.ogg"])
    assert not is_audio_like_value([None, None, None, "clip.ogg"])
    assert not is_audio_like_value({"bytes": b"\x89PNG\r\n\x1a\n", "path": "image.png"})
    assert not is_audio_like_value("a sentence that mentions clip.wav in passing")


def test_find_audio_source_prefers_embedded_bytes_and_follows_pointers() -> None:
    """Prefer struct bytes over paths, return references otherwise, and walk pointers."""
    data = wav_bytes(0.05)
    value = {"clips": [{"bytes": None, "path": "a.flac"}, {"bytes": data, "path": "b.wav"}]}

    assert find_audio_source(value_at_pointer(value, ["clips", 1])) == (data, "audio/wav")
    assert find_audio_source(value_at_pointer(value, ["clips", 0])) == "a.flac"
    assert find_audio_source("notes.txt") is None
    with pytest.raises(KeyError):
        value_at_pointer(value, ["clips", 5])


def test_resolve_audio_path_prefers_dataset_directory_and_rejects_escapes() -> None:
    """Resolve relative references beside the dataset and keep them inside allowed roots."""
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "set" / "clips").mkdir(parents=True)
        clip = root / "set" / "clips" / "a.wav"
        clip.write_bytes(wav_bytes(0.01))
        outside = Path(tmpdir).parent / "outside.wav"

        assert resolve_audio_path("clips/a.wav", dataset_path=root / "set" / "data.parquet", allowed_roots=[root]) == clip.resolve()
        with pytest.raises(ValueError, match="remote"):
            resolve_audio_path("https://example.com/a.wav", dataset_path=None, allowed_roots=[root])
        with pytest.raises(ValueError, match="missing or outside"):
            resolve_audio_path(str(outside), dataset_path=None, allowed_roots=[root])


def test_raw_media_file_allows_audio_suffixes() -> None:
    """Let /api/raw serve local audio files under the allowed roots."""
    with TemporaryDirectory() as tmpdir:
        clip = Path(tmpdir) / "clip.opus"
        clip.write_bytes(b"OggS" + b"\x00" * 16)

        assert resolve_raw_media_file(str(clip), [Path(tmpdir)]) == clip.resolve()
        assert audio_mime_from_path("clip.OPUS") == "audio/ogg"


def test_raw_media_endpoint_returns_embedded_and_referenced_audio() -> None:
    """Return a cell's audio bytes, including structs, list items, and dataset-relative files."""
    embedded = wav_bytes(0.25)
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "clips").mkdir()
        (root / "clips" / "ref.flac").write_bytes(flac_header())
        path = root / "speech.parquet"
        table = pa.table(
            {
                "audio": [{"bytes": embedded, "path": "a.wav"}],
                "playlist": [["clips/ref.flac"]],
                "text": ["hello"],
            }
        )
        pq.write_table(table, path)
        client = TestClient(app)
        with (
            patch("local_data_studio.server.api.datasets.resolve_data_file", return_value=path),
            patch("local_data_studio.server.api.datasets.FILE_SERVE_ROOTS", [root]),
        ):
            struct_response = client.post("/api/raw_media", json={"file": "speech.parquet", "row_id": 1, "column": "audio"})
            listed_response = client.post("/api/raw_media", json={"file": "speech.parquet", "row_id": 1, "column": "playlist", "pointer": [0]})
            text_response = client.post("/api/raw_media", json={"file": "speech.parquet", "row_id": 1, "column": "text"})
            missing_response = client.post("/api/raw_media", json={"file": "speech.parquet", "row_id": 1, "column": "playlist", "pointer": [3]})

    assert struct_response.status_code == 200
    assert struct_response.headers["content-type"] == "audio/wav"
    assert struct_response.content == embedded
    assert listed_response.status_code == 200
    assert listed_response.headers["content-type"] == "audio/flac"
    assert listed_response.content == flac_header()
    assert text_response.status_code == 415
    assert missing_response.status_code == 404

"""Read audio sources and decode them into mono float32 waveforms.

Speech models in this package take 16 kHz mono float32 arrays. PyAV decodes
every container the preview plays and resamples without an ffmpeg binary.
"""

from __future__ import annotations

import io
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import numpy as np

from .audio_values import find_audio_source, resolve_audio_path

SPEECH_SAMPLING_RATE = 16_000
AUDIO_SOURCE_MAX_BYTES = 512 * 1024 * 1024
AUDIO_URL_TIMEOUT_SECONDS = 30


class AudioDecodeError(ValueError):
    """Raised when audio cannot be read, decoded, or exceeds a length limit."""


def _read_url(url: str, max_bytes: int) -> bytes:
    request = Request(url, headers={"User-Agent": "local-data-studio/audio"})
    chunks: list[bytes] = []
    total = 0
    try:
        with urlopen(request, timeout=AUDIO_URL_TIMEOUT_SECONDS) as response:
            while chunk := response.read(1024 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    raise AudioDecodeError(f"audio exceeds {max_bytes} bytes: {url}")
                chunks.append(chunk)
    except OSError as exc:
        raise AudioDecodeError(f"failed to read audio URL {url}: {exc}") from exc
    return b"".join(chunks)


def audio_source_bytes(
    value: Any,
    *,
    dataset_path: Path | None,
    allowed_roots: Sequence[Path],
    max_bytes: int = AUDIO_SOURCE_MAX_BYTES,
) -> bytes:
    """Return the encoded audio bytes a cell value embeds or references.

    Embedded bytes win over paths. Relative paths resolve beside the dataset
    or below the allowed roots, and HTTP(S) URLs are downloaded with a size
    limit.

    Raises:
        AudioDecodeError: The value holds no audio, or the audio cannot be read.
    """
    source = find_audio_source(value)
    if source is None:
        raise AudioDecodeError("the value does not contain audio")
    if isinstance(source, tuple):
        return source[0]
    if re.match(r"^https?://", source, re.IGNORECASE):
        return _read_url(source, max_bytes)
    try:
        path = resolve_audio_path(source, dataset_path=dataset_path, allowed_roots=allowed_roots)
    except ValueError as exc:
        raise AudioDecodeError(str(exc)) from exc
    if path.stat().st_size > max_bytes:
        raise AudioDecodeError(f"audio exceeds {max_bytes} bytes: {source}")
    return path.read_bytes()


def decode_audio_waveform(
    data: bytes,
    *,
    sampling_rate: int = SPEECH_SAMPLING_RATE,
    max_seconds: float | None = None,
    truncate: bool = False,
) -> np.ndarray:
    """Decode encoded audio into a mono float32 waveform at ``sampling_rate``.

    Args:
        data: Encoded audio such as WAV, FLAC, MP3, OGG, Opus, or M4A bytes.
        sampling_rate: Output rate in Hz.
        max_seconds: Optional length limit.
        truncate: Keep the first ``max_seconds`` instead of failing when the
            audio is longer.

    Raises:
        AudioDecodeError: The bytes cannot be decoded, contain no audio stream,
            or exceed ``max_seconds`` while ``truncate`` is false.
    """
    import av  # noqa: PLC0415

    limit = int(max_seconds * sampling_rate) if max_seconds else None
    resampler = av.AudioResampler(format="flt", layout="mono", rate=sampling_rate)
    chunks: list[np.ndarray] = []
    total = 0
    try:
        with av.open(io.BytesIO(data), mode="r") as container:
            if not container.streams.audio:
                raise AudioDecodeError("the file has no audio stream")
            for frame in container.decode(container.streams.audio[0]):
                for resampled in resampler.resample(frame):
                    samples = resampled.to_ndarray().reshape(-1)
                    chunks.append(samples)
                    total += samples.size
                if limit is not None and total > limit:
                    if not truncate:
                        raise AudioDecodeError(f"audio is longer than {max_seconds:g} seconds")
                    break
            else:
                chunks.extend(resampled.to_ndarray().reshape(-1) for resampled in resampler.resample(None))
    except av.error.FFmpegError as exc:
        raise AudioDecodeError(f"failed to decode audio: {exc}") from exc
    waveform = np.concatenate(chunks).astype(np.float32, copy=False) if chunks else np.zeros(0, dtype=np.float32)
    return waveform[:limit] if limit is not None else waveform

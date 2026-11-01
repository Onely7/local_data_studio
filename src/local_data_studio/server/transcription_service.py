"""Speech recognition for audio cells with on-demand model loading.

Qwen3-ASR checkpoints in the transformers format run through their processor's
transcription request. Every other model runs through the transformers
``automatic-speech-recognition`` pipeline, which covers Whisper and CTC models.
One model stays loaded at a time and is released after an idle period.
"""

from __future__ import annotations

import contextlib
import gc
import hashlib
import json
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

import numpy as np

from .audio_decode import SPEECH_SAMPLING_RATE, AudioDecodeError, audio_source_bytes, decode_audio_waveform
from .audio_values import value_at_pointer
from .config import CACHE_DIR, FILE_SERVE_ROOTS
from .jobs import JobCancelledError
from .transcription_config import TRANSCRIPTION_SETTINGS, TranscriptionModelProfile, TranscriptionSettings

TRANSCRIPT_CACHE_DIR = CACHE_DIR / "transcripts"
TRANSCRIPT_CACHE_MAX_BYTES = 64 * 1024 * 1024
TRANSCRIPT_CACHE_VERSION = "transcript-v1"
# Speech rarely exceeds 8 tokens per second; the margin covers dense scripts.
TOKENS_PER_SECOND = 15
MIN_NEW_TOKENS = 64
CHUNKING_PIPELINE_TYPES = frozenset({"seq2seq_whisper", "ctc", "ctc_with_lm"})


@dataclass(frozen=True, slots=True)
class TranscriptionResult:
    """Text and detected language for one waveform."""

    text: str
    language: str | None


@dataclass(frozen=True, slots=True)
class AudioCellReference:
    """One audio value selected by dataset row or SQL result row."""

    file: str
    column: str
    pointer: Sequence[str | int] = ()
    row_id: int | None = None
    sql: str | None = None
    offset: int | None = None


class SpeechRecognizer(Protocol):
    """A loaded speech model that transcribes 16 kHz mono waveforms."""

    def transcribe(self, waveform: np.ndarray, *, language: str | None, max_new_tokens: int) -> TranscriptionResult:
        """Transcribe one waveform."""
        ...


def _torch_device(device: str) -> str:
    import torch  # noqa: PLC0415

    if device == "auto":
        return "cuda:0" if torch.cuda.is_available() else "cpu"
    if device.startswith("cuda") and not torch.cuda.is_available():
        raise RuntimeError("CUDA is not available for transcription")
    return "cuda:0" if device == "cuda" else device


def _torch_dtype(device: str) -> Any:
    import torch  # noqa: PLC0415

    if not device.startswith("cuda"):
        return torch.float32
    return torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16


class Qwen3AsrRecognizer:
    """Qwen3-ASR through ``apply_transcription_request`` and ``generate``."""

    def __init__(self, location: Path | str, device: str) -> None:
        """Load the processor and model in bfloat16 on CUDA or float32 on CPU."""
        from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor  # noqa: PLC0415

        self.processor = AutoProcessor.from_pretrained(location)
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(location, dtype=_torch_dtype(device)).to(device).eval()

    def transcribe(self, waveform: np.ndarray, *, language: str | None, max_new_tokens: int) -> TranscriptionResult:
        """Transcribe one waveform and parse the detected language."""
        import torch  # noqa: PLC0415

        inputs = self.processor.apply_transcription_request(audio=[waveform], language=[language]).to(self.model.device, self.model.dtype)
        with torch.inference_mode():
            output = self.model.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
        generated = output[:, inputs["input_ids"].shape[1] :]
        parsed = self.processor.decode(generated, return_format="parsed")[0]
        return TranscriptionResult(text=str(parsed.get("transcription") or "").strip(), language=parsed.get("language") or None)


class PipelineRecognizer:
    """Any model the transformers speech-recognition pipeline accepts."""

    def __init__(self, location: Path | str, device: str) -> None:
        """Build the speech-recognition pipeline on the selected device."""
        from transformers import pipeline  # noqa: PLC0415

        self.pipe = pipeline("automatic-speech-recognition", model=str(location), device=device, dtype=_torch_dtype(device))

    def transcribe(self, waveform: np.ndarray, *, language: str | None, max_new_tokens: int) -> TranscriptionResult:
        """Transcribe one waveform, chunking long audio where the model supports it."""
        kwargs: dict[str, Any] = {}
        pipeline_type = getattr(self.pipe, "type", "")
        if pipeline_type in CHUNKING_PIPELINE_TYPES:
            kwargs.update(chunk_length_s=30, batch_size=4)
        if pipeline_type == "seq2seq_whisper":
            kwargs["return_language"] = True
            kwargs["generate_kwargs"] = {"language": language} if language else {}
        result = cast(dict[str, Any], self.pipe({"raw": waveform, "sampling_rate": SPEECH_SAMPLING_RATE}, **kwargs))
        chunks = result.get("chunks") or []
        detected = next((chunk.get("language") for chunk in chunks if chunk.get("language")), None)
        return TranscriptionResult(text=str(result.get("text") or "").strip(), language=language or detected)


def model_type(location: Path | str) -> str:
    """Return the transformers ``model_type`` of a model directory or Hub ID."""
    from transformers import AutoConfig  # noqa: PLC0415

    return str(getattr(AutoConfig.from_pretrained(location), "model_type", ""))


def load_recognizer(profile: TranscriptionModelProfile) -> SpeechRecognizer:
    """Load the recognizer that matches the profile's model type."""
    location = profile.model_location()
    device = _torch_device(profile.device)
    if model_type(location) == "qwen3_asr":
        return Qwen3AsrRecognizer(location, device)
    return PipelineRecognizer(location, device)


def split_at_quiet_points(waveform: np.ndarray, *, sampling_rate: int, max_seconds: float, search_seconds: float = 5.0) -> list[np.ndarray]:
    """Split long audio into segments of at most ``max_seconds``.

    Each cut falls at the quietest 100 ms window near the end of the segment,
    so it rarely splits a word.
    """
    max_length = int(max_seconds * sampling_rate)
    if waveform.size <= max_length:
        return [waveform]
    window = max(1, sampling_rate // 10)
    segments: list[np.ndarray] = []
    start = 0
    while waveform.size - start > max_length:
        end = start + max_length
        search_start = max(start + max_length // 2, end - int(search_seconds * sampling_rate))
        region = waveform[search_start:end]
        frames = region[: region.size // window * window].reshape(-1, window)
        cut = search_start + int(np.argmin(np.square(frames).mean(axis=1))) * window + window // 2 if frames.size else end
        segments.append(waveform[start:cut])
        start = cut
    segments.append(waveform[start:])
    return segments


def _is_unspaced_script(character: str) -> bool:
    code = ord(character)
    return 0x3000 <= code <= 0x30FF or 0x3400 <= code <= 0x4DBF or 0x4E00 <= code <= 0x9FFF or 0xFF00 <= code <= 0xFFEF


def join_transcripts(parts: Sequence[str]) -> str:
    """Join segment transcripts, adding spaces except between Chinese or Japanese text."""
    text = ""
    for part in (item.strip() for item in parts):
        if not part:
            continue
        if text and not (_is_unspaced_script(text[-1]) and _is_unspaced_script(part[0])):
            text += " "
        text += part
    return text


class TranscriptionService:
    """Load one speech model at a time and transcribe waveforms with it."""

    def __init__(
        self, settings: TranscriptionSettings = TRANSCRIPTION_SETTINGS, loader: Callable[[TranscriptionModelProfile], SpeechRecognizer] = load_recognizer
    ) -> None:
        """Use ``loader`` to build recognizers from profiles; tests replace it."""
        self.settings = settings
        self._loader = loader
        self._lock = threading.RLock()
        self._loaded_id: str | None = None
        self._recognizer: SpeechRecognizer | None = None
        self._last_used = 0.0
        self._idle_timer: threading.Timer | None = None

    @property
    def loaded_model(self) -> str | None:
        """Return the ID of the loaded model, if any."""
        return self._loaded_id

    def ensure_loaded(self, profile: TranscriptionModelProfile) -> SpeechRecognizer:
        """Load ``profile`` unless it is already loaded, releasing any other model."""
        with self._lock:
            if self._recognizer is not None and self._loaded_id == profile.id:
                return self._recognizer
            self._unload_locked()
            self._recognizer = self._loader(profile)
            self._loaded_id = profile.id
            self._last_used = time.monotonic()
            return self._recognizer

    def transcribe(
        self,
        profile: TranscriptionModelProfile,
        waveform: np.ndarray,
        *,
        check_cancelled: Callable[[], None] | None = None,
    ) -> TranscriptionResult:
        """Transcribe one waveform, splitting long audio at quiet points."""
        with self._lock:
            recognizer = self.ensure_loaded(profile)
            try:
                segments = split_at_quiet_points(waveform, sampling_rate=SPEECH_SAMPLING_RATE, max_seconds=self.settings.chunk_seconds)
                texts: list[str] = []
                languages: list[str] = []
                for segment in segments:
                    if check_cancelled is not None:
                        check_cancelled()
                    seconds = segment.size / SPEECH_SAMPLING_RATE
                    max_new_tokens = profile.max_new_tokens or max(MIN_NEW_TOKENS, int(seconds * TOKENS_PER_SECOND))
                    result = recognizer.transcribe(segment, language=profile.language, max_new_tokens=max_new_tokens)
                    texts.append(result.text)
                    if result.language:
                        languages.append(result.language)
            finally:
                self._last_used = time.monotonic()
                self._schedule_unload()
        return TranscriptionResult(text=join_transcripts(texts), language=languages[0] if languages else None)

    def unload(self, *, only_if_idle: bool = False) -> None:
        """Release the loaded model and its GPU memory."""
        with self._lock:
            if only_if_idle and time.monotonic() - self._last_used < self.settings.idle_unload_seconds:
                return
            self._unload_locked()

    def _unload_locked(self) -> None:
        if self._idle_timer is not None:
            self._idle_timer.cancel()
            self._idle_timer = None
        if self._recognizer is None:
            return
        self._recognizer = None
        self._loaded_id = None
        gc.collect()
        with contextlib.suppress(Exception):
            import torch  # noqa: PLC0415

            if torch.cuda.is_available():
                torch.cuda.empty_cache()

    def _schedule_unload(self) -> None:
        if self._idle_timer is not None:
            self._idle_timer.cancel()
        if self.settings.idle_unload_seconds <= 0:
            return
        timer = threading.Timer(self.settings.idle_unload_seconds, self.unload, kwargs={"only_if_idle": True})
        timer.daemon = True
        timer.start()
        self._idle_timer = timer


TRANSCRIPTION_SERVICE = TranscriptionService()


def transcript_cache_path(data: bytes, profile: TranscriptionModelProfile) -> Path:
    """Return the cache file for one audio payload and model configuration."""
    digest = hashlib.sha256()
    for part in (TRANSCRIPT_CACHE_VERSION, profile.model, profile.language or "", str(profile.max_new_tokens or "")):
        digest.update(part.encode("utf-8"))
        digest.update(b"\0")
    digest.update(hashlib.sha256(data).digest())
    key = digest.hexdigest()
    return TRANSCRIPT_CACHE_DIR / key[:2] / f"{key}.json"


def _read_cached_transcript(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) and isinstance(payload.get("text"), str) else None


def _write_cached_transcript(path: Path, payload: dict[str, Any]) -> None:
    from .cache import prune_cache_dir  # noqa: PLC0415

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)
    prune_cache_dir(TRANSCRIPT_CACHE_DIR, TRANSCRIPT_CACHE_MAX_BYTES, preserve=(path,))


def load_cell_audio(reference: AudioCellReference) -> bytes:
    """Read the encoded audio that one dataset or SQL result cell holds.

    Raises:
        AudioDecodeError: The cell is missing or holds no readable audio.
    """
    from fastapi import HTTPException  # noqa: PLC0415

    from .files import resolve_data_file  # noqa: PLC0415
    from .readers import fetch_raw_row  # noqa: PLC0415
    from .sql import fetch_raw_query_row_guarded  # noqa: PLC0415

    try:
        path = resolve_data_file(reference.file)
        if reference.row_id is not None:
            columns, values = fetch_raw_row(path, reference.row_id)
        elif reference.sql:
            columns, values = fetch_raw_query_row_guarded(path=path, sql=reference.sql, offset=max(0, reference.offset or 0))
        else:
            raise AudioDecodeError("row_id or sql is required")
    except HTTPException as exc:
        raise AudioDecodeError(str(exc.detail)) from exc
    if reference.column not in columns:
        raise AudioDecodeError(f"column not found: {reference.column}")
    try:
        value = value_at_pointer(values[columns.index(reference.column)], list(reference.pointer))
    except KeyError as exc:
        raise AudioDecodeError("audio value not found in the row") from exc
    return audio_source_bytes(value, dataset_path=path, allowed_roots=FILE_SERVE_ROOTS)


def transcribe_cells(
    items: Sequence[tuple[str, AudioCellReference]],
    *,
    model: str | None,
    context: Any,
    service: TranscriptionService = TRANSCRIPTION_SERVICE,
    read_audio: Callable[[AudioCellReference], bytes] = load_cell_audio,
) -> dict[str, Any]:
    """Transcribe audio cells in order, reporting progress and per-item errors.

    Results are cached by audio content and model configuration, so the same
    clip is not transcribed twice.

    Raises:
        KeyError: ``model`` names no configured profile.
        JobCancelledError: The job was cancelled.
    """
    profile = service.settings.profile(model)
    results: list[dict[str, Any]] = []
    total = len(items)
    for index, (item_id, reference) in enumerate(items):
        context.check_cancelled()
        context.update(progress=index / total, message=f"Transcribing {index + 1} of {total} with {profile.display_label}")
        try:
            data = read_audio(reference)
            cache_path = transcript_cache_path(data, profile)
            cached = _read_cached_transcript(cache_path)
            if cached is not None:
                results.append({"id": item_id, **cached, "cached": True})
                continue
            waveform = decode_audio_waveform(data, max_seconds=service.settings.max_audio_seconds)
            if service.loaded_model != profile.id:
                context.update(progress=index / total, message=f"Loading {profile.display_label}")
            result = service.transcribe(profile, waveform, check_cancelled=context.check_cancelled)
            payload = {"text": result.text, "language": result.language, "duration": round(waveform.size / SPEECH_SAMPLING_RATE, 3)}
            _write_cached_transcript(cache_path, payload)
            results.append({"id": item_id, **payload, "cached": False})
        except JobCancelledError:
            raise
        except AudioDecodeError as exc:
            results.append({"id": item_id, "error": str(exc)})
    context.update(progress=1.0, message="Transcription finished")
    return {"model": profile.id, "model_label": profile.display_label, "items": results}

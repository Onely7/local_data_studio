"""Capability-driven encoder adapters used by Atlas projection jobs."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
from embedding_atlas.embedding import create_embedder
from PIL import Image
from sentence_transformers import SentenceTransformer

from ..audio_decode import SPEECH_SAMPLING_RATE, decode_audio_waveform
from ..embedder_capabilities import BackendName, analyze_model_capabilities, transformers_pooling_spec
from .contracts import AtlasModality, AtlasOptions
from .prompts import PromptedEmbeddingValue

# Sentence Transformers leaves max_seq_length unbounded for checkpoints whose
# tokenizer reports no limit; 8,192 tokens is EmbeddingGemma 2's context.
SENTENCE_TRANSFORMER_MAX_SEQ_LENGTH = 8_192
# EmbeddingGemma 2 turns one second of audio into 25 tokens, so 300 seconds
# fit its 8,192-token context; longer clips are embedded from their start.
ATLAS_AUDIO_MAX_SECONDS = 300
# Audio sequences are long, so Sentence Transformers encodes a few at a time.
AUDIO_ENCODE_BATCH_SIZE = 4


@dataclass(frozen=True, slots=True)
class AtlasEmbeddingBackend:
    """Resolved backend and adapter selected from local model capabilities."""

    name: BackendName
    adapter: str | None

    @classmethod
    def for_model(
        cls,
        *,
        modality: AtlasModality,
        model_path: Path,
        options: AtlasOptions,
    ) -> AtlasEmbeddingBackend:
        """Resolve an available backend without model-name special cases."""
        capabilities = analyze_model_capabilities(model_path, allow_remote_code=options.trust_remote_code)
        selected = options.backend or capabilities.default_backend
        if selected not in {"transformers", "sentence-transformers"}:
            raise ValueError("no supported embedding backend is available")
        backend_name = selected
        capability = capabilities.backend(backend_name)
        if not capability.available or modality not in capability.modalities:
            raise ValueError(f"{selected} cannot embed {modality} values: {capability.reason}")
        return cls(backend_name, capability.adapter)


def effective_embedder_for_modality(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> str:
    """Return the capability-validated backend included in cache identity."""
    return AtlasEmbeddingBackend.for_model(modality=modality, model_path=model_path, options=options).name


def load_sentence_transformer_model(model_path: Path, options: AtlasOptions) -> Any:
    """Load a local SentenceTransformer without network fallback.

    Without CUDA, weights load as float32: bfloat16 checkpoints such as
    EmbeddingGemma 2 are slow on CPUs, and their model cards rule out
    float16. A missing or unbounded sequence limit is capped so one long value
    cannot allocate an unbounded attention matrix.
    """
    import torch  # noqa: PLC0415

    model = SentenceTransformer(
        str(model_path),
        local_files_only=True,
        trust_remote_code=options.trust_remote_code,
        model_kwargs=None if torch.cuda.is_available() else {"dtype": torch.float32},
    )
    if not model.max_seq_length or model.max_seq_length > SENTENCE_TRANSFORMER_MAX_SEQ_LENGTH:
        model.max_seq_length = SENTENCE_TRANSFORMER_MAX_SEQ_LENGTH
    return model


def audio_sampling_rate(model_path: Path) -> int:
    """Return the sampling rate a model's audio feature extractor expects."""
    for name in ("preprocessor_config.json", "processor_config.json"):
        try:
            config = json.loads((model_path / name).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        sections = [config, config.get("feature_extractor")] if isinstance(config, dict) else []
        for section in sections:
            rate = section.get("sampling_rate") if isinstance(section, dict) else None
            if isinstance(rate, int) and rate > 0:
                return rate
    return SPEECH_SAMPLING_RATE


def _sentence_transformer_input(value: Any, modality: AtlasModality, *, multimodal: bool, sampling_rate: int = SPEECH_SAMPLING_RATE) -> Any:
    if modality == "audio":
        # Resample here: Sentence Transformers does not check the rate, and one
        # batch may hold only one rate.
        data = value.get("audio_bytes") if isinstance(value, dict) else value
        waveform = decode_audio_waveform(bytes(data), sampling_rate=sampling_rate, max_seconds=ATLAS_AUDIO_MAX_SECONDS, truncate=True)
        return {"array": waveform, "sampling_rate": sampling_rate}
    if modality != "image":
        # Multimodal models treat strings that look like media paths or URLs
        # as media, so text rows are marked as text explicitly.
        return {"text": value} if multimodal and isinstance(value, str) else value
    if isinstance(value, dict) and isinstance(value.get("bytes"), bytes | bytearray):
        return {"image": Image.open(BytesIO(bytes(value["bytes"]))).convert("RGB")}
    if isinstance(value, dict) and value.get("image") is not None:
        return value
    return {"image": value}


def create_sentence_transformer_embedder(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> Any:
    """Create a reusable SentenceTransformer adapter with per-row prompt support."""
    sentence_model = load_sentence_transformer_model(model_path, options)
    capability = analyze_model_capabilities(model_path, allow_remote_code=options.trust_remote_code).sentence_transformers
    multimodal = any(modality != "text" for modality in capability.modalities)
    sampling_rate = audio_sampling_rate(model_path) if modality == "audio" else SPEECH_SAMPLING_RATE

    async def embed(batch: list[Any], *, model: str | None, embedder_args: dict) -> np.ndarray:  # noqa: ARG001
        """Encode prompt groups and restore their original row ordering."""
        grouped: dict[str | None, list[tuple[int, Any]]] = defaultdict(list)
        for index, item in enumerate(batch):
            if isinstance(item, PromptedEmbeddingValue):
                value, prompt = item.value, item.prompt
            else:
                value, prompt = item, options.prompt
            if modality == "audio":
                prompt = None
            grouped[prompt].append((index, _sentence_transformer_input(value, modality, multimodal=multimodal, sampling_rate=sampling_rate)))
        ordered: list[np.ndarray | None] = [None] * len(batch)
        for prompt, entries in grouped.items():
            values = [value for _, value in entries]
            kwargs: dict[str, Any] = {
                "show_progress_bar": False,
                "batch_size": max(len(values), 1),
            }
            if prompt is not None:
                kwargs["prompt"] = prompt
            if modality == "audio":
                # Without max_length the feature extractor cuts audio at 30 seconds.
                kwargs["batch_size"] = min(len(values), AUDIO_ENCODE_BATCH_SIZE)
                kwargs["processing_kwargs"] = {"audio": {"max_length": max((len(value["array"]) for value in values), default=1)}}
            encoded = np.asarray(sentence_model.encode(values, **kwargs), dtype=np.float32)
            for (index, _), vector in zip(entries, encoded):
                ordered[index] = vector
        if any(vector is None for vector in ordered):
            raise ValueError("Sentence Transformers returned incomplete embeddings")
        complete = [vector for vector in ordered if vector is not None]
        return np.stack(complete).astype(np.float32)

    return embed


def load_transformers_components(model_path: Path, *, multimodal: bool, trust_remote_code: bool) -> tuple[Any, Any, Any]:
    """Load one local AutoModel and matching processor for an adapter session."""
    import torch  # noqa: PLC0415
    from transformers import AutoModel, AutoProcessor, AutoTokenizer  # noqa: PLC0415

    kwargs = {"local_files_only": True, "trust_remote_code": trust_remote_code}
    processor = AutoProcessor.from_pretrained(model_path, **kwargs) if multimodal else AutoTokenizer.from_pretrained(model_path, **kwargs)
    model = AutoModel.from_pretrained(model_path, **kwargs)
    device = torch.device("cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu")
    model.to(device).eval()
    return model, processor, device


def load_transformers_image_components(model_path: Path, *, trust_remote_code: bool) -> tuple[Any, Any, Any]:
    """Load a local image AutoModel and matching image processor."""
    import torch  # noqa: PLC0415
    from transformers import AutoImageProcessor, AutoModel  # noqa: PLC0415

    kwargs = {"local_files_only": True, "trust_remote_code": trust_remote_code}
    processor = AutoImageProcessor.from_pretrained(model_path, **kwargs)
    model = AutoModel.from_pretrained(model_path, **kwargs)
    device = torch.device("cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu")
    model.to(device).eval()
    return model, processor, device


def _pil_image(value: Any) -> Image.Image:
    if isinstance(value, dict) and isinstance(value.get("bytes"), bytes | bytearray):
        return Image.open(BytesIO(bytes(value["bytes"]))).convert("RGB")
    if isinstance(value, Image.Image):
        return value.convert("RGB")
    raise ValueError("Transformers image input must contain decoded bytes")


def _pool_token_embeddings(token_embeddings: Any, attention_mask: Any, modes: tuple[str, ...]) -> Any:
    import torch  # noqa: PLC0415

    mask = attention_mask.to(token_embeddings.device).unsqueeze(-1)
    pooled: list[Any] = []
    for mode in modes:
        if mode == "cls":
            pooled.append(token_embeddings[:, 0])
        elif mode == "max":
            pooled.append(token_embeddings.masked_fill(mask == 0, -torch.inf).max(dim=1).values)
        elif mode in {"mean", "mean_sqrt_len_tokens"}:
            denominator = mask.sum(dim=1).clamp(min=1)
            if mode == "mean_sqrt_len_tokens":
                denominator = denominator.sqrt()
            pooled.append((token_embeddings * mask).sum(dim=1) / denominator)
        elif mode == "weightedmean":
            positions = torch.arange(1, token_embeddings.shape[1] + 1, device=token_embeddings.device).view(1, -1, 1)
            weights = mask * positions
            pooled.append((token_embeddings * weights).sum(dim=1) / weights.sum(dim=1).clamp(min=1))
        elif mode == "lasttoken":
            indices = attention_mask.sum(dim=1).clamp(min=1).long() - 1
            rows = torch.arange(token_embeddings.shape[0], device=token_embeddings.device)
            pooled.append(token_embeddings[rows, indices])
    return torch.cat(pooled, dim=-1) if len(pooled) > 1 else pooled[0]


def create_transformers_pooling_embedder(
    modality: AtlasModality,
    model_path: Path,
    options: AtlasOptions,
    *,
    multimodal: bool,
) -> Any:
    """Create an AutoModel adapter reproducing saved Pooling and Normalize steps."""
    import torch  # noqa: PLC0415
    from torch.nn import functional  # noqa: PLC0415

    spec = transformers_pooling_spec(model_path)
    if spec is None:
        raise ValueError("saved pooling configuration is unavailable")
    capabilities = analyze_model_capabilities(model_path, allow_remote_code=options.trust_remote_code)
    transformer_model, processor, device = load_transformers_components(
        model_path,
        multimodal=multimodal,
        trust_remote_code=options.trust_remote_code,
    )

    async def embed(batch: list[Any], *, model: str | None, embedder_args: dict) -> np.ndarray:  # noqa: ARG001
        """Process one batch and apply the statically verified pooling contract."""
        values = [item.value if isinstance(item, PromptedEmbeddingValue) else item for item in batch]
        if any(isinstance(item, PromptedEmbeddingValue) and item.prompt for item in batch):
            raise ValueError("prompt overrides are not supported by the transformers backend")
        if modality == "image":
            images = [_pil_image(value) for value in values]
            if multimodal and hasattr(processor, "apply_chat_template"):
                default_prompt = capabilities.default_prompt or "Represent the user's input."
                messages = [
                    [
                        {"role": "system", "content": [{"type": "text", "text": default_prompt}]},
                        {"role": "user", "content": [{"type": "image", "image": image}]},
                    ]
                    for image in images
                ]
                text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
                inputs = processor(text=text, images=images, padding=True, return_tensors="pt")
            else:
                inputs = processor(images=images, return_tensors="pt")
        else:
            # Multimodal processors take images as their first positional argument.
            inputs = processor(text=[str(value) for value in values], padding=True, truncation=True, return_tensors="pt")
        inputs = {key: value.to(device) for key, value in inputs.items()}
        with torch.no_grad():
            outputs = transformer_model(**inputs)
            token_embeddings = outputs.last_hidden_state if hasattr(outputs, "last_hidden_state") else outputs[0]
            attention_mask = inputs.get("attention_mask")
            if attention_mask is None:
                attention_mask = torch.ones(token_embeddings.shape[:2], dtype=torch.long, device=device)
            embeddings = _pool_token_embeddings(token_embeddings, attention_mask, spec.modes)
            if spec.normalize:
                embeddings = functional.normalize(embeddings, p=2, dim=-1)
        return embeddings.detach().cpu().float().numpy()

    return embed


def create_transformers_image_pooler_embedder(model_path: Path, options: AtlasOptions) -> Any:
    """Create an image adapter that uses the model's declared pooled output."""
    import torch  # noqa: PLC0415

    transformer_model, processor, device = load_transformers_image_components(
        model_path,
        trust_remote_code=options.trust_remote_code,
    )

    async def embed(batch: list[Any], *, model: str | None, embedder_args: dict) -> np.ndarray:  # noqa: ARG001
        """Process one image batch and return the model-owned pooled vectors."""
        values = [item.value if isinstance(item, PromptedEmbeddingValue) else item for item in batch]
        images = [_pil_image(value) for value in values]
        inputs = processor(images=images, return_tensors="pt")
        inputs = {key: value.to(device) for key, value in inputs.items()}
        with torch.no_grad():
            outputs = transformer_model(**inputs)
        pooled = getattr(outputs, "pooler_output", None)
        if pooled is None:
            raise ValueError("Transformers image model did not return the verified pooler_output")
        return pooled.detach().cpu().float().numpy()

    return embed


def resolve_embedder_callable(modality: AtlasModality, model_path: Path, options: AtlasOptions) -> tuple[Any, dict[str, bool]]:
    """Create one capability-selected encoder callable for an Atlas job."""
    backend = AtlasEmbeddingBackend.for_model(modality=modality, model_path=model_path, options=options)
    embedder_args = {"trust_remote_code": options.trust_remote_code, "local_files_only": True}
    if backend.name == "sentence-transformers":
        return create_sentence_transformer_embedder(modality, model_path, options), embedder_args
    if backend.adapter == "auto-pooling":
        return create_transformers_pooling_embedder(modality, model_path, options, multimodal=False), embedder_args
    if backend.adapter == "auto-pooling-multimodal":
        return create_transformers_pooling_embedder(modality, model_path, options, multimodal=True), embedder_args
    if backend.adapter == "auto-image-pooler":
        return create_transformers_image_pooler_embedder(model_path, options), embedder_args
    return create_embedder("transformers", modality=modality, model=str(model_path), embedder_args=embedder_args), embedder_args

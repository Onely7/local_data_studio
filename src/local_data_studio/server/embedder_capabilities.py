"""Static capability detection for locally installed embedding models."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Literal

BackendName = Literal["sentence-transformers", "transformers"]
SUPPORTED_POOLING_MODES = {"cls", "max", "mean", "mean_sqrt_len_tokens", "weightedmean", "lasttoken"}
BUILTIN_SENTENCE_PREFIXES = ("sentence_transformers.",)
CONFIG_FILES = (
    "config.json",
    "modules.json",
    "config_sentence_transformers.json",
    "sentence_bert_config.json",
    "tokenizer_config.json",
    "preprocessor_config.json",
)
WEIGHT_FILES = ("model.safetensors", "pytorch_model.bin")


@dataclass(frozen=True, slots=True)
class BackendCapability:
    """One backend's statically verified execution capability."""

    status: str
    available: bool
    modalities: tuple[str, ...]
    reason: str
    adapter: str | None = None

    def to_response(self) -> dict[str, Any]:
        """Return browser-safe metadata without exposing internal adapter details."""
        return {
            "status": self.status,
            "available": self.available,
            "modalities": list(self.modalities),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ModelCapabilities:
    """Immutable backend, prompt, and fingerprint metadata for one model root."""

    fingerprint: str
    sentence_transformers: BackendCapability
    transformers: BackendCapability
    default_backend: BackendName | None
    default_prompt_name: str | None
    default_prompt: str | None

    def backend(self, name: BackendName) -> BackendCapability:
        """Return capability metadata for an API backend name."""
        return self.sentence_transformers if name == "sentence-transformers" else self.transformers

    def to_response(self) -> dict[str, Any]:
        """Return JSON-compatible capability metadata for model discovery."""
        return {
            "backends": {
                "sentence-transformers": self.sentence_transformers.to_response(),
                "transformers": self.transformers.to_response(),
            },
            "default_backend": self.default_backend,
            "default_prompt_name": self.default_prompt_name,
            "default_prompt": self.default_prompt,
            "capability_fingerprint": self.fingerprint,
        }


def library_versions() -> dict[str, str]:
    """Return installed library versions used by capability detection."""
    versions: dict[str, str] = {}
    for package in ("transformers", "sentence-transformers"):
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = "unavailable"
    return versions


def model_capability_fingerprint(path: Path) -> str:
    """Fingerprint relevant configuration contents and weight file metadata."""
    digest = hashlib.sha256()
    for relative in CONFIG_FILES:
        candidate = path / relative
        digest.update(relative.encode())
        try:
            digest.update(candidate.read_bytes())
        except OSError:
            digest.update(b"<missing>")
    modules = _read_json(path / "modules.json")
    if isinstance(modules, list):
        for module in modules:
            if not isinstance(module, dict) or not isinstance(module.get("path"), str):
                continue
            module_path = module["path"]
            if module_path:
                config_path = path / module_path / "config.json"
                digest.update(config_path.relative_to(path).as_posix().encode())
                try:
                    digest.update(config_path.read_bytes())
                except OSError:
                    digest.update(b"<missing>")
    for relative in WEIGHT_FILES:
        candidate = path / relative
        try:
            stat = candidate.stat()
        except OSError:
            continue
        digest.update(f"{relative}:{stat.st_size}:{stat.st_mtime_ns}".encode())
    for package, package_version in sorted(library_versions().items()):
        digest.update(f"{package}:{package_version}".encode())
    return digest.hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _module_modalities(path: Path) -> tuple[str, ...]:
    config = _read_json(path / "sentence_bert_config.json")
    modality_config = config.get("modality_config") if isinstance(config, dict) else None
    if isinstance(modality_config, dict):
        values = tuple(name for name in ("text", "image", "audio", "video") if name in modality_config)
        if values:
            return values
    return ("text",)


def _fallback_modalities(path: Path) -> tuple[str, ...]:
    modalities: list[str] = []
    if (path / "tokenizer_config.json").exists():
        modalities.append("text")
    if (path / "preprocessor_config.json").exists():
        modalities.append("image")
    return tuple(modalities or ["text"])


def _sentence_module_analysis(path: Path) -> tuple[str, bool, tuple[str, ...], str, list[dict[str, Any]] | None]:  # noqa: PLR0911
    modules = _read_json(path / "modules.json")
    if not isinstance(modules, list) or not modules:
        if (path / "config_sentence_transformers.json").exists():
            return "metadata_only", False, (), "Sentence Transformers metadata exists without modules.json.", None
        return "generic_fallback", True, _fallback_modalities(path), "Sentence Transformers can add default pooling to this Transformers model.", None
    normalized: list[dict[str, Any]] = []
    for expected_index, module in enumerate(modules):
        if not isinstance(module, dict):
            return "unknown", False, (), "modules.json contains a non-object entry.", None
        normalized_module: dict[str, Any] = {str(key): value for key, value in module.items()}
        module_type = normalized_module.get("type")
        module_path = normalized_module.get("path", "")
        if normalized_module.get("idx") != expected_index or not isinstance(module_type, str) or not isinstance(module_path, str):
            return "unknown", False, (), "modules.json has invalid indexes, types, or paths.", None
        if not module_type.startswith(BUILTIN_SENTENCE_PREFIXES):
            return "unknown", False, (), "A custom Sentence Transformers module requires explicit code trust.", None
        if module_type.endswith("Pooling") and not (path / module_path / "config.json").is_file():
            return "unknown", False, (), "A Pooling module is missing its config.json.", None
        if module_type.endswith("Dense") and not (path / module_path).is_dir():
            return "unknown", False, (), "A Dense module directory is missing.", None
        normalized.append(normalized_module)
    return "native", True, _module_modalities(path), "modules.json defines a complete built-in Sentence Transformers pipeline.", normalized


def _pooling_modes(path: Path, modules: list[dict[str, Any]]) -> tuple[tuple[str, ...], bool] | None:
    pooling = [module for module in modules if str(module.get("type", "")).endswith("Pooling")]
    if len(pooling) != 1:
        return None
    config = _read_json(path / str(pooling[0].get("path", "")) / "config.json")
    if not isinstance(config, dict) or config.get("include_prompt", True) is False:
        return None
    if isinstance(config.get("pooling_mode"), str):
        modes = (config["pooling_mode"],)
    else:
        names = {
            "pooling_mode_cls_token": "cls",
            "pooling_mode_max_tokens": "max",
            "pooling_mode_mean_tokens": "mean",
            "pooling_mode_mean_sqrt_len_tokens": "mean_sqrt_len_tokens",
            "pooling_mode_weightedmean_tokens": "weightedmean",
            "pooling_mode_lasttoken": "lasttoken",
        }
        modes = tuple(mode for key, mode in names.items() if config.get(key) is True)
    if not modes or any(mode not in SUPPORTED_POOLING_MODES for mode in modes):
        return None
    normalize = any(str(module.get("type", "")).endswith("Normalize") for module in modules)
    return modes, normalize


def _resolve_auto_config(path: Path) -> tuple[Any | None, bool]:
    config_json = _read_json(path / "config.json")
    remote_code = isinstance(config_json, dict) and isinstance(config_json.get("auto_map"), dict)
    try:
        from transformers import AutoConfig  # noqa: PLC0415

        return AutoConfig.from_pretrained(path, local_files_only=True, trust_remote_code=False), remote_code
    except Exception:
        return None, remote_code


def _has_auto_model(config: Any) -> tuple[bool, bool]:
    try:
        from transformers import AutoModel, AutoModelForMultimodalLM  # noqa: PLC0415

        AutoModel._model_mapping[type(config)]
        direct = True
        try:
            AutoModelForMultimodalLM._model_mapping[type(config)]
            multimodal = True
        except KeyError:
            multimodal = False
        return direct, multimodal
    except (ImportError, KeyError):
        return False, False


def _transformers_analysis(
    path: Path,
    sentence_status: str,
    sentence_modalities: tuple[str, ...],
    modules: list[dict[str, Any]] | None,
) -> BackendCapability:
    config, remote_code = _resolve_auto_config(path)
    if config is None:
        status = "remote_code" if remote_code else "unknown"
        reason = "Model configuration requires remote code." if remote_code else "AutoConfig cannot resolve the local model."
        return BackendCapability(status, False, (), reason)
    direct, multimodal = _has_auto_model(config)
    if not direct:
        return BackendCapability("unsupported", False, (), "No installed Transformers AutoModel mapping accepts this config.")
    modalities = sentence_modalities if sentence_status == "native" else _fallback_modalities(path)
    if modules is None:
        adapter = "pipeline"
    else:
        unsupported_modules = [module for module in modules if not str(module.get("type", "")).endswith(("Transformer", "Pooling", "Normalize"))]
        pooling = _pooling_modes(path, modules)
        if unsupported_modules or pooling is None:
            return BackendCapability(
                "backbone_only",
                False,
                modalities,
                "AutoModel can load the backbone, but the saved embedding pipeline cannot be reproduced safely.",
            )
        adapter = "auto-pooling-multimodal" if multimodal and "image" in modalities else "auto-pooling"
    return BackendCapability("direct", True, modalities, "AutoModel, input processing, and embedding post-processing are supported.", adapter)


def _prompt_metadata(path: Path) -> tuple[str | None, str | None]:
    config = _read_json(path / "config_sentence_transformers.json")
    if not isinstance(config, dict):
        return None, None
    default_name = config.get("default_prompt_name")
    prompts = config.get("prompts")
    if not isinstance(default_name, str) or not isinstance(prompts, dict):
        return None, None
    prompt = prompts.get(default_name)
    return default_name, prompt if isinstance(prompt, str) else None


@lru_cache(maxsize=256)
def _analyze_cached(path_text: str, fingerprint: str) -> ModelCapabilities:
    path = Path(path_text)
    sentence_status, sentence_available, modalities, sentence_reason, modules = _sentence_module_analysis(path)
    sentence = BackendCapability(sentence_status, sentence_available, modalities, sentence_reason, "sentence-transformers" if sentence_available else None)
    transformers = _transformers_analysis(path, sentence_status, modalities, modules)
    default_backend: BackendName | None = None
    if sentence.available:
        default_backend = "sentence-transformers"
    elif transformers.available:
        default_backend = "transformers"
    default_prompt_name, default_prompt = _prompt_metadata(path)
    return ModelCapabilities(fingerprint, sentence, transformers, default_backend, default_prompt_name, default_prompt)


def analyze_model_capabilities(path: Path) -> ModelCapabilities:
    """Inspect a local model without loading weights or repository Python code."""
    resolved = path.resolve()
    fingerprint = model_capability_fingerprint(resolved)
    return _analyze_cached(str(resolved), fingerprint)

"""Lightweight discovery helpers for locally installed embedder models."""

from pathlib import Path

from fastapi import HTTPException

MODEL_MARKER_FILES = (
    "config.json",
    "modules.json",
    "tokenizer_config.json",
    "preprocessor_config.json",
    "model.safetensors",
    "pytorch_model.bin",
)


def is_model_directory(path: Path) -> bool:
    """Return whether a directory looks like a Hugging Face model root."""
    if not path.is_dir() or path.name.startswith("."):
        return False
    return any((path / marker).exists() for marker in MODEL_MARKER_FILES)


def model_label(path: Path, root: Path) -> str:
    """Return the model dropdown label relative to the configured model root."""
    return path.relative_to(root).as_posix()


def discover_embedder_models(root: Path) -> list[dict[str, str]]:
    """Return locally installed encoder models without importing the ML stack."""
    root.mkdir(parents=True, exist_ok=True)
    models: list[dict[str, str]] = []
    discovered_roots: list[Path] = []
    paths = sorted(
        root.rglob("*"),
        key=lambda item: (len(item.relative_to(root).parts), item.as_posix()),
    )
    for path in paths:
        if any(model_root == path or model_root in path.parents for model_root in discovered_roots):
            continue
        if not is_model_directory(path):
            continue
        discovered_roots.append(path)
        label = model_label(path, root)
        models.append(
            {
                "name": label,
                "value": label,
                "path": str(path),
            }
        )
    return models


def resolve_embedder_model(model: str, root: Path) -> Path:
    """Resolve a model dropdown value to a directory below the model root."""
    model_name = model.strip()
    if not model_name:
        raise HTTPException(status_code=400, detail="model is required")
    candidate = (root / model_name).resolve()
    resolved_root = root.resolve()
    if resolved_root != candidate and resolved_root not in candidate.parents:
        raise HTTPException(status_code=400, detail="invalid model path")
    if not is_model_directory(candidate):
        raise HTTPException(status_code=404, detail="model not found under models/embedder")
    return candidate

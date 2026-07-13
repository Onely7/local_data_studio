"""Encoder-only Atlas projection input construction."""

from __future__ import annotations

import tempfile
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ..config import ATLAS_TEXT_MAX_CHARS
from .contracts import ATLAS_EMBED_INPUT_COLUMN, AtlasModality
from .image_values import image_value_to_bytes
from .prompts import CompiledPromptTemplate, PromptedEmbeddingValue


def text_for_embedding(value: Any) -> str:
    """Convert a text value to a bounded deterministic encoder string."""
    text = "null" if value is None else str(value)
    return text[:ATLAS_TEXT_MAX_CHARS] if ATLAS_TEXT_MAX_CHARS and len(text) > ATLAS_TEXT_MAX_CHARS else text


class _TextProjectionSequence(Sequence[Any]):
    def __init__(self, data_frame: Any, column: str, prompt_template: CompiledPromptTemplate | None) -> None:
        self._data_frame = data_frame
        self._column = column
        self._prompt_template = prompt_template
        self._prefix = prompt_template.render({}) if prompt_template is not None and not prompt_template.has_placeholders else None

    def __len__(self) -> int:
        return len(self._data_frame)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return [self[item] for item in range(*index.indices(len(self)))]
        row = self._data_frame.iloc[index]
        if self._prompt_template is None:
            return text_for_embedding(row[self._column])
        if self._prompt_template.has_placeholders:
            return PromptedEmbeddingValue(self._prompt_template.render(row.to_dict()), "")
        return PromptedEmbeddingValue(text_for_embedding(row[self._column]), self._prefix or "")


class _ImageSpoolSequence(Sequence[Any]):
    """Index image payloads stored in one temporary file.

    The sequence owns its temporary directory and stream. Call ``close`` after
    projection; repeated calls are safe.
    """

    def __init__(
        self,
        temporary_dir: tempfile.TemporaryDirectory[str],
        offsets: list[tuple[int, int]],
        output_frame: Any,
        prompt_template: CompiledPromptTemplate | None,
    ) -> None:
        self._temporary_dir: tempfile.TemporaryDirectory[str] | None = temporary_dir
        self._offsets = offsets
        self._output_frame = output_frame
        self._prompt_template = prompt_template
        self._stream = (Path(temporary_dir.name) / "images.bin").open("rb")
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._offsets)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return [self[item] for item in range(*index.indices(len(self)))]
        offset, length = self._offsets[index]
        with self._lock:
            self._stream.seek(offset)
            image_bytes = self._stream.read(length)
        value: Any = {"bytes": image_bytes}
        if self._prompt_template is not None:
            value = PromptedEmbeddingValue(value, self._prompt_template.render(self._output_frame.iloc[index].to_dict()))
        return value

    def close(self) -> None:
        """Close the spool and remove its temporary directory."""
        with self._lock:
            if not self._stream.closed:
                self._stream.close()
        if self._temporary_dir is not None:
            self._temporary_dir.cleanup()
            self._temporary_dir = None

    def __del__(self) -> None:
        self.close()


class _ProjectionColumn:
    def __init__(self, values: Sequence[Any]) -> None:
        self._values = values
        self.iloc = self

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, index: int | slice) -> Any:
        return self._values[index]

    def tolist(self) -> list[Any]:
        """Materialize values only for legacy callers that explicitly request it."""
        return list(self._values)

    def to_list(self) -> list[Any]:
        """Return the pandas-compatible spelling of ``tolist``."""
        return self.tolist()


class _ProjectionIloc:
    def __init__(self, column: _ProjectionColumn) -> None:
        self._column = column

    def __getitem__(self, index: tuple[int, int]) -> Any:
        row, column = index
        if column != 0:
            raise IndexError("projection input has one column")
        return self._column[row]


class ProjectionInputFrame:
    """Single-column, DataFrame-compatible view over lazy encoder inputs.

    The object owns any disk-backed image spool. Callers must invoke ``close``
    after embedding; closing a text-backed instance is a no-op.
    """

    def __init__(self, values: Sequence[Any]) -> None:
        """Wrap an owned lazy sequence as a one-column projection frame."""
        self._values = values
        self._column = _ProjectionColumn(values)
        self._extra_columns: dict[str, _ProjectionColumn] = {}
        self.iloc = _ProjectionIloc(self._column)
        self.columns = [ATLAS_EMBED_INPUT_COLUMN]

    def __len__(self) -> int:
        """Return the number of embedding inputs."""
        return len(self._values)

    def __getitem__(self, column: str) -> _ProjectionColumn:
        """Return the hidden embedding column or raise ``KeyError``."""
        if column == ATLAS_EMBED_INPUT_COLUMN:
            return self._column
        try:
            return self._extra_columns[column]
        except KeyError:
            raise KeyError(column) from None

    def __setitem__(self, column: str, values: Sequence[Any]) -> None:
        """Store a compatibility-only derived column without touching inputs."""
        if len(values) != len(self):
            raise ValueError("derived projection columns must match the input row count")
        self._extra_columns[column] = _ProjectionColumn(values)
        if column not in self.columns:
            self.columns.append(column)

    def close(self) -> None:
        """Release a disk-backed sequence when one is owned."""
        close = getattr(self._values, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> ProjectionInputFrame:
        """Return this owned projection input."""
        return self

    def __exit__(self, *_args: object) -> None:
        """Release owned temporary resources."""
        self.close()


def prepare_image_projection_input(
    data_frame: Any,
    *,
    column: str,
    dataset_path: Path,
    prompt_template: CompiledPromptTemplate | None = None,
) -> tuple[ProjectionInputFrame, Any]:
    """Spool readable image bytes and return aligned lazy encoder inputs.

    Rows that cannot be decoded are excluded from both returned objects.
    The returned projection input owns a temporary file and must be closed.

    Raises:
        ValueError: The column is unavailable or contains no readable image.
    """
    try:
        values = data_frame[column]
    except Exception as exc:
        raise ValueError(f"failed to read image column {column}: {exc}") from exc
    temporary_dir = tempfile.TemporaryDirectory(prefix="local-data-studio-atlas-images-")
    spool_path = Path(temporary_dir.name) / "images.bin"
    kept_indices: list[int] = []
    offsets: list[tuple[int, int]] = []
    first_error: str | None = None
    try:
        with spool_path.open("wb") as spool:
            for index, value in enumerate(values):
                try:
                    image_bytes = image_value_to_bytes(value, dataset_path)
                except ValueError as exc:
                    if first_error is None:
                        first_error = f"row {index + 1}: {exc}"
                    continue
                offset = spool.tell()
                spool.write(image_bytes)
                offsets.append((offset, len(image_bytes)))
                kept_indices.append(index)
        if not kept_indices:
            raise ValueError(f"no readable images in column {column}; first error: {first_error or 'no image values found'}")
        output_frame = data_frame.iloc[kept_indices].copy().reset_index(drop=True)
        sequence = _ImageSpoolSequence(temporary_dir, offsets, output_frame, prompt_template)
        return ProjectionInputFrame(sequence), output_frame
    except Exception:
        temporary_dir.cleanup()
        raise


def prepare_projection_input(
    data_frame: Any,
    *,
    column: str,
    modality: AtlasModality,
    dataset_path: Path,
    prompt_template: CompiledPromptTemplate | None = None,
) -> tuple[Any, str, Any]:
    """Separate lazy encoder inputs from an aligned Atlas display frame."""
    if modality == "text":
        values = _TextProjectionSequence(data_frame, column, prompt_template)
        return ProjectionInputFrame(values), ATLAS_EMBED_INPUT_COLUMN, data_frame
    if modality != "image":
        return data_frame, column, data_frame
    frame, output = prepare_image_projection_input(
        data_frame,
        column=column,
        dataset_path=dataset_path,
        prompt_template=prompt_template,
    )
    return frame, ATLAS_EMBED_INPUT_COLUMN, output

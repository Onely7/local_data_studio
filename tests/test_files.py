"""Tests for files behavior."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi import HTTPException

from local_data_studio.server.files import (
    discover_dataset_files,
    refresh_dataset_file_catalog,
    resolve_data_file,
    resolve_raw_image_file,
)


class DatasetDiscoveryTests(TestCase):
    """Test dataset discovery behavior."""

    def test_discover_dataset_files_prunes_excluded_directories(self) -> None:
        """Verify that discover dataset files prunes excluded directories."""
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            keep_file = data_root / "keep.csv"
            keep_file.write_text("id,name\n1,Ada\n", encoding="utf-8")
            unsupported_file = data_root / "notes.txt"
            unsupported_file.write_text("not a dataset", encoding="utf-8")

            excluded_dir = data_root / "skip"
            excluded_dir.mkdir()
            excluded_file = excluded_dir / "hidden.jsonl"
            excluded_file.write_text('{"id": 1}\n', encoding="utf-8")

            files = discover_dataset_files(data_root, None, [excluded_dir])

            self.assertEqual([keep_file], files)

    def test_discover_dataset_files_excludes_configured_files(self) -> None:
        """Verify that individual dataset files are skipped without pruning siblings."""
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            keep_file = data_root / "keep.csv"
            excluded_root_file = data_root / "omit.jsonl"
            nested_dir = data_root / "nested"
            excluded_nested_file = nested_dir / "hidden.parquet"
            nested_keep_file = nested_dir / "keep.tsv"
            nested_dir.mkdir()
            for path in (keep_file, excluded_root_file, excluded_nested_file, nested_keep_file):
                path.write_text("value\n1\n", encoding="utf-8")

            files = discover_dataset_files(
                data_root,
                None,
                [],
                [data_root / "omit.jsonl", data_root / "nested" / "hidden.parquet"],
            )

            self.assertEqual([keep_file, nested_keep_file], files)

    def test_resolve_data_file_uses_discovered_name_allowlist(self) -> None:
        """Verify that resolve data file uses discovered name allowlist."""
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            dataset = data_root / "example.jsonl"
            dataset.write_text('{"value": 1}\n', encoding="utf-8")

            with (
                patch("local_data_studio.server.files.DATA_ROOT", data_root),
                patch("local_data_studio.server.files.SINGLE_FILE", None),
                patch("local_data_studio.server.files.VIS_EXCLUDE_PATHS", []),
                patch("local_data_studio.server.files.VIS_EXCLUDE_FILE_PATHS", [dataset]),
            ):
                refresh_dataset_file_catalog()
                with self.assertRaises(HTTPException):
                    resolve_data_file("example.jsonl")
                with self.assertRaises(HTTPException):
                    resolve_data_file("../outside.jsonl")


class RawImageFileResolutionTests(TestCase):
    """Test raw image file resolution behavior."""

    def test_resolve_raw_image_file_accepts_file_url_inside_allowed_root(self) -> None:
        """Verify that resolve raw image file accepts file url inside allowed root."""
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            image_path = data_root / "image.png"
            image_path.write_bytes(b"image")

            resolved = resolve_raw_image_file(f"file://{image_path}", [data_root])

            self.assertEqual(image_path, resolved)

    def test_resolve_raw_image_file_rejects_non_image_file(self) -> None:
        """Verify that resolve raw image file rejects non image file."""
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            text_path = data_root / "notes.txt"
            text_path.write_text("not an image", encoding="utf-8")

            with self.assertRaises(HTTPException) as raised:
                resolve_raw_image_file(str(text_path), [data_root])

            self.assertEqual(400, raised.exception.status_code)

    def test_resolve_raw_image_file_rejects_image_outside_allowed_roots(self) -> None:
        """Verify that resolve raw image file rejects image outside allowed roots."""
        with TemporaryDirectory() as allowed_tmpdir, TemporaryDirectory() as outside_tmpdir:
            allowed_root = Path(allowed_tmpdir).resolve()
            outside_root = Path(outside_tmpdir).resolve()
            outside_image = outside_root / "outside.jpg"
            outside_image.write_bytes(b"image")

            with self.assertRaises(HTTPException) as raised:
                resolve_raw_image_file(str(outside_image), [allowed_root])

            self.assertEqual(403, raised.exception.status_code)

    def test_resolve_raw_image_file_rejects_similarly_prefixed_directory(self) -> None:
        """Verify that resolve raw image file rejects similarly prefixed directory."""
        with TemporaryDirectory() as tmpdir:
            base = Path(tmpdir).resolve()
            allowed_root = base / "images"
            similarly_prefixed_root = base / "images-private"
            allowed_root.mkdir()
            similarly_prefixed_root.mkdir()
            outside_image = similarly_prefixed_root / "outside.jpg"
            outside_image.write_bytes(b"image")

            with self.assertRaises(HTTPException) as raised:
                resolve_raw_image_file(str(outside_image), [allowed_root])

            self.assertEqual(403, raised.exception.status_code)

    def test_resolve_raw_image_file_rejects_invalid_path(self) -> None:
        """Verify that resolve raw image file rejects invalid path."""
        with TemporaryDirectory() as tmpdir:
            with self.assertRaises(HTTPException) as raised:
                resolve_raw_image_file("\x00invalid.jpg", [Path(tmpdir)])

            self.assertEqual(403, raised.exception.status_code)

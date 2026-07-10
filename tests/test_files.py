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
    def test_discover_dataset_files_prunes_excluded_directories(self) -> None:
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

    def test_resolve_data_file_uses_discovered_name_allowlist(self) -> None:
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            dataset = data_root / "example.jsonl"
            dataset.write_text('{"value": 1}\n', encoding="utf-8")

            with (
                patch("local_data_studio.server.files.DATA_ROOT", data_root),
                patch("local_data_studio.server.files.SINGLE_FILE", None),
                patch("local_data_studio.server.files.VIS_EXCLUDE_PATHS", []),
            ):
                refresh_dataset_file_catalog()
                self.assertEqual(dataset, resolve_data_file("example.jsonl"))
                with self.assertRaises(HTTPException):
                    resolve_data_file("../outside.jsonl")


class RawImageFileResolutionTests(TestCase):
    def test_resolve_raw_image_file_accepts_file_url_inside_allowed_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            image_path = data_root / "image.png"
            image_path.write_bytes(b"image")

            resolved = resolve_raw_image_file(f"file://{image_path}", [data_root])

            self.assertEqual(image_path, resolved)

    def test_resolve_raw_image_file_rejects_non_image_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir).resolve()
            text_path = data_root / "notes.txt"
            text_path.write_text("not an image", encoding="utf-8")

            with self.assertRaises(HTTPException) as raised:
                resolve_raw_image_file(str(text_path), [data_root])

            self.assertEqual(400, raised.exception.status_code)

    def test_resolve_raw_image_file_rejects_image_outside_allowed_roots(self) -> None:
        with TemporaryDirectory() as allowed_tmpdir, TemporaryDirectory() as outside_tmpdir:
            allowed_root = Path(allowed_tmpdir).resolve()
            outside_root = Path(outside_tmpdir).resolve()
            outside_image = outside_root / "outside.jpg"
            outside_image.write_bytes(b"image")

            with self.assertRaises(HTTPException) as raised:
                resolve_raw_image_file(str(outside_image), [allowed_root])

            self.assertEqual(403, raised.exception.status_code)

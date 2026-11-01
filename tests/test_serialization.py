"""Tests for serialization behavior."""

import base64
from io import BytesIO
from unittest import TestCase

from PIL import Image

from local_data_studio.server.config import MAX_CELL_CHARS, MAX_SEQ_ITEMS
from local_data_studio.server.serialization import THUMBNAIL_MAX_SIDE, serialize_raw_value, serialize_rows, serialize_value


class SerializationTests(TestCase):
    """Test serialization behavior."""

    def test_serialize_value_truncates_long_sequences(self) -> None:
        """Verify that serialize value truncates long sequences."""
        value = list(range(MAX_SEQ_ITEMS + 2))

        serialized = serialize_value(value)

        self.assertEqual(MAX_SEQ_ITEMS + 1, len(serialized))
        self.assertIn("truncated", serialized[-1])

    def test_serialize_value_truncates_large_dicts(self) -> None:
        """Verify that serialize value truncates large dicts."""
        value = {f"key_{index}": index for index in range(MAX_SEQ_ITEMS + 2)}

        serialized = serialize_value(value)

        self.assertIn("__truncated__", serialized)

    def test_serialize_value_only_iterates_the_bounded_dict_prefix(self) -> None:
        """Avoid materializing every item before applying the field limit."""

        class TrackingDict(dict):
            """Count items requested from a deliberately large mapping."""

            def __init__(self) -> None:
                """Create a mapping and reset its iteration counter."""
                super().__init__((str(index), index) for index in range(MAX_SEQ_ITEMS + 100))
                self.yielded = 0

            def items(self):  # noqa: ANN201
                """Yield items while recording bounded consumption."""
                for item in super().items():
                    self.yielded += 1
                    yield item

        value = TrackingDict()

        serialized = serialize_value(value)

        self.assertEqual(MAX_SEQ_ITEMS, value.yielded)
        self.assertIn("__truncated__", serialized)

    def test_serialize_value_truncates_large_bytes(self) -> None:
        """Verify that serialize value truncates large bytes."""
        value = b"x" * (MAX_CELL_CHARS + 1)

        serialized = serialize_value(value)

        self.assertLessEqual(len(serialized), MAX_CELL_CHARS + len("... (truncated)"))
        self.assertIn("truncated", serialized)

    def test_serialize_value_keeps_undecodable_image_bytes_losslessly(self) -> None:
        """Keep signed but undecodable image bytes complete as a data URL."""
        image_bytes = b"\x89PNG\r\n\x1a\n" + (b"x" * MAX_CELL_CHARS)

        serialized = serialize_value({"bytes": image_bytes})

        self.assertEqual("data:image/png;base64," + base64.b64encode(image_bytes).decode(), serialized["bytes"])

    def test_serialize_value_shrinks_large_images_to_marked_thumbnails(self) -> None:
        """Send preview thumbnails instead of full images, for bytes dicts and raw blobs."""
        photo = _png_bytes((1_200, 800))

        in_dict = serialize_value({"bytes": photo, "path": "photo.png"})["bytes"]
        blob = serialize_value(photo)

        for serialized in (in_dict, blob):
            header, _, payload = serialized.partition(",")
            with Image.open(BytesIO(base64.b64decode(payload))) as thumbnail:
                self.assertEqual("data:image/webp;thumbnail=1;base64", header)
                self.assertEqual(THUMBNAIL_MAX_SIDE, max(thumbnail.size))
                self.assertLess(len(payload), len(photo))

    def test_serialize_value_keeps_small_images_unchanged(self) -> None:
        """Pass browser-native images that already fit the thumbnail box through unchanged."""
        icon = _png_bytes((32, 32))

        serialized = serialize_value({"bytes": base64.b64encode(icon).decode()})

        self.assertEqual("data:image/png;base64," + base64.b64encode(icon).decode(), serialized["bytes"])

    def test_serialize_rows_preserves_row_order(self) -> None:
        """Serialize rows concurrently without reordering them."""
        rows = [[index, f"row-{index}"] for index in range(50)]

        self.assertEqual(rows, serialize_rows(rows))

    def test_serialize_value_keeps_image_hex_string_in_bytes_dict(self) -> None:
        """Verify that serialize value keeps image hex string in bytes dict."""
        image_hex = "89504e47" + ("a" * MAX_CELL_CHARS)

        serialized = serialize_value({"bytes": image_hex})

        self.assertEqual(image_hex, serialized["bytes"])

    def test_serialize_value_keeps_long_image_references(self) -> None:
        """Verify that serialize value keeps long image references."""
        value = f"https://example.com/{'a' * (MAX_CELL_CHARS + 1)}.jpg"

        serialized = serialize_value(value)

        self.assertEqual(value, serialized)

    def test_serialize_value_still_truncates_long_non_image_strings(self) -> None:
        """Verify that serialize value still truncates long non image strings."""
        value = "x" * (MAX_CELL_CHARS + 1)

        serialized = serialize_value(value)

        self.assertIn("truncated", serialized)

    def test_serialize_raw_value_never_uses_preview_limits(self) -> None:
        """Verify that serialize raw value never uses preview limits."""
        long_text = "x" * (MAX_CELL_CHARS + 1)
        value = {
            "text": long_text,
            "items": list(range(MAX_SEQ_ITEMS + 2)),
            "payload": {f"key_{index}": index for index in range(MAX_SEQ_ITEMS + 2)},
            "bytes": b"x" * (MAX_CELL_CHARS + 1),
        }

        serialized = serialize_raw_value(value)

        self.assertEqual(long_text, serialized["text"])
        self.assertEqual(value["items"], serialized["items"])
        self.assertEqual(value["payload"], serialized["payload"])
        self.assertEqual(value["bytes"].hex(), serialized["bytes"])

    def test_serialize_raw_value_returns_full_resolution_image_urls(self) -> None:
        """Return complete image bytes as a data URL for enlarged views."""
        photo = _png_bytes((1_200, 800))

        serialized = serialize_raw_value({"bytes": photo})

        self.assertEqual("data:image/png;base64," + base64.b64encode(photo).decode(), serialized["bytes"])


def _png_bytes(size: tuple[int, int]) -> bytes:
    output = BytesIO()
    Image.new("RGB", size, (200, 30, 90)).save(output, format="PNG")
    return output.getvalue()

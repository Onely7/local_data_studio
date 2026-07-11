"""Tests for serialization behavior."""

from unittest import TestCase

from local_data_studio.server.config import MAX_CELL_CHARS, MAX_SEQ_ITEMS
from local_data_studio.server.serialization import serialize_raw_value, serialize_value


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

    def test_serialize_value_truncates_large_bytes(self) -> None:
        """Verify that serialize value truncates large bytes."""
        value = b"x" * (MAX_CELL_CHARS + 1)

        serialized = serialize_value(value)

        self.assertLessEqual(len(serialized), MAX_CELL_CHARS + len("... (truncated)"))
        self.assertIn("truncated", serialized)

    def test_serialize_value_keeps_image_bytes_in_bytes_dict(self) -> None:
        """Verify that serialize value keeps image bytes in bytes dict."""
        image_bytes = b"\x89PNG\r\n\x1a\n" + (b"x" * MAX_CELL_CHARS)

        serialized = serialize_value({"bytes": image_bytes})

        self.assertEqual(image_bytes.hex(), serialized["bytes"])

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

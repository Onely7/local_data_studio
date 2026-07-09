from unittest import TestCase

from local_data_studio.server.config import MAX_CELL_CHARS, MAX_SEQ_ITEMS
from local_data_studio.server.serialization import serialize_value


class SerializationTests(TestCase):
    def test_serialize_value_truncates_long_sequences(self) -> None:
        value = list(range(MAX_SEQ_ITEMS + 2))

        serialized = serialize_value(value)

        self.assertEqual(MAX_SEQ_ITEMS + 1, len(serialized))
        self.assertIn("truncated", serialized[-1])

    def test_serialize_value_truncates_large_dicts(self) -> None:
        value = {f"key_{index}": index for index in range(MAX_SEQ_ITEMS + 2)}

        serialized = serialize_value(value)

        self.assertIn("__truncated__", serialized)

    def test_serialize_value_truncates_large_bytes(self) -> None:
        value = b"x" * (MAX_CELL_CHARS + 1)

        serialized = serialize_value(value)

        self.assertLessEqual(len(serialized), MAX_CELL_CHARS + len("... (truncated)"))
        self.assertIn("truncated", serialized)

    def test_serialize_value_keeps_image_bytes_in_bytes_dict(self) -> None:
        image_bytes = b"\x89PNG\r\n\x1a\n" + (b"x" * MAX_CELL_CHARS)

        serialized = serialize_value({"bytes": image_bytes})

        self.assertEqual(image_bytes.hex(), serialized["bytes"])

    def test_serialize_value_keeps_image_hex_string_in_bytes_dict(self) -> None:
        image_hex = "89504e47" + ("a" * MAX_CELL_CHARS)

        serialized = serialize_value({"bytes": image_hex})

        self.assertEqual(image_hex, serialized["bytes"])

    def test_serialize_value_keeps_long_image_references(self) -> None:
        value = f"https://example.com/{'a' * (MAX_CELL_CHARS + 1)}.jpg"

        serialized = serialize_value(value)

        self.assertEqual(value, serialized)

    def test_serialize_value_still_truncates_long_non_image_strings(self) -> None:
        value = "x" * (MAX_CELL_CHARS + 1)

        serialized = serialize_value(value)

        self.assertIn("truncated", serialized)

"""Tests for provider-neutral SQL generation contracts."""

from unittest import TestCase

from fastapi import HTTPException

from local_data_studio.server.llm import _clean_sql
from local_data_studio.server.llm_prompt import MAX_SAMPLE_CONTEXT_CHARS, build_sql_generation_messages


class SqlCleaningTests(TestCase):
    """Keep generated SQL constrained to one read-only statement."""

    def test_removes_trailing_semicolon(self) -> None:
        """Return a generated SELECT without its optional trailing delimiter."""
        self.assertEqual("SELECT * FROM data", _clean_sql("SELECT * FROM data;"))

    def test_removes_markdown_fences(self) -> None:
        """Accept a fenced SELECT while returning plain SQL."""
        self.assertEqual("SELECT * FROM data", _clean_sql("```sql\nSELECT * FROM data;\n```"))

    def test_accepts_common_table_expressions(self) -> None:
        """Allow a WITH query used to compose a read-only result."""
        sql = "WITH selected AS (SELECT * FROM data) SELECT * FROM selected"
        self.assertEqual(sql, _clean_sql(sql))

    def test_rejects_multiple_statements(self) -> None:
        """Reject generated output containing more than one statement."""
        with self.assertRaisesRegex(HTTPException, "multi-statement"):
            _clean_sql("SELECT * FROM data; DELETE FROM data")

    def test_rejects_non_select_statements(self) -> None:
        """Reject generated output that does not begin with SELECT or WITH."""
        with self.assertRaisesRegex(HTTPException, "only SELECT"):
            _clean_sql("DELETE FROM data")


class SqlPromptTests(TestCase):
    """Keep provider-neutral prompts text-only and bounded."""

    def test_builds_one_user_message_with_schema_and_sample(self) -> None:
        """Avoid provider-specific system-message behavior."""
        messages = build_sql_generation_messages(
            "ratingを降順にしてください",
            [{"name": "rating", "type": "INTEGER"}],
            {"rating": 5},
        )

        self.assertEqual(1, len(messages))
        self.assertEqual("user", messages[0]["role"])
        self.assertIn('"name":"rating"', messages[0]["content"])
        self.assertIn('"rating":5', messages[0]["content"])
        self.assertIn("ratingを降順にしてください", messages[0]["content"])

    def test_bounds_sample_context(self) -> None:
        """Prevent caller-supplied sample data from growing prompts without limit."""
        messages = build_sql_generation_messages("select", [], {"text": "x" * (MAX_SAMPLE_CONTEXT_CHARS * 2)})
        self.assertIn("... (truncated)", messages[0]["content"])

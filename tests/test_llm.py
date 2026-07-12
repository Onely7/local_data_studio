"""Tests for provider-neutral SQL generation contracts."""

from unittest import TestCase

from fastapi import HTTPException

from local_data_studio.server.llm import _clean_sql


class SqlCleaningTests(TestCase):
    """Keep generated SQL constrained to one read-only statement."""

    def test_removes_trailing_semicolon(self) -> None:
        """Return a generated SELECT without its optional trailing delimiter."""
        self.assertEqual("SELECT * FROM data", _clean_sql("SELECT * FROM data;"))

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

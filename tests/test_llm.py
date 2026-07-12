"""Tests for provider-neutral SQL generation contracts."""

import os
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from fastapi import HTTPException

from local_data_studio.server.llm import _clean_sql
from local_data_studio.server.llm_client import complete_text, extract_completion_text
from local_data_studio.server.llm_profiles import LlmModelProfile, LlmProfileRegistry
from local_data_studio.server.llm_prompt import MAX_SAMPLE_CONTEXT_CHARS, build_sql_generation_messages
from local_data_studio.server.llm_service import generate_sql_request


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


class LiteLlmClientTests(TestCase):
    """Verify the common LiteLLM call surface across providers."""

    def test_passes_only_common_arguments_and_copied_provider_options(self) -> None:
        """Keep provider-specific settings isolated to the selected profile."""
        providers = (
            ("openai/gpt-5.2", None, {"max_completion_tokens": 400}),
            ("anthropic/claude-sonnet", None, {"max_tokens": 400}),
            ("gemini/gemini-2.5-flash", None, {}),
            ("hosted_vllm/Qwen/Qwen3-8B", "http://127.0.0.1:8000/v1", {"temperature": 0, "extra_body": {"top_k": 20}}),
        )
        for model, base_url, options in providers:
            with self.subTest(model=model):
                captured: dict[str, object] = {}

                def completion(_captured=captured, **kwargs):  # noqa: ANN001, ANN003, B008
                    _captured.update(kwargs)
                    return {"choices": [{"message": {"content": "SELECT * FROM data"}}]}

                fake_litellm = SimpleNamespace(completion=completion, Timeout=type("Timeout", (Exception,), {}))
                profile = LlmModelProfile(
                    id="selected",
                    label="Selected",
                    model=model,
                    base_url=base_url,
                    provider_options=options,
                )
                with patch("local_data_studio.server.llm_client._load_litellm", return_value=fake_litellm):
                    text = complete_text(profile, [{"role": "user", "content": "SQL"}], default_timeout_seconds=60)

                self.assertEqual("SELECT * FROM data", text)
                self.assertEqual(model, captured["model"])
                self.assertEqual(60, captured["timeout"])
                self.assertNotIn("api_key", captured)
                if base_url:
                    self.assertEqual(base_url, captured["base_url"])
                self.assertEqual(options, profile.provider_options)

    def test_resolves_api_key_without_exposing_its_environment_name(self) -> None:
        """Read a configured credential only when building the provider call."""
        name = "LOCAL_DATA_STUDIO_LLM_TEST_KEY"
        original = os.environ.get(name)
        os.environ[name] = "secret-value"
        captured: dict[str, object] = {}

        def completion(**kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return {"choices": [{"message": {"content": "SELECT 1"}}]}

        try:
            profile = LlmModelProfile(id="keyed", label="Keyed", model="openai/model", api_key_env=name)
            fake_litellm = SimpleNamespace(completion=completion, Timeout=type("Timeout", (Exception,), {}))
            with patch("local_data_studio.server.llm_client._load_litellm", return_value=fake_litellm):
                complete_text(profile, [{"role": "user", "content": "SQL"}], default_timeout_seconds=60)
            self.assertEqual("secret-value", captured["api_key"])
        finally:
            if original is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = original

    def test_extracts_dictionary_object_and_text_block_responses(self) -> None:
        """Accept LiteLLM's normalized dictionary and object response shapes."""
        dictionary = {"choices": [{"message": {"content": "SELECT 1"}}]}
        object_response = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=[{"type": "text", "text": "SELECT 2"}]))])
        self.assertEqual("SELECT 1", extract_completion_text(dictionary))
        self.assertEqual("SELECT 2", extract_completion_text(object_response))

    def test_maps_timeout_and_provider_failures_without_leaking_details(self) -> None:
        """Return stable gateway errors without provider exception text."""
        timeout_type = type("Timeout", (Exception,), {})
        profile = LlmModelProfile(id="model", label="Model", model="openai/model")
        for error, expected_status in ((timeout_type("contains-secret"), 504), (RuntimeError("contains-secret"), 502)):

            def fail(_error=error, **kwargs):  # noqa: ANN001, ANN003, ARG001, B008
                raise _error

            fake_litellm = SimpleNamespace(completion=fail, Timeout=timeout_type)
            with (
                self.subTest(error=type(error).__name__),
                patch("local_data_studio.server.llm_client._load_litellm", return_value=fake_litellm),
                self.assertRaises(HTTPException) as raised,
            ):
                complete_text(profile, [{"role": "user", "content": "SQL"}], default_timeout_seconds=60)
            self.assertEqual(expected_status, raised.exception.status_code)
            self.assertNotIn("contains-secret", str(raised.exception.detail))

    def test_rejects_empty_provider_responses(self) -> None:
        """Fail when the normalized response contains no assistant text."""
        profile = LlmModelProfile(id="model", label="Model", model="openai/model")
        fake_litellm = SimpleNamespace(completion=lambda **kwargs: {"choices": []}, Timeout=type("Timeout", (Exception,), {}))  # noqa: ARG005
        with (
            patch("local_data_studio.server.llm_client._load_litellm", return_value=fake_litellm),
            self.assertRaises(HTTPException) as raised,
        ):
            complete_text(profile, [{"role": "user", "content": "SQL"}], default_timeout_seconds=60)
        self.assertEqual(502, raised.exception.status_code)


class SqlGenerationServiceTests(TestCase):
    """Test profile resolution through validated SQL output."""

    def test_returns_sql_and_selected_profile_metadata(self) -> None:
        """Preserve the model identity needed by API and UI responses."""
        registry = LlmProfileRegistry.model_validate({"models": [{"id": "gemini", "label": "Gemini", "model": "gemini/gemini-2.5-flash"}]})
        with patch("local_data_studio.server.llm_service.complete_text", return_value="```sql\nSELECT * FROM data;\n```"):
            result = generate_sql_request("all rows", [{"name": "id", "type": "INTEGER"}], None, registry=registry)
        self.assertEqual("SELECT * FROM data", result.sql)
        self.assertEqual("gemini", result.profile_id)
        self.assertEqual("Gemini", result.profile_label)

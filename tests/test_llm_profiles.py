"""Tests for server-managed LiteLLM model profiles."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from local_data_studio.server.llm_profiles import LlmProfileRegistry, load_llm_profile_registry


def _write_config(root: Path, llm_config: str) -> Path:
    path = root / "local_data_studio.toml"
    path.write_text(llm_config, encoding="utf-8")
    return path


class LlmProfileTests(TestCase):
    """Validate model selection without exposing credential values."""

    def test_loads_supported_provider_profiles(self) -> None:
        """Load OpenAI, Anthropic, Gemini, and hosted vLLM profiles."""
        with TemporaryDirectory() as tmp:
            path = _write_config(
                Path(tmp),
                """
[llm]
default_model = "openai-main"
timeout_seconds = 45

[[llm.models]]
id = "openai-main"
label = "GPT"
model = "openai/gpt-5.2"
api_key_env = "TEST_OPENAI_API_KEY"
provider_options = { max_completion_tokens = 400 }

[[llm.models]]
id = "claude-main"
label = "Claude"
model = "anthropic/claude-sonnet"

[[llm.models]]
id = "gemini-main"
label = "Gemini"
model = "gemini/gemini-2.5-flash"

[[llm.models]]
id = "local-qwen"
label = "Local Qwen"
model = "hosted_vllm/Qwen/Qwen3-8B"
base_url = "http://127.0.0.1:8000/v1/"
provider_options = { temperature = 0, extra_body = { top_k = 20 } }
""",
            )

            registry = load_llm_profile_registry(str(path))

        self.assertEqual("openai-main", registry.effective_default_model)
        self.assertEqual(45, registry.timeout_seconds)
        self.assertEqual(["openai", "anthropic", "gemini", "hosted_vllm"], [profile.provider for profile in registry.models])
        self.assertEqual("http://127.0.0.1:8000/v1", registry.models[-1].base_url)

    def test_uses_first_profile_as_implicit_default(self) -> None:
        """Select the first profile when no default is configured."""
        registry = LlmProfileRegistry.model_validate({"models": [{"id": "first", "label": "First", "model": "openai/example"}]})
        self.assertEqual("first", registry.effective_default_model)

    def test_rejects_invalid_profile_contracts(self) -> None:
        """Reject ambiguous providers, duplicate IDs, and response-shaping options."""
        invalid_configs = (
            {"models": [{"id": "bad", "label": "Bad", "model": "model-without-provider"}]},
            {"models": [{"id": "bad", "label": "Bad", "model": "vllm/model"}]},
            {
                "models": [
                    {"id": "same", "label": "One", "model": "openai/one"},
                    {"id": "same", "label": "Two", "model": "openai/two"},
                ]
            },
            {"default_model": "missing", "models": [{"id": "one", "label": "One", "model": "openai/one"}]},
            {
                "models": [
                    {
                        "id": "bad",
                        "label": "Bad",
                        "model": "hosted_vllm/model",
                        "provider_options": {"extra_body": {"structured_outputs": {"choice": ["a"]}}},
                    }
                ]
            },
        )
        for config in invalid_configs:
            with self.subTest(config=config), self.assertRaises(ValidationError):
                LlmProfileRegistry.model_validate(config)

    def test_public_models_never_include_secrets_or_connection_options(self) -> None:
        """Expose only selector metadata and credential availability."""
        name = "LOCAL_DATA_STUDIO_TEST_LLM_KEY"
        original = os.environ.pop(name, None)
        try:
            registry = LlmProfileRegistry.model_validate(
                {
                    "models": [
                        {
                            "id": "private",
                            "label": "Private",
                            "model": "openai/private",
                            "base_url": "https://secret.example/v1",
                            "api_key_env": name,
                            "provider_options": {"temperature": 0},
                        }
                    ]
                }
            )
            public = registry.public_models()[0]
            self.assertFalse(public["available"])
            self.assertNotIn("base_url", public)
            self.assertNotIn("api_key_env", public)
            self.assertNotIn("provider_options", public)
            with self.assertRaisesRegex(HTTPException, "credential environment variable"):
                registry.profile("private")
        finally:
            if original is not None:
                os.environ[name] = original

    def test_unknown_profile_is_a_client_error_and_empty_registry_is_unavailable(self) -> None:
        """Distinguish invalid selection from an unconfigured server."""
        registry = LlmProfileRegistry.model_validate({"models": [{"id": "known", "label": "Known", "model": "openai/known"}]})
        with self.assertRaises(HTTPException) as unknown:
            registry.profile("unknown")
        self.assertEqual(400, unknown.exception.status_code)
        with self.assertRaises(HTTPException) as unavailable:
            LlmProfileRegistry().profile(None)
        self.assertEqual(503, unavailable.exception.status_code)

    def test_resolves_credentials_from_selected_dotenv_file(self) -> None:
        """Keep .env credentials usable without exporting them into the process."""
        name = "LOCAL_DATA_STUDIO_DOTENV_LLM_KEY"
        original = os.environ.pop(name, None)
        try:
            with TemporaryDirectory() as tmp:
                env_file = Path(tmp) / ".env"
                env_file.write_text(f'{name}="dotenv-secret"\n', encoding="utf-8")
                profile = LlmProfileRegistry.model_validate(
                    {"models": [{"id": "model", "label": "Model", "model": "openai/model", "api_key_env": name}]}
                ).models[0]
                with patch("local_data_studio.server.llm_profiles.ENV_FILE", env_file):
                    self.assertEqual("dotenv-secret", profile.api_key())
        finally:
            if original is not None:
                os.environ[name] = original

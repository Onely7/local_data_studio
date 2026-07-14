"""Validated language registry and limits for manual LLM translation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..runtime_config import config_section, read_runtime_config


@dataclass(frozen=True, slots=True)
class TranslationLanguage:
    """One target language exposed to the browser."""

    code: str
    name: str
    native_name: str


TRANSLATION_LANGUAGES = (
    TranslationLanguage("af", "Afrikaans", "Afrikaans"),
    TranslationLanguage("sq", "Albanian", "Shqip"),
    TranslationLanguage("am", "Amharic", "አማርኛ"),
    TranslationLanguage("ar", "Arabic", "العربية"),
    TranslationLanguage("hy", "Armenian", "Հայերեն"),
    TranslationLanguage("az", "Azerbaijani", "Azərbaycan dili"),
    TranslationLanguage("eu", "Basque", "Euskara"),
    TranslationLanguage("be", "Belarusian", "Беларуская"),
    TranslationLanguage("bn", "Bengali", "বাংলা"),
    TranslationLanguage("bs", "Bosnian", "Bosanski"),
    TranslationLanguage("bg", "Bulgarian", "Български"),
    TranslationLanguage("ca", "Catalan", "Català"),
    TranslationLanguage("zh", "Chinese", "中文"),
    TranslationLanguage("hr", "Croatian", "Hrvatski"),
    TranslationLanguage("cs", "Czech", "Čeština"),
    TranslationLanguage("da", "Danish", "Dansk"),
    TranslationLanguage("nl", "Dutch", "Nederlands"),
    TranslationLanguage("en", "English", "English"),
    TranslationLanguage("et", "Estonian", "Eesti"),
    TranslationLanguage("fi", "Finnish", "Suomi"),
    TranslationLanguage("fr", "French", "Français"),
    TranslationLanguage("gl", "Galician", "Galego"),
    TranslationLanguage("ka", "Georgian", "ქართული"),
    TranslationLanguage("de", "German", "Deutsch"),
    TranslationLanguage("el", "Greek", "Ελληνικά"),
    TranslationLanguage("gu", "Gujarati", "ગુજરાતી"),
    TranslationLanguage("ht", "Haitian Creole", "Kreyòl ayisyen"),
    TranslationLanguage("he", "Hebrew", "עברית"),
    TranslationLanguage("hi", "Hindi", "हिन्दी"),
    TranslationLanguage("hu", "Hungarian", "Magyar"),
    TranslationLanguage("is", "Icelandic", "Íslenska"),
    TranslationLanguage("id", "Indonesian", "Bahasa Indonesia"),
    TranslationLanguage("ga", "Irish", "Gaeilge"),
    TranslationLanguage("it", "Italian", "Italiano"),
    TranslationLanguage("ja", "Japanese", "日本語"),
    TranslationLanguage("kn", "Kannada", "ಕನ್ನಡ"),
    TranslationLanguage("kk", "Kazakh", "Қазақ тілі"),
    TranslationLanguage("ko", "Korean", "한국어"),
    TranslationLanguage("lv", "Latvian", "Latviešu"),
    TranslationLanguage("lt", "Lithuanian", "Lietuvių"),
    TranslationLanguage("mk", "Macedonian", "Македонски"),
    TranslationLanguage("ms", "Malay", "Bahasa Melayu"),
    TranslationLanguage("ml", "Malayalam", "മലയാളം"),
    TranslationLanguage("mt", "Maltese", "Malti"),
    TranslationLanguage("mr", "Marathi", "मराठी"),
    TranslationLanguage("mn", "Mongolian", "Монгол"),
    TranslationLanguage("ne", "Nepali", "नेपाली"),
    TranslationLanguage("no", "Norwegian", "Norsk"),
    TranslationLanguage("fa", "Persian", "فارسی"),
    TranslationLanguage("pl", "Polish", "Polski"),
    TranslationLanguage("pt", "Portuguese", "Português"),
    TranslationLanguage("pa", "Punjabi", "ਪੰਜਾਬੀ"),
    TranslationLanguage("ro", "Romanian", "Română"),
    TranslationLanguage("ru", "Russian", "Русский"),
    TranslationLanguage("sr", "Serbian", "Српски"),
    TranslationLanguage("sk", "Slovak", "Slovenčina"),
    TranslationLanguage("sl", "Slovenian", "Slovenščina"),
    TranslationLanguage("es", "Spanish", "Español"),
    TranslationLanguage("sw", "Swahili", "Kiswahili"),
    TranslationLanguage("sv", "Swedish", "Svenska"),
    TranslationLanguage("ta", "Tamil", "தமிழ்"),
    TranslationLanguage("te", "Telugu", "తెలుగు"),
    TranslationLanguage("th", "Thai", "ไทย"),
    TranslationLanguage("tr", "Turkish", "Türkçe"),
    TranslationLanguage("uk", "Ukrainian", "Українська"),
    TranslationLanguage("ur", "Urdu", "اردو"),
    TranslationLanguage("uz", "Uzbek", "Oʻzbekcha"),
    TranslationLanguage("vi", "Vietnamese", "Tiếng Việt"),
)
LANGUAGES_BY_CODE = {language.code: language for language in TRANSLATION_LANGUAGES}


class TranslationSettings(BaseModel):
    """Server-owned limits for manually requested translations."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_batch_rows: int = Field(default=500, ge=1)
    max_total_characters: int = Field(default=50_000, ge=1)
    max_strings: int = Field(default=2_000, ge=1)
    chunk_characters: int = Field(default=12_000, ge=1)
    max_concurrency: int = Field(default=2, ge=1)
    confirm_rows: int = Field(default=25, ge=1)
    confirm_characters: int = Field(default=10_000, ge=1)
    default_target_language: str | None = None

    @field_validator("default_target_language")
    @classmethod
    def validate_default_target_language(cls, value: str | None) -> str | None:
        """Normalize and validate the configured target-language code."""
        if value is None:
            return None
        code = value.strip().lower()
        if code not in LANGUAGES_BY_CODE:
            raise ValueError("default_target_language must be a supported translation language code")
        return code


def load_translation_settings(path: str | None = None) -> TranslationSettings:
    """Load optional translation limits from the runtime TOML file."""
    config, _ = read_runtime_config(path)
    section: Mapping[str, Any] = config_section(config, "translation")
    return TranslationSettings.model_validate(dict(section)) if section else TranslationSettings()


TRANSLATION_SETTINGS = load_translation_settings()


def public_translation_config(settings: TranslationSettings = TRANSLATION_SETTINGS) -> dict[str, Any]:
    """Return browser-safe languages and limits without model credentials."""
    return {
        "languages": [asdict(language) for language in TRANSLATION_LANGUAGES],
        "configured_default_language": settings.default_target_language,
        "default_language": "ja",
        "limits": settings.model_dump(exclude={"default_target_language"}),
    }

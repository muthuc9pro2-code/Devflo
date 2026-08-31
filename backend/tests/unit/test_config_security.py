import pytest
from pydantic import ValidationError

from app.core.config import AppSettings, Settings


def _settings_with_secret(secret_key: str) -> AppSettings:
    values = Settings.model_dump()
    values["SECRET_KEY"] = secret_key
    return AppSettings(_env_file=None, **values)


@pytest.mark.parametrize(
    "secret_key",
    [
        "",
        "replace-me",
        "replace-with-at-least-32-random-bytes",
        "trivially-short-secret",
    ],
)
def test_unsafe_secret_keys_are_rejected(secret_key):
    with pytest.raises(ValidationError, match="at least 32 bytes"):
        _settings_with_secret(secret_key)


def test_sufficiently_long_test_secret_is_accepted():
    secret_key = "deterministic-test-secret-0123456789abcdef"

    settings = _settings_with_secret(secret_key)

    assert settings.SECRET_KEY == secret_key


def test_gemini_api_key_is_optional_and_blank_values_normalize_to_none(monkeypatch):
    values = Settings.model_dump()
    values.pop("GEMINI_API_KEY", None)
    # conftest.py deliberately supplies a dummy Gemini key for the normal
    # suite. Remove it only inside this test so we can prove that the real
    # Settings model also boots when Gemini is genuinely unconfigured.
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    without_key = AppSettings(_env_file=None, **values)
    blank_key = AppSettings(_env_file=None, **values, GEMINI_API_KEY="   ")

    assert without_key.GEMINI_API_KEY is None
    assert blank_key.GEMINI_API_KEY is None

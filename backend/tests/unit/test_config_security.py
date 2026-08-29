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

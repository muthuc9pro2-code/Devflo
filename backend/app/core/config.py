from ipaddress import ip_address
from urllib.parse import urlsplit

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_UNSAFE_SECRET_KEYS = {
    "replace-me",
    "replace-with-at-least-32-random-bytes",
}


class AppSettings(BaseSettings):
    APP_NAME: str 
    APP_VERSION: str 
    DATABASE_URL: str

    REDIS_BROKER_URL: str
    REDIS_RESULT_BACKEND_URL: str
    REDIS_EVENTS_URL: str

    COOKIE_SECURE: bool = False

    RESEND_API_KEY: str
    EMAIL_FROM: str

    SECRET_KEY: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int
    REFRESH_TOKEN_EXPIRE_DAYS: int

    PASSWORD_RESET_TOKEN_EXPIRE_MINUTES: int

    FRONTEND_URL: str

    GEMINI_API_KEY: str | None = None
    GEMINI_MODEL: str = "gemini-3.5-flash-lite"

    @field_validator("SECRET_KEY")
    @classmethod
    def validate_secret_key(cls, value: str) -> str:
        candidate = value.strip()
        if (
            candidate.lower() in _UNSAFE_SECRET_KEYS
            or len(candidate.encode("utf-8")) < 32
        ):
            raise ValueError(
                "SECRET_KEY must contain at least 32 bytes and must not be a placeholder"
            )
        return value

    @field_validator("GEMINI_API_KEY", mode="before")
    @classmethod
    def normalize_optional_gemini_api_key(cls, value):
        if value is None:
            return None
        candidate = str(value).strip()
        return candidate or None

    @model_validator(mode="after")
    def validate_cookie_security(self):
        frontend = urlsplit(self.FRONTEND_URL)
        hostname = frontend.hostname
        is_loopback = hostname == "localhost"
        if hostname and not is_loopback:
            try:
                is_loopback = ip_address(hostname).is_loopback
            except ValueError:
                pass
        # Local development is intentionally allowed to run over plain HTTP.
        if is_loopback:
            if frontend.scheme not in {"http", "https"}:
                raise ValueError("FRONTEND_URL must use http or https")
            return self
        # Anything non-local is treated as production-like configuration.
        # Failing closed here prevents accidentally issuing non-Secure auth
        # cookies on a real deployed host.
        if frontend.scheme != "https" or not hostname:
            raise ValueError("Non-local FRONTEND_URL must use https")
        if not self.COOKIE_SECURE:
            raise ValueError(
                "COOKIE_SECURE must be true when FRONTEND_URL is non-local"
            )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True
    )

Settings = AppSettings()

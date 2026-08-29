from pydantic import field_validator
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

    GEMINI_API_KEY: str
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

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True
    )

Settings = AppSettings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
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

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True
    )

Settings = Settings()

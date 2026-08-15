from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_NAME: str 
    APP_VERSION: str 
    DATABASE_URL: str

    AWS_REGION: str
    SES_FROM_EMAIL: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str

    FRONTEND_URL: str

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True
    )

Settings = Settings()



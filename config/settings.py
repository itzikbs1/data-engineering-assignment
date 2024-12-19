from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    API_KEY: str
    API_BASE_URL: str = "https://api.openaq.org/v3"

    # Database settings
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int

    # ETL settings
    MAX_LOCATIONS: int = 100

    class Config:
        env_file = ".env"


settings = Settings()

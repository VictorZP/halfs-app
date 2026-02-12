"""Application configuration loaded from environment variables."""

import os
from pathlib import Path
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Settings loaded from .env file or environment variables."""

    # Database
    database_url: str = "sqlite:///./data/app.db"

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True

    # CORS
    cors_origins: str = "http://localhost:3000,http://localhost:5173"

    # Telegram (optional)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # BetsAPI
    betsapi_url: str = "https://betsapi.com/cio/basketball"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def data_dir(self) -> Path:
        """Directory where SQLite databases are stored."""
        d = Path("data")
        d.mkdir(parents=True, exist_ok=True)
        return d

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()

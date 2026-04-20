"""
Application configuration via environment variables.

Uses pydantic-settings to load config from .env files or environment variables.
Supports pluggable AI backends: ollama (default), openai, or none.
"""

import os
from enum import Enum
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class AIBackend(str, Enum):
    """Supported AI inference backends."""
    OLLAMA = "ollama"
    OPENAI = "openai"
    NONE = "none"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # --- Application ---
    app_name: str = "Data Reliability Autopilot"
    app_version: str = "1.0.0"
    debug: bool = False

    # --- Database ---
    db_path: str = Field(
        default=":memory:",
        description="DuckDB database path. Use ':memory:' for in-memory.",
    )

    # --- Data ---
    data_dir: str = Field(
        default_factory=lambda: os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
        )
    )

    # --- AI Backend ---
    ai_backend: AIBackend = AIBackend.OLLAMA

    # Ollama settings
    ollama_url: str = "http://localhost:11434/api/generate"
    ollama_model: str = "llama3"
    ollama_timeout: int = 120

    # OpenAI settings
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    openai_timeout: int = 60

    # --- Sandbox ---
    sandbox_timeout: int = 30
    sandbox_max_rows: int = 10000

    # --- Server ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: list[str] = ["*"]

    model_config = {
        "env_prefix": "AUTOPILOT_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


@lru_cache()
def get_settings() -> Settings:
    """Return cached application settings singleton."""
    return Settings()

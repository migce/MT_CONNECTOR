"""
Monitor API configuration — loaded from environment variables.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Database ─────────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://monitor:monitor@monitor-db:5432/monitor"

    # ── MT5 Connector API ────────────────────────────────────────────
    mt5_api_url: str = "http://host.docker.internal:9000"

    # ── JWT ──────────────────────────────────────────────────────────
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7

    # ── Server ───────────────────────────────────────────────────────
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False

    model_config = {"env_prefix": "MONITOR_", "env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()

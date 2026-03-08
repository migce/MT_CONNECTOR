"""
REST endpoint: ``/api/v1/health``

Service health check — reports MT5 / DB / Redis connectivity,
uptime, and active symbol count.
"""

from __future__ import annotations

import time

import redis.asyncio as aioredis
from fastapi import APIRouter
from sqlalchemy import text

from src.api.schemas import HealthResponse
from src.config import get_settings
from src.db.engine import get_engine

router = APIRouter(prefix="/api/v1", tags=["health"])

# Set once when the module is first imported (≈ app startup).
_start_time: float = time.time()

# Reusable Redis client for health checks (lazy singleton)
_redis_client: aioredis.Redis | None = None


def _get_redis_client() -> aioredis.Redis:
    """Return a cached async Redis client instance."""
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password,
            db=settings.redis_db,
            socket_connect_timeout=2,
            retry_on_error=[ConnectionError, TimeoutError],
            socket_keepalive=True,
        )
    return _redis_client


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health check",
)
async def health_check() -> HealthResponse:
    settings = get_settings()

    # DB check
    db_ok = False
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    # Redis check (reuses persistent connection)
    redis_ok = False
    try:
        r = _get_redis_client()
        await r.ping()
        redis_ok = True
    except Exception:
        pass

    return HealthResponse(
        status="ok" if db_ok else "degraded",
        mt5_connected=False,  # MT5 runs in the poller, not in the API process
        db_connected=db_ok,
        redis_connected=redis_ok,
        uptime_sec=round(time.time() - _start_time, 1),
        symbols_active=len(settings.symbols),
    )

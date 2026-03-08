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
from src.redis_bus.pool import get_redis_pool

router = APIRouter(prefix="/api/v1", tags=["health"])

# Set once when the module is first imported (≈ app startup).
_start_time: float = time.time()


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

    # Redis check (reuses shared pool)
    redis_ok = False
    try:
        r = get_redis_pool()
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

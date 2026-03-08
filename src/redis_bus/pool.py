"""
Shared Redis connection pool singleton.

All components that need Redis should call ``get_redis_pool()`` instead
of creating their own ``aioredis.Redis`` instances.  This keeps the total
number of connections bounded and makes cleanup straightforward.
"""

from __future__ import annotations

import redis.asyncio as aioredis
import structlog

from src.config import Settings, get_settings

logger = structlog.get_logger(__name__)

_pool: aioredis.Redis | None = None


def get_redis_pool(settings: Settings | None = None) -> aioredis.Redis:
    """Return (and cache) a shared async Redis client backed by a connection pool."""
    global _pool
    if _pool is not None:
        return _pool

    settings = settings or get_settings()
    _pool = aioredis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
        db=settings.redis_db,
        decode_responses=False,
        retry_on_error=[ConnectionError, TimeoutError],
        socket_connect_timeout=5,
        socket_keepalive=True,
        max_connections=20,
    )
    logger.info(
        "redis_pool_created",
        host=settings.redis_host,
        port=settings.redis_port,
    )
    return _pool


async def close_redis_pool() -> None:
    """Gracefully close the shared Redis pool."""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
        logger.info("redis_pool_closed")

"""
Shared Redis connection pool singleton.

All components that need Redis should call ``get_redis_pool()`` instead
of creating their own ``aioredis.Redis`` instances.  This keeps the total
number of connections bounded and makes cleanup straightforward.
"""

from __future__ import annotations

from urllib.parse import urlsplit

import redis.asyncio as aioredis
import structlog

from src.config import Settings, get_settings

logger = structlog.get_logger(__name__)

_pool: aioredis.Redis | None = None
_control_pool: aioredis.Redis | None = None


def get_cache_redis_pool(settings: Settings | None = None) -> aioredis.Redis:
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
        socket_timeout=10,
        socket_keepalive=True,
        max_connections=50,
    )
    logger.info(
        "redis_pool_created",
        host=settings.redis_host,
        port=settings.redis_port,
    )
    return _pool


def new_control_redis(settings: Settings, **kwargs) -> aioredis.Redis:
    """Dedicated control connection; no failure-driven cache fallback."""
    options = dict(decode_responses=False, socket_connect_timeout=3,
                   socket_timeout=10, socket_keepalive=True, max_connections=30)
    options.update(kwargs)
    if settings.control_redis_url:
        endpoint = urlsplit(settings.control_redis_url)
        if endpoint.query:
            raise ValueError("Control Redis URL query overrides are not supported")
        if endpoint.scheme != "rediss" and endpoint.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("Remote control Redis requires verified TLS")
        if endpoint.scheme == "rediss":
            options.update(ssl_cert_reqs="required", ssl_check_hostname=True)
        if settings.control_redis_ca_file:
            options.update(ssl_ca_certs=settings.control_redis_ca_file,
                           ssl_cert_reqs="required", ssl_check_hostname=True)
        return aioredis.Redis.from_url(settings.control_redis_url, **options)
    return aioredis.Redis(host=settings.redis_host, port=settings.redis_port,
                         password=settings.redis_password, db=settings.redis_db, **options)


def get_redis_pool(settings: Settings | None = None) -> aioredis.Redis:
    """Control/status/queue store; market PubSub uses get_cache_redis_pool."""
    global _control_pool
    settings = settings or get_settings()
    if not settings.control_redis_url:
        return get_cache_redis_pool(settings)
    if _control_pool is None:
        _control_pool = new_control_redis(settings)
    return _control_pool


async def close_redis_pool() -> None:
    """Gracefully close the shared Redis pool."""
    global _pool, _control_pool
    if _control_pool is not None:
        await _control_pool.aclose()
        _control_pool = None
    if _pool is not None:
        await _pool.aclose()
        _pool = None
        logger.info("redis_pool_closed")

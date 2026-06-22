"""
Active symbol tracking.

Maintains the set of symbols that should be actively polled in real-time.
A symbol becomes "active" when the API receives a request for it.
Symbols remain active for ``ACTIVE_TTL_SEC`` (default 7 days) after
the last API request.

**Redis keys**::

    symbol:active:{SYMBOL}  →  ISO timestamp of last access  (TTL = 7 days)

The poller reads these keys periodically and merges them with the
baseline ``SYMBOLS`` from configuration to compute the full active set.
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)

# 7 days in seconds
ACTIVE_TTL_SEC = 7 * 24 * 3600

_REDIS_PREFIX = "symbol:active:"


async def touch_symbol(symbol: str) -> None:
    """Record an API access for *symbol*, extending its active TTL.

    Called from API routes/validation on every symbol request.
    Lightweight — single Redis SET with TTL.
    """
    try:
        from src.redis_bus.pool import get_redis_pool
        from datetime import datetime, timezone

        redis = get_redis_pool()
        key = f"{_REDIS_PREFIX}{symbol.upper()}"
        now = datetime.now(timezone.utc).isoformat()
        await redis.set(key, now, ex=ACTIVE_TTL_SEC)
    except Exception:
        # Non-critical — don't break the request
        logger.debug("touch_symbol_failed", symbol=symbol, exc_info=True)


async def get_active_symbols() -> set[str]:
    """Read all currently active symbols from Redis.

    Returns the set of symbol names (upper-cased) that have been
    accessed within the TTL window.
    """
    try:
        from src.redis_bus.pool import get_redis_pool

        redis = get_redis_pool()
        keys: list[bytes | str] = []
        async for key in redis.scan_iter(match=f"{_REDIS_PREFIX}*", count=500):
            keys.append(key)

        symbols = set()
        prefix_len = len(_REDIS_PREFIX)
        for key in keys:
            name = key if isinstance(key, str) else key.decode()
            symbols.add(name[prefix_len:].upper())
        return symbols
    except Exception:
        logger.warning("get_active_symbols_failed", exc_info=True)
        return set()

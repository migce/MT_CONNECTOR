"""
Dynamic symbol registry for the API process.

On startup, loads the full list of MT5-available symbols from Redis
(published by the poller via ``meta:mt5_symbols``).  A background task
refreshes the cache periodically so new symbols become available
without restarting the API.
"""

from __future__ import annotations

import asyncio

import structlog

logger = structlog.get_logger(__name__)

# In-memory cache: {SYMBOL_NAME: description}
_mt5_symbols: dict[str, str] = {}
_loaded = False


async def load_mt5_symbols() -> dict[str, str]:
    """Read the symbol catalogue from Redis and update the local cache."""
    global _loaded
    try:
        import orjson
        from src.redis_bus.pool import get_redis_pool

        redis = get_redis_pool()
        raw = await redis.get("meta:mt5_symbols")
        if raw:
            entries = orjson.loads(raw)
            _mt5_symbols.clear()
            for entry in entries:
                name = entry["name"].upper()
                _mt5_symbols[name] = entry.get("description", "")
            _loaded = True
            logger.info("mt5_symbols_loaded", count=len(_mt5_symbols))
        else:
            logger.warning("mt5_symbols_not_in_redis")
    except Exception:
        logger.warning("mt5_symbols_load_failed", exc_info=True)
    return _mt5_symbols


def get_all_mt5_symbols() -> dict[str, str]:
    """Return cached ``{symbol: description}`` mapping."""
    return _mt5_symbols


def is_symbol_available(symbol: str) -> bool:
    """Check whether *symbol* exists in the MT5 broker's catalogue.

    Falls back to ``True`` if the registry hasn't been populated yet
    (e.g. poller hasn't started), so requests are not blocked.
    """
    if not _loaded:
        return True  # registry not populated — don't block
    return symbol.upper() in _mt5_symbols


# ------------------------------------------------------------------
# Background refresh task (started from API lifespan)
# ------------------------------------------------------------------

async def symbol_registry_refresh_loop(interval: float = 300.0) -> None:
    """Periodically reload the symbol list from Redis."""
    while True:
        try:
            await asyncio.sleep(interval)
            await load_mt5_symbols()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("symbol_registry_refresh_error", exc_info=True)
            await asyncio.sleep(60.0)

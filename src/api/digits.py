"""
Symbol price-digits cache for the API process.

The poller fetches ``symbol_info().digits`` from MT5 and writes the
mapping to Redis (key ``meta:symbol_digits``).  This module loads it
once at API startup and exposes :func:`get_digits` for rounding.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN

import structlog

logger = structlog.get_logger(__name__)

_symbol_digits: dict[str, int] = {}


async def load_symbol_digits() -> dict[str, int]:
    """Read the symbol→digits mapping from Redis and cache locally."""
    try:
        import orjson
        from src.redis_bus.pool import get_redis_pool
        redis = get_redis_pool()
        raw = await redis.get("meta:symbol_digits")
        if raw:
            _symbol_digits.update(orjson.loads(raw))
            logger.info("symbol_digits_loaded", count=len(_symbol_digits), symbols=_symbol_digits)
        else:
            logger.warning("symbol_digits_not_in_redis")
    except Exception:
        logger.warning("symbol_digits_load_failed", exc_info=True)
    return _symbol_digits


def get_digits(symbol: str) -> int:
    """Return cached price digits for *symbol* (default 5)."""
    return _symbol_digits.get(symbol, 5)


def normalize_price(value: float, digits: int) -> float:
    """Round a price value using Decimal to avoid IEEE-754 artifacts.

    >>> normalize_price(1.1521100000000002, 5)
    1.15211
    """
    if value == 0.0:
        return 0.0
    return float(Decimal(str(value)).quantize(
        Decimal(10) ** -digits, rounding=ROUND_HALF_EVEN,
    ))


def normalize_money(value: float, digits: int = 2) -> float:
    """Round a monetary value (profit, swap, commission, balance, etc.).

    >>> normalize_money(50.0300000000001)
    50.03
    """
    if value == 0.0:
        return 0.0
    return float(Decimal(str(value)).quantize(
        Decimal(10) ** -digits, rounding=ROUND_HALF_EVEN,
    ))

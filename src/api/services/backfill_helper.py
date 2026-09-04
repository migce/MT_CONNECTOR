"""
On-demand backfill helper for API route handlers.

Provides ``maybe_backfill_candles`` and ``maybe_backfill_ticks`` which:

1. Query the DB for the requested range.
2. If data is missing or does not cover the requested ``from`` date,
   send a backfill request to the MT5 poller via Redis.
3. Wait for the poller to finish downloading.
4. Re-query and return the data.
"""

from __future__ import annotations

import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from fastapi import HTTPException

from src.config import Timeframe
from src.db import repository as repo

logger = structlog.get_logger(__name__)

# If the earliest candle is more than this many seconds after the requested
# ``from``, we consider the range "uncovered" and trigger a backfill.
_CANDLE_GAP_TOLERANCE_SEC = 120  # 2 minutes (covers M1 granularity)
_TICK_GAP_TOLERANCE_SEC = 60     # 1 minute

# Market is open ~5/7 of the week; add 50 % safety margin for gaps/weekends
_MARKET_HOURS_FACTOR = 1.5

# -----------------------------------------------------------------------
# Backfill cooldown — avoid re-triggering identical backfills.
# -----------------------------------------------------------------------
_BACKFILL_COOLDOWN_SEC = 300  # 5 minutes
_AUTO_BACKFILL_REUSE_SEC = 21_600  # 6 hours after a successful MT5 fetch
_recent_backfills: dict[str, float] = {}  # "SYMBOL:TF_OR_TYPE" → monotonic ts


def _backfill_on_cooldown(symbol: str, key: str) -> bool:
    cache_key = f"{symbol}:{key}"
    ts = _recent_backfills.get(cache_key)
    if ts is None:
        return False
    return (_time.monotonic() - ts) < _BACKFILL_COOLDOWN_SEC


def _record_backfill(symbol: str, key: str) -> None:
    _recent_backfills[f"{symbol}:{key}"] = _time.monotonic()


def _automatic_reuse_scope(
    symbol: str,
    data_type: str,
    timeframe: str | None,
    requested_from: datetime | None,
    requested_to: datetime | None,
    limit: int,
) -> str:
    """Stable cross-worker scope without conflating different history ranges."""
    tf = timeframe or "ticks"
    if requested_from is not None:
        start = requested_from.isoformat()
        end = requested_to.isoformat() if requested_to is not None else "live"
        return f"auto:{symbol}:{data_type}:{tf}:range:{start}:{end}"

    # A live request's calculated from/to moves every call. Bucket it by UTC
    # date and limit so all API workers share one successful availability
    # probe, while a different limit or historical anchor remains independent.
    anchor = (requested_to or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return (
        f"auto:{symbol}:{data_type}:{tf}:limit:{limit}:"
        f"anchor:{anchor.date().isoformat()}"
    )


def _estimate_from_for_limit(
    timeframe: str,
    limit: int,
    reference_time: datetime | None = None,
) -> datetime:
    """Estimate how far back we need to go to satisfy *limit* candles.

    When *reference_time* is provided the lookback is computed relative
    to it (used for ``to``-only requests).  Otherwise ``now()`` is used.
    """
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        tf_sec = 3600  # fallback to H1
    else:
        tf_sec = tf.seconds
    needed_seconds = int(tf_sec * limit * _MARKET_HOURS_FACTOR)
    ref = reference_time or datetime.now(timezone.utc)
    return ref - timedelta(seconds=needed_seconds)


async def maybe_backfill_candles(
    symbol: str,
    timeframe: str,
    dt_from: datetime | None,
    dt_to: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    """
    Return candle rows, triggering an on-demand backfill if the requested
    ``from`` is before our stored data.
    """
    rows = await repo.query_candles(symbol, timeframe, dt_from, dt_to, limit)

    if not _needs_backfill_candles(rows, dt_from, limit, timeframe, dt_to):
        return rows

    # Skip if we recently triggered a backfill for this exact combo
    if _backfill_on_cooldown(symbol, timeframe):
        return rows

    # Soft rate-limit: return available data instead of raising 429
    from src.api.services.validation import backfill_limiter
    try:
        await backfill_limiter.check(symbol)
    except HTTPException:
        logger.warning(
            "backfill_rate_limited",
            symbol=symbol,
            timeframe=timeframe,
        )
        return rows

    from src.api.app import get_backfill_requester
    requester = get_backfill_requester()
    if requester is None:
        return rows  # no requester available, return what we have

    # Determine the range to fetch
    if dt_from is not None:
        bf_from = dt_from
    else:
        # Estimate lookback relative to dt_to (or now if not set)
        ref = dt_to or datetime.now(timezone.utc)
        bf_from = _estimate_from_for_limit(timeframe, limit, reference_time=ref)
    bf_to = dt_to or datetime.now(timezone.utc)

    # Sanity check: bf_from must be before bf_to
    if bf_from >= bf_to:
        return rows

    logger.info(
        "on_demand_backfill_trigger",
        symbol=symbol,
        timeframe=timeframe,
        bf_from=str(bf_from),
        bf_to=str(bf_to),
    )

    result = await requester.request_and_wait(
        symbol=symbol,
        data_type="candles",
        dt_from=bf_from,
        dt_to=bf_to,
        timeframe=timeframe,
        timeout=60.0,
        # Shared across API workers. One successful automatic fill suppresses
        # duplicate work for this symbol/timeframe while callers re-query DB.
        reuse_scope=_automatic_reuse_scope(
            symbol, "candles", timeframe, dt_from, dt_to, limit,
        ),
        reuse_ttl=_AUTO_BACKFILL_REUSE_SEC,
    )

    # Record cooldown only after the attempt (so failures can be retried)
    _record_backfill(symbol, timeframe)

    if result and result.get("status") == "ok" and result.get("rows", 0) > 0:
        # Re-query with the new data
        rows = await repo.query_candles(symbol, timeframe, dt_from, dt_to, limit)

    return rows


async def maybe_backfill_ticks(
    symbol: str,
    dt_from: datetime | None,
    dt_to: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    """
    Return tick rows, triggering an on-demand backfill if the requested
    ``from`` is before our stored data.
    """
    rows = await repo.query_ticks(symbol, dt_from, dt_to, limit)

    if not _needs_backfill_ticks(rows, dt_from, limit):
        return rows

    # Skip if we recently triggered a backfill for ticks on this symbol
    if _backfill_on_cooldown(symbol, "ticks"):
        return rows

    # Soft rate-limit: return available data instead of raising 429
    from src.api.services.validation import backfill_limiter
    try:
        await backfill_limiter.check(symbol)
    except HTTPException:
        logger.warning("backfill_rate_limited", symbol=symbol, data_type="ticks")
        return rows

    from src.api.app import get_backfill_requester
    requester = get_backfill_requester()
    if requester is None:
        return rows

    if dt_from is not None:
        bf_from = dt_from
    else:
        # Estimate: assume ~4 ticks/second on average for major pairs
        needed_seconds = max(limit // 4, 60)
        ref = dt_to or datetime.now(timezone.utc)
        bf_from = ref - timedelta(seconds=needed_seconds)
    bf_to = dt_to or datetime.now(timezone.utc)

    if bf_from >= bf_to:
        return rows

    logger.info(
        "on_demand_backfill_ticks_trigger",
        symbol=symbol,
        bf_from=str(bf_from),
        bf_to=str(bf_to),
    )

    result = await requester.request_and_wait(
        symbol=symbol,
        data_type="ticks",
        dt_from=bf_from,
        dt_to=bf_to,
        timeout=60.0,
        reuse_scope=_automatic_reuse_scope(
            symbol, "ticks", None, dt_from, dt_to, limit,
        ),
        reuse_ttl=_AUTO_BACKFILL_REUSE_SEC,
    )

    _record_backfill(symbol, "ticks")

    if result and result.get("status") == "ok" and result.get("rows", 0) > 0:
        rows = await repo.query_ticks(symbol, dt_from, dt_to, limit)

    return rows


# -----------------------------------------------------------------------
# Heuristics
# -----------------------------------------------------------------------


def _needs_backfill_candles(
    rows: list[dict[str, Any]],
    dt_from: datetime | None,
    limit: int = 1000,
    timeframe: str = "M1",
    dt_to: datetime | None = None,
) -> bool:
    """Return True if data appears to be missing for the requested range."""
    # No explicit start but got fewer rows than requested → need more data
    if dt_from is None:
        if len(rows) < limit:
            return True
        # Even if we have enough rows, check if the latest row is close
        # to dt_to.  If there's a large gap the DB may hold only old data
        # that doesn't cover the requested period.
        if dt_to is not None and rows:
            latest = rows[-1].get("time")
            if latest is not None:
                if not getattr(latest, "tzinfo", None):
                    latest = latest.replace(tzinfo=timezone.utc)
                _dt_to = dt_to
                if not getattr(_dt_to, "tzinfo", None):
                    _dt_to = _dt_to.replace(tzinfo=timezone.utc)
                gap = (_dt_to - latest).total_seconds()
                if gap > _CANDLE_GAP_TOLERANCE_SEC:
                    return True
        return False

    # Empty result for an explicit from → definitely missing
    if not rows:
        return True

    # Check if the earliest row is too far from the requested start
    earliest = rows[0].get("time")
    if earliest is None:
        return True

    if not earliest.tzinfo:
        earliest = earliest.replace(tzinfo=timezone.utc)
    if not dt_from.tzinfo:
        dt_from = dt_from.replace(tzinfo=timezone.utc)

    gap = (earliest - dt_from).total_seconds()
    return gap > _CANDLE_GAP_TOLERANCE_SEC


def _needs_backfill_ticks(
    rows: list[dict[str, Any]],
    dt_from: datetime | None,
    limit: int = 1000,
) -> bool:
    if dt_from is None:
        return len(rows) < limit

    if not rows:
        return True

    earliest = rows[0].get("time_msc")
    if earliest is None:
        return True

    if not earliest.tzinfo:
        earliest = earliest.replace(tzinfo=timezone.utc)
    if not dt_from.tzinfo:
        dt_from = dt_from.replace(tzinfo=timezone.utc)

    gap = (earliest - dt_from).total_seconds()
    return gap > _TICK_GAP_TOLERANCE_SEC

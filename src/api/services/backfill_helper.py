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

from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from src.config import Timeframe
from src.db import repository as repo

logger = structlog.get_logger(__name__)

# If the earliest candle is more than this many seconds after the requested
# ``from``, we consider the range "uncovered" and trigger a backfill.
_CANDLE_GAP_TOLERANCE_SEC = 120  # 2 minutes (covers M1 granularity)
_TICK_GAP_TOLERANCE_SEC = 60     # 1 minute

# Market is open ~5/7 of the week; add 50 % safety margin for gaps/weekends
_MARKET_HOURS_FACTOR = 1.5


def _estimate_from_for_limit(timeframe: str, limit: int) -> datetime:
    """Estimate how far back we need to go to satisfy *limit* candles."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        tf_sec = 3600  # fallback to H1
    else:
        tf_sec = tf.seconds
    needed_seconds = int(tf_sec * limit * _MARKET_HOURS_FACTOR)
    return datetime.now(timezone.utc) - timedelta(seconds=needed_seconds)


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

    if not _needs_backfill_candles(rows, dt_from, limit, timeframe):
        return rows

    from src.api.app import get_backfill_requester
    requester = get_backfill_requester()
    if requester is None:
        return rows  # no requester available, return what we have

    # Determine the range to fetch
    if dt_from is not None:
        bf_from = dt_from
    else:
        # No explicit from — estimate based on limit & timeframe
        bf_from = _estimate_from_for_limit(timeframe, limit)
    bf_to = dt_to or datetime.now(timezone.utc)

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
    )

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

    from src.api.app import get_backfill_requester
    requester = get_backfill_requester()
    if requester is None:
        return rows

    if dt_from is not None:
        bf_from = dt_from
    else:
        # Estimate: assume ~4 ticks/second on average for major pairs
        needed_seconds = max(limit // 4, 60)
        bf_from = datetime.now(timezone.utc) - timedelta(seconds=needed_seconds)
    bf_to = dt_to or datetime.now(timezone.utc)

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
    )

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
) -> bool:
    """Return True if data appears to be missing for the requested range."""
    # No explicit start but got fewer rows than requested → need more data
    if dt_from is None:
        return len(rows) < limit

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

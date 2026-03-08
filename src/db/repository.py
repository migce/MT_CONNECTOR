"""
Database repository — batch write / read operations for ticks, candles,
and sync-state metadata.

All write methods use UPSERT semantics:
  - candles: ON CONFLICT DO UPDATE  (MT5 may revise the current open bar)
  - ticks:   ON CONFLICT DO NOTHING (append-only, duplicates silently skipped)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.engine import get_session_factory

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------
# Ticks
# ---------------------------------------------------------------

_INSERT_TICKS_SQL = text("""
    INSERT INTO ticks (time_msc, symbol, bid, ask, last, volume, flags)
    VALUES (:time_msc, :symbol, :bid, :ask, :last, :volume, :flags)
    ON CONFLICT (symbol, time_msc) DO NOTHING
""")


async def insert_ticks(rows: list[dict[str, Any]]) -> int:
    """Batch-insert tick rows.  Returns number of rows actually inserted."""
    if not rows:
        return 0
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(_INSERT_TICKS_SQL, rows)
            inserted = result.rowcount  # type: ignore[union-attr]
    logger.debug("ticks_inserted", count=inserted, total=len(rows))
    return inserted


# ---------------------------------------------------------------
# Candles
# ---------------------------------------------------------------

_UPSERT_CANDLES_SQL = text("""
    INSERT INTO candles (time, symbol, timeframe, open, high, low, close,
                         tick_volume, real_volume, spread)
    VALUES (:time, :symbol, :timeframe, :open, :high, :low, :close,
            :tick_volume, :real_volume, :spread)
    ON CONFLICT (symbol, timeframe, time)
    DO UPDATE SET
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        tick_volume = EXCLUDED.tick_volume,
        real_volume = EXCLUDED.real_volume,
        spread      = EXCLUDED.spread
""")


async def upsert_candles(rows: list[dict[str, Any]]) -> int:
    """Batch-upsert candle rows.  Returns affected row count."""
    if not rows:
        return 0
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(_UPSERT_CANDLES_SQL, rows)
            affected = result.rowcount  # type: ignore[union-attr]
    logger.debug("candles_upserted", count=affected, total=len(rows))
    return affected


# ---------------------------------------------------------------
# Sync state
# ---------------------------------------------------------------

_UPSERT_SYNC_SQL = text("""
    INSERT INTO sync_state (symbol, data_type, last_synced_at, last_tick_msc, updated_at)
    VALUES (:symbol, :data_type, :last_synced_at, :last_tick_msc, NOW())
    ON CONFLICT (symbol, data_type)
    DO UPDATE SET
        last_synced_at = EXCLUDED.last_synced_at,
        last_tick_msc  = EXCLUDED.last_tick_msc,
        updated_at     = NOW()
""")


async def update_sync_state(
    symbol: str,
    data_type: str,
    last_synced_at: datetime,
    last_tick_msc: int = 0,
) -> None:
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            await session.execute(
                _UPSERT_SYNC_SQL,
                {
                    "symbol": symbol,
                    "data_type": data_type,
                    "last_synced_at": last_synced_at,
                    "last_tick_msc": last_tick_msc,
                },
            )


async def get_sync_state(symbol: str, data_type: str) -> dict[str, Any] | None:
    """Return the sync state row or None."""
    factory = get_session_factory()
    async with factory() as session:
        row = await session.execute(
            text(
                "SELECT last_synced_at, last_tick_msc FROM sync_state "
                "WHERE symbol = :symbol AND data_type = :data_type"
            ),
            {"symbol": symbol, "data_type": data_type},
        )
        r = row.mappings().first()
        return dict(r) if r else None


# ---------------------------------------------------------------
# Query helpers (for API)
# ---------------------------------------------------------------

async def query_candles(
    symbol: str,
    timeframe: str,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Retrieve candle rows for a given symbol & timeframe."""
    clauses = ["symbol = :symbol", "timeframe = :timeframe"]
    params: dict[str, Any] = {"symbol": symbol, "timeframe": timeframe, "limit": limit}

    if dt_from:
        clauses.append("time >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("time <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    sql = text(
        f"SELECT time, symbol, timeframe, open, high, low, close, "
        f"tick_volume, real_volume, spread "
        f"FROM candles WHERE {where} ORDER BY time ASC LIMIT :limit"
    )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


async def query_ticks(
    symbol: str,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Retrieve raw ticks for a given symbol."""
    clauses = ["symbol = :symbol"]
    params: dict[str, Any] = {"symbol": symbol, "limit": limit}

    if dt_from:
        clauses.append("time_msc >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("time_msc <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    sql = text(
        f"SELECT time_msc, symbol, bid, ask, last, volume, flags "
        f"FROM ticks WHERE {where} ORDER BY time_msc ASC LIMIT :limit"
    )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


async def find_candle_gaps(
    symbol: str,
    timeframe: str,
    dt_from: datetime,
    dt_to: datetime,
    interval_seconds: int,
) -> list[datetime]:
    """
    Detect missing candles by generating the expected time series
    and LEFT JOINing against actual candles.

    Returns a list of timestamps where candles are missing.
    """
    sql = text("""
        SELECT gs AS missing_time
        FROM generate_series(
            :dt_from ::timestamptz,
            :dt_to   ::timestamptz,
            :interval::interval
        ) AS gs
        LEFT JOIN candles c
            ON c.symbol    = :symbol
           AND c.timeframe = :timeframe
           AND c.time      = gs
        WHERE c.time IS NULL
        ORDER BY gs
    """)

    params = {
        "dt_from": dt_from,
        "dt_to": dt_to,
        "interval": f"{interval_seconds} seconds",
        "symbol": symbol,
        "timeframe": timeframe,
    }

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [row[0] for row in result.all()]


async def get_latest_candle_time(symbol: str, timeframe: str) -> datetime | None:
    """Return the most recent candle time for a symbol/tf pair."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            text(
                "SELECT MAX(time) FROM candles "
                "WHERE symbol = :symbol AND timeframe = :timeframe"
            ),
            {"symbol": symbol, "timeframe": timeframe},
        )
        row = result.scalar()
        return row


# ---------------------------------------------------------------
# Custom timeframe — aggregate from M1 candles via time_bucket
# ---------------------------------------------------------------

async def query_custom_tf_candles(
    symbol: str,
    bucket_seconds: int,
    tf_label: str,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 1000,
    source_tf: str = "M1",
) -> list[dict[str, Any]]:
    """
    Build candles of arbitrary duration from stored M1 (or other source)
    candles using TimescaleDB ``time_bucket``.

    Parameters
    ----------
    symbol : str
        Instrument name (EURUSD, …).
    bucket_seconds : int
        Custom bar width in seconds (e.g. 120 for M2, 21600 for H6).
    tf_label : str
        Human label written into the ``timeframe`` field of results.
    dt_from / dt_to : datetime, optional
        Time range filter.
    limit : int
        Max rows to return.
    source_tf : str
        Source timeframe to aggregate from.  Defaults to "M1".
        For large buckets (>= 1h) you can pass "H1" for speed.
    """
    bucket = f"{bucket_seconds} seconds"
    clauses = ["c.symbol = :symbol", "c.timeframe = :source_tf"]
    params: dict[str, Any] = {
        "symbol": symbol,
        "source_tf": source_tf,
        "tf_label": tf_label,
        "limit": limit,
    }

    if dt_from:
        clauses.append("c.time >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("c.time <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    # bucket_seconds is always a positive integer we control – safe to embed
    sql = text(f"""
        SELECT
            time_bucket(interval '{bucket_seconds} seconds', c.time) AS time,
            c.symbol,
            :tf_label                                        AS timeframe,
            (ARRAY_AGG(c.open  ORDER BY c.time ASC))[1]      AS open,
            MAX(c.high)                                      AS high,
            MIN(c.low)                                       AS low,
            (ARRAY_AGG(c.close ORDER BY c.time DESC))[1]     AS close,
            SUM(c.tick_volume)::bigint                       AS tick_volume,
            SUM(c.real_volume)::bigint                       AS real_volume,
            MAX(c.spread)                                    AS spread
        FROM candles c
        WHERE {where}
        GROUP BY time_bucket(interval '{bucket_seconds} seconds', c.time), c.symbol
        ORDER BY time ASC
        LIMIT :limit
    """)

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


# ---------------------------------------------------------------
# Tick bars — build OHLCV bars from every N raw ticks
# ---------------------------------------------------------------

async def query_tick_bars(
    symbol: str,
    tick_count: int,
    tf_label: str,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 1000,
    price_field: str = "bid",
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    """
    Build OHLCV bars where each bar is formed from exactly *tick_count*
    consecutive ticks.

    Parameters
    ----------
    symbol : str
        Instrument name.
    tick_count : int
        Number of ticks per bar (e.g. 100, 500).
    tf_label : str
        Label written into the ``timeframe`` field (e.g. "T100").
    dt_from / dt_to : datetime, optional
        Time range filter (applied **before** grouping).
    limit : int
        Maximum number of bars to return.
    price_field : str
        Which tick price to use for OHLCV: ``bid`` (default), ``ask``,
        ``last``, or ``mid`` (computed as (bid+ask)/2).
    include_incomplete : bool
        If True, the last bar may have fewer than *tick_count* ticks.
    """
    # Build the price expression
    if price_field == "mid":
        price_expr = "(t.bid + t.ask) / 2.0"
    elif price_field in ("bid", "ask", "last"):
        price_expr = f"t.{price_field}"
    else:
        raise ValueError(f"Unknown price_field '{price_field}'. Use bid/ask/last/mid.")

    clauses = ["t.symbol = :symbol"]
    params: dict[str, Any] = {
        "symbol": symbol,
        "tick_count": tick_count,
        "tf_label": tf_label,
        "limit": limit,
    }
    if dt_from:
        clauses.append("t.time_msc >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("t.time_msc <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    having = "" if include_incomplete else "HAVING COUNT(*) = :tick_count"

    sql = text(f"""
        WITH numbered AS (
            SELECT
                t.time_msc,
                {price_expr}   AS price,
                t.volume,
                t.flags,
                (ROW_NUMBER() OVER (ORDER BY t.time_msc ASC) - 1)
                    / :tick_count  AS bar_idx
            FROM ticks t
            WHERE {where}
        )
        SELECT
            MIN(time_msc)                                    AS time,
            :symbol                                          AS symbol,
            :tf_label                                        AS timeframe,
            (ARRAY_AGG(price ORDER BY time_msc ASC))[1]      AS open,
            MAX(price)                                       AS high,
            MIN(price)                                       AS low,
            (ARRAY_AGG(price ORDER BY time_msc DESC))[1]     AS close,
            COUNT(*)::bigint                                 AS tick_volume,
            COALESCE(SUM(volume), 0)::bigint                 AS real_volume,
            0                                                AS spread
        FROM numbered
        GROUP BY bar_idx
        {having}
        ORDER BY MIN(time_msc) ASC
        LIMIT :limit
    """)

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]

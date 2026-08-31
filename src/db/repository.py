"""
Database repository — batch write / read operations for ticks, candles,
and sync-state metadata.

All write methods use UPSERT semantics:
  - candles: ON CONFLICT DO UPDATE  (MT5 may revise the current open bar)
  - ticks:   ON CONFLICT DO NOTHING (append-only, duplicates silently skipped)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import structlog
from sqlalchemy import text
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import Timeframe
from src.db.engine import get_session_factory

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = structlog.get_logger(__name__)

_LATEST_CANDLE_MIN_LOOKBACK = timedelta(days=14)
_LATEST_CANDLE_GAP_FACTOR = 4

# --- Retry decorator for transient DB errors ---
_db_retry = retry(
    retry=retry_if_exception_type((OSError, ConnectionError, TimeoutError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    reraise=True,
)


# ---------------------------------------------------------------
# Ticks
# ---------------------------------------------------------------

_INSERT_TICKS_SQL = text("""
    INSERT INTO ticks (time_msc, symbol, bid, ask, last, volume, flags)
    VALUES (:time_msc, :symbol, :bid, :ask, :last, :volume, :flags)
    ON CONFLICT (symbol, time_msc) DO NOTHING
""")


@_db_retry
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

_INSERT_CANDLES_SQL = text("""
    INSERT INTO candles (time, symbol, timeframe, open, high, low, close,
                         tick_volume, real_volume, spread)
    VALUES (:time, :symbol, :timeframe, :open, :high, :low, :close,
            :tick_volume, :real_volume, :spread)
    ON CONFLICT (symbol, timeframe, time) DO NOTHING
""")


@_db_retry
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


@_db_retry
async def insert_candles(rows: list[dict[str, Any]]) -> int:
    """Insert only absent candles, preserving canonical stored rows."""
    if not rows:
        return 0
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(_INSERT_CANDLES_SQL, rows)
        return int(result.rowcount or 0)  # type: ignore[union-attr]


_UPSERT_TICKS_SQL = text("""
    INSERT INTO ticks (time_msc, symbol, bid, ask, last, volume, flags)
    VALUES (:time_msc, :symbol, :bid, :ask, :last, :volume, :flags)
    ON CONFLICT (symbol, time_msc) DO UPDATE SET
        bid=EXCLUDED.bid, ask=EXCLUDED.ask, last=EXCLUDED.last,
        volume=EXCLUDED.volume, flags=EXCLUDED.flags
""")


@_db_retry
async def upsert_ticks(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(_UPSERT_TICKS_SQL, rows)
        return int(result.rowcount or 0)  # type: ignore[union-attr]


@_db_retry
async def insert_missing_candles_from_ticks(
    symbol: str,
    timeframe: str,
    bucket_seconds: int,
    dt_from: datetime,
    dt_to: datetime,
    spread_scale: int,
) -> int:
    """Build only absent closed candles from persisted source ticks.

    This is a recovery path for a terminal candle-history tail that did not
    advance after a Connector interruption. Existing native MT5 candles are
    never overwritten; every OHLCV value is aggregated from actual bid ticks.
    ``dt_to`` is exclusive so the currently forming bucket is not persisted.
    """
    if dt_from >= dt_to:
        return 0

    sql = text("""
        INSERT INTO candles (
            time, symbol, timeframe, open, high, low, close,
            tick_volume, real_volume, spread
        )
        SELECT
            time_bucket(
                make_interval(secs => CAST(:bucket_seconds AS double precision)),
                t.time_msc
            )                                                     AS time,
            :symbol                                               AS symbol,
            :timeframe                                            AS timeframe,
            (ARRAY_AGG(t.bid ORDER BY t.time_msc ASC))[1]         AS open,
            MAX(t.bid)                                            AS high,
            MIN(t.bid)                                            AS low,
            (ARRAY_AGG(t.bid ORDER BY t.time_msc DESC))[1]        AS close,
            COUNT(*)::bigint                                      AS tick_volume,
            0::bigint                                             AS real_volume,
            ROUND(AVG(GREATEST(t.ask - t.bid, 0)) * :spread_scale)::integer AS spread
        FROM ticks t
        WHERE t.symbol = :symbol
          AND t.time_msc >= :dt_from
          AND t.time_msc < :dt_to
          AND t.bid > 0
          AND t.ask >= t.bid
        GROUP BY time_bucket(
            make_interval(secs => CAST(:bucket_seconds AS double precision)),
            t.time_msc
        )
        ON CONFLICT (symbol, timeframe, time) DO NOTHING
        RETURNING time
    """)
    params = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bucket_seconds": bucket_seconds,
        "dt_from": dt_from,
        "dt_to": dt_to,
        "spread_scale": spread_scale,
    }
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(sql, params)
        inserted = len(result.all())
    logger.info(
        "missing_candles_rebuilt_from_ticks",
        symbol=symbol,
        timeframe=timeframe,
        inserted=inserted,
        range_from=str(dt_from),
        range_to=str(dt_to),
    )
    return inserted


@_db_retry
async def upsert_candles_and_sync(
    rows: list[dict[str, Any]],
    sync_rows: list[dict[str, Any]],
) -> int:
    """Persist one collector cycle atomically in a single transaction.

    ``rows`` contains only candles whose persisted OHLCV payload changed.
    ``sync_rows`` contains symbol/timeframe pairs whose latest bar timestamp
    advanced; unchanged rows already have the same authoritative sync value.
    """
    if not rows and not sync_rows:
        return 0

    factory = get_session_factory()
    async with factory() as session, session.begin():
        affected = 0
        if rows:
            result = await session.execute(_UPSERT_CANDLES_SQL, rows)
            affected = result.rowcount  # type: ignore[union-attr]
        if sync_rows:
            await session.execute(_UPSERT_SYNC_SQL, sync_rows)

    logger.debug(
        "collector_candle_batch_persisted",
        candles=affected,
        candle_rows=len(rows),
        sync_rows=len(sync_rows),
    )
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


@_db_retry
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

@_db_retry
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
    latest_sql_template = (
        "SELECT * FROM ("
        "  SELECT time, symbol, timeframe, open, high, low, close, "
        "  tick_volume, real_volume, spread "
        "  FROM candles WHERE {where} ORDER BY time DESC LIMIT :limit"
        ") sub ORDER BY time ASC"
    )
    if dt_from:
        # Explicit start — return oldest-first from that point
        sql = text(
            f"SELECT time, symbol, timeframe, open, high, low, close, "
            f"tick_volume, real_volume, spread "
            f"FROM candles WHERE {where} ORDER BY time ASC LIMIT :limit"
        )
    else:
        # No start specified (to-only or no range) —
        # return the LATEST N candles (optionally capped by dt_to)
        sql = text(latest_sql_template.format(where=where))

    factory = get_session_factory()
    async with factory() as session:
        if not dt_from:
            try:
                timeframe_seconds = Timeframe.from_string(timeframe).seconds
            except ValueError:
                timeframe_seconds = 0
            if timeframe_seconds:
                anchor = dt_to or datetime.now(UTC)
                estimated = timedelta(
                    seconds=timeframe_seconds * max(1, limit) * _LATEST_CANDLE_GAP_FACTOR,
                )
                params["recent_from"] = anchor - max(
                    estimated,
                    _LATEST_CANDLE_MIN_LOOKBACK,
                )
                recent_where = f"{where} AND time >= :recent_from"
                recent_result = await session.execute(
                    text(latest_sql_template.format(where=recent_where)),
                    params,
                )
                recent_rows = [dict(row._mapping) for row in recent_result.all()]
                if len(recent_rows) >= limit:
                    return recent_rows

                # Sparse/offline symbols and unusually long market closures
                # retain the exact historical behavior by falling back to the
                # unrestricted query when the bounded window is insufficient.
                params.pop("recent_from", None)
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


@_db_retry
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
    if dt_from or dt_to:
        sql = text(
            f"SELECT time_msc, symbol, bid, ask, last, volume, flags "
            f"FROM ticks WHERE {where} ORDER BY time_msc ASC LIMIT :limit"
        )
    else:
        # No range — return the LATEST N ticks in ascending order
        sql = text(
            f"SELECT * FROM ("
            f"  SELECT time_msc, symbol, bid, ask, last, volume, flags "
            f"  FROM ticks WHERE {where} ORDER BY time_msc DESC LIMIT :limit"
            f") sub ORDER BY time_msc ASC"
        )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


# ---------------------------------------------------------------
# Spread history (from candles — fast, or from ticks — granular)
# ---------------------------------------------------------------

@_db_retry
async def query_spread_from_candles(
    symbol: str,
    timeframe: str = "M1",
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return spread time-series from the candles table."""
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
        f"SELECT time, spread FROM candles "
        f"WHERE {where} ORDER BY time ASC LIMIT :limit"
    )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


@_db_retry
async def query_spread_from_ticks(
    symbol: str,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return computed spread (ask − bid) from raw ticks."""
    clauses = ["symbol = :symbol", "ask IS NOT NULL", "bid IS NOT NULL"]
    params: dict[str, Any] = {"symbol": symbol, "limit": limit}

    if dt_from:
        clauses.append("time_msc >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("time_msc <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    sql = text(
        f"SELECT time_msc AS time, (ask - bid) AS spread_raw "
        f"FROM ticks WHERE {where} ORDER BY time_msc ASC LIMIT :limit"
    )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


@_db_retry
async def query_spread_aggregated(
    symbol: str,
    bucket: str = "1 hour",
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """
    Aggregate tick-level spread into time buckets using TimescaleDB
    ``time_bucket``. Returns avg / min / max spread per bucket.
    """
    clauses = ["symbol = :symbol", "ask IS NOT NULL", "bid IS NOT NULL"]
    params: dict[str, Any] = {"symbol": symbol, "limit": limit}

    if dt_from:
        clauses.append("time_msc >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("time_msc <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    # bucket is validated against _VALID_BUCKETS whitelist in the route,
    # so safe to inline. asyncpg cannot bind interval parameters directly.
    # Wrap in subquery to get the LATEST N buckets in ascending order.
    sql = text(
        f"SELECT * FROM ("
        f"  SELECT time_bucket('{bucket}'::interval, time_msc) AS time, "
        f"    avg(ask - bid) AS spread_avg, "
        f"    min(ask - bid) AS spread_min, "
        f"    max(ask - bid) AS spread_max "
        f"  FROM ticks WHERE {where} "
        f"  GROUP BY 1 ORDER BY 1 DESC LIMIT :limit"
        f") sub ORDER BY time ASC"
    )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


@_db_retry
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
            CAST(:dt_from AS timestamptz),
            CAST(:dt_to   AS timestamptz),
            CAST(:gap_interval AS interval)
        ) AS gs
        LEFT JOIN candles c
            ON c.symbol    = :symbol
           AND c.timeframe = :timeframe
           AND c.time      = gs
        WHERE c.time IS NULL
        ORDER BY gs
        LIMIT 50000
    """)

    params = {
        "dt_from": dt_from,
        "dt_to": dt_to,
        "gap_interval": timedelta(seconds=interval_seconds),
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
    clauses = ["c.symbol = :symbol", "c.timeframe = :source_tf"]
    params: dict[str, Any] = {
        "symbol": symbol,
        "source_tf": source_tf,
        "tf_label": tf_label,
        "limit": limit,
        "bucket_seconds": bucket_seconds,
    }

    if dt_from:
        clauses.append("c.time >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("c.time <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)

    def build_inner(effective_where: str) -> str:
        return f"""
            SELECT
                time_bucket(make_interval(secs => CAST(:bucket_seconds AS double precision)), c.time) AS time,
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
            WHERE {effective_where}
            GROUP BY time_bucket(make_interval(secs => CAST(:bucket_seconds AS double precision)), c.time), c.symbol
        """

    inner = build_inner(where)

    if dt_from:
        sql = text(f"{inner} ORDER BY time ASC LIMIT :limit")
    else:
        # No start specified (to-only or no range) —
        # return the LATEST N bars in ascending order
        sql = text(
            f"SELECT * FROM ({inner} ORDER BY time DESC LIMIT :limit) sub ORDER BY time ASC"
        )

    factory = get_session_factory()
    async with factory() as session:
        if not dt_from:
            anchor = dt_to or datetime.now(UTC)
            estimated = timedelta(
                seconds=bucket_seconds * max(1, limit) * _LATEST_CANDLE_GAP_FACTOR,
            )
            params["recent_from"] = anchor - max(
                estimated,
                _LATEST_CANDLE_MIN_LOOKBACK,
            )
            recent_where = (
                f"{where} AND c.time >= "
                "time_bucket("
                "make_interval(secs => CAST(:bucket_seconds AS double precision)), "
                "CAST(:recent_from AS timestamptz)"
                ")"
            )
            recent_inner = build_inner(recent_where)
            recent_sql = text(
                f"SELECT * FROM ({recent_inner} ORDER BY time DESC LIMIT :limit) "
                "sub ORDER BY time ASC"
            )
            recent_result = await session.execute(recent_sql, params)
            recent_rows = [dict(row._mapping) for row in recent_result.all()]
            if len(recent_rows) >= limit:
                return recent_rows

            params.pop("recent_from", None)
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
    price_field: Literal["bid", "ask", "last", "mid"] = "bid",
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
        # The result can consume at most this many source rows.  Bounding the
        # indexed tick scan before ROW_NUMBER/GROUP BY avoids sorting and
        # spilling the symbol's complete history for every T<n> request.
        "source_limit": tick_count * limit,
    }
    if dt_from:
        clauses.append("t.time_msc >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        clauses.append("t.time_msc <= :dt_to")
        params["dt_to"] = dt_to

    where = " AND ".join(clauses)
    having = "" if include_incomplete else "HAVING COUNT(*) = :tick_count"

    if dt_from or dt_to:
        # Explicit range — oldest-first
        sql = text(f"""
            WITH source_ticks AS (
                SELECT
                    t.time_msc,
                    {price_expr}   AS price,
                    t.volume,
                    t.flags
                FROM ticks t
                WHERE {where}
                ORDER BY t.time_msc ASC
                LIMIT :source_limit
            ),
            numbered AS (
                SELECT
                    time_msc,
                    price,
                    volume,
                    flags,
                    (ROW_NUMBER() OVER (ORDER BY t.time_msc ASC) - 1)
                        / :tick_count  AS bar_idx
                FROM source_ticks t
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
    else:
        # No range — return the LATEST N bars in ascending order
        sql = text(f"""
            WITH source_ticks AS (
                SELECT
                    t.time_msc,
                    {price_expr}   AS price,
                    t.volume,
                    t.flags
                FROM ticks t
                WHERE {where}
                ORDER BY t.time_msc DESC
                LIMIT :source_limit
            ),
            numbered AS (
                SELECT
                    time_msc,
                    price,
                    volume,
                    flags,
                    (ROW_NUMBER() OVER (ORDER BY t.time_msc DESC) - 1)
                        / :tick_count  AS bar_idx
                FROM source_ticks t
            ),
            bars AS (
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
                ORDER BY MIN(time_msc) DESC
                LIMIT :limit
            )
            SELECT * FROM bars ORDER BY time ASC
        """)

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


# ---------------------------------------------------------------
# Coverage statistics (for /api/v1/coverage)
# ---------------------------------------------------------------

async def query_candle_coverage() -> list[dict[str, Any]]:
    """Per-symbol, per-timeframe: first bar, last bar, total count."""
    sql = text("""
        SELECT symbol, timeframe,
               MIN(time) AS first_bar,
               MAX(time) AS last_bar,
               COUNT(*)  AS total
        FROM candles
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql)
        return [dict(r._mapping) for r in result.all()]


async def query_tick_coverage() -> list[dict[str, Any]]:
    """Per-symbol: first tick, last tick, total count."""
    sql = text("""
        SELECT symbol,
               MIN(time_msc) AS first_tick,
               MAX(time_msc) AS last_tick,
               COUNT(*)      AS total
        FROM ticks
        GROUP BY symbol
        ORDER BY symbol
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql)
        return [dict(r._mapping) for r in result.all()]


async def query_candle_bounds(
    timeframes: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Return exact candle first/last bounds without aggregate row counts.

    The primary ``(symbol, timeframe, time DESC)`` index resolves the first
    and latest timestamps for each sync-state row without scanning all candles.
    """
    clauses = ["s.data_type <> 'tick'"]
    params: dict[str, Any] = {}
    if timeframes:
        clauses.append("s.data_type = ANY(CAST(:timeframes AS text[]))")
        params["timeframes"] = [value.upper() for value in timeframes]

    sql = text(f"""
        SELECT s.symbol,
               s.data_type AS timeframe,
               first_row.time AS first_bar,
               last_row.time AS last_bar,
               0::bigint AS total
        FROM sync_state s
        LEFT JOIN LATERAL (
            SELECT c.time
            FROM candles c
            WHERE c.symbol = s.symbol
              AND c.timeframe = s.data_type
            ORDER BY c.time ASC
            LIMIT 1
        ) first_row ON TRUE
        LEFT JOIN LATERAL (
            SELECT c.time
            FROM candles c
            WHERE c.symbol = s.symbol
              AND c.timeframe = s.data_type
            ORDER BY c.time DESC
            LIMIT 1
        ) last_row ON TRUE
        WHERE {' AND '.join(clauses)}
        ORDER BY s.symbol, s.data_type
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]


async def query_tick_bounds() -> list[dict[str, Any]]:
    """Return exact tick first/last bounds without scanning/counting ticks."""
    sql = text("""
        SELECT s.symbol,
               first_row.time_msc AS first_tick,
               last_row.time_msc AS last_tick,
               0::bigint AS total
        FROM sync_state s
        LEFT JOIN LATERAL (
            SELECT t.time_msc
            FROM ticks t
            WHERE t.symbol = s.symbol
            ORDER BY t.time_msc ASC
            LIMIT 1
        ) first_row ON TRUE
        LEFT JOIN LATERAL (
            SELECT t.time_msc
            FROM ticks t
            WHERE t.symbol = s.symbol
            ORDER BY t.time_msc DESC
            LIMIT 1
        ) last_row ON TRUE
        WHERE s.data_type = 'tick'
        ORDER BY s.symbol
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql)
        return [dict(r._mapping) for r in result.all()]


async def query_all_sync_states() -> list[dict[str, Any]]:
    """Return all sync_state rows."""
    sql = text("""
        SELECT symbol, data_type, last_synced_at
        FROM sync_state
        ORDER BY symbol, data_type
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql)
        return [dict(r._mapping) for r in result.all()]


# ---------------------------------------------------------------
# Daily statistics persistence
# ---------------------------------------------------------------

async def upsert_daily_poller_stats(
    date_val: datetime,
    *,
    ticks_received: int = 0,
    ticks_flushed: int = 0,
    candles_upserted: int = 0,
    redis_published: int = 0,
    poller_errors: int = 0,
    reconnects: int = 0,
    gaps_found: int = 0,
    poller_uptime_sec: float = 0.0,
) -> None:
    """UPSERT-add poller metric deltas for the given date."""
    sql = text("""
        INSERT INTO daily_stats (
            date, ticks_received, ticks_flushed, candles_upserted,
            redis_published, poller_errors, reconnects, gaps_found,
            poller_uptime_sec, updated_at
        ) VALUES (
            :dt, :ticks_r, :ticks_f, :candles, :redis_p,
            :p_err, :reconn, :gaps, :uptime, NOW()
        )
        ON CONFLICT (date) DO UPDATE SET
            ticks_received    = daily_stats.ticks_received    + EXCLUDED.ticks_received,
            ticks_flushed     = daily_stats.ticks_flushed     + EXCLUDED.ticks_flushed,
            candles_upserted  = daily_stats.candles_upserted  + EXCLUDED.candles_upserted,
            redis_published   = daily_stats.redis_published   + EXCLUDED.redis_published,
            poller_errors     = daily_stats.poller_errors      + EXCLUDED.poller_errors,
            reconnects        = daily_stats.reconnects         + EXCLUDED.reconnects,
            gaps_found        = daily_stats.gaps_found         + EXCLUDED.gaps_found,
            poller_uptime_sec = daily_stats.poller_uptime_sec  + EXCLUDED.poller_uptime_sec,
            updated_at        = NOW()
    """)
    factory = get_session_factory()
    async with factory() as session:
        await session.execute(sql, {
            "dt": date_val.date() if isinstance(date_val, datetime) else date_val,
            "ticks_r": ticks_received,
            "ticks_f": ticks_flushed,
            "candles": candles_upserted,
            "redis_p": redis_published,
            "p_err": poller_errors,
            "reconn": reconnects,
            "gaps": gaps_found,
            "uptime": poller_uptime_sec,
        })
        await session.commit()


async def upsert_daily_api_stats(
    date_val: datetime,
    *,
    api_requests: int = 0,
    api_errors: int = 0,
    api_latency_sum_ms: float = 0.0,
    api_latency_count: int = 0,
    api_uptime_sec: float = 0.0,
) -> None:
    """UPSERT-add API metric deltas for the given date."""
    sql = text("""
        INSERT INTO daily_stats (
            date, api_requests, api_errors, api_latency_sum_ms,
            api_latency_count, api_uptime_sec, updated_at
        ) VALUES (
            :dt, :reqs, :errs, :lat_sum, :lat_cnt, :uptime, NOW()
        )
        ON CONFLICT (date) DO UPDATE SET
            api_requests       = daily_stats.api_requests       + EXCLUDED.api_requests,
            api_errors         = daily_stats.api_errors         + EXCLUDED.api_errors,
            api_latency_sum_ms = daily_stats.api_latency_sum_ms + EXCLUDED.api_latency_sum_ms,
            api_latency_count  = daily_stats.api_latency_count  + EXCLUDED.api_latency_count,
            api_uptime_sec     = daily_stats.api_uptime_sec     + EXCLUDED.api_uptime_sec,
            updated_at         = NOW()
    """)
    factory = get_session_factory()
    async with factory() as session:
        await session.execute(sql, {
            "dt": date_val.date() if isinstance(date_val, datetime) else date_val,
            "reqs": api_requests,
            "errs": api_errors,
            "lat_sum": api_latency_sum_ms,
            "lat_cnt": api_latency_count,
            "uptime": api_uptime_sec,
        })
        await session.commit()


async def load_today_poller_stats() -> dict[str, Any] | None:
    """Load today's daily_stats row (for baseline restoration on startup)."""
    sql = text("""
        SELECT ticks_received, ticks_flushed, candles_upserted,
               redis_published, poller_errors, reconnects, gaps_found,
               poller_uptime_sec
        FROM daily_stats
        WHERE date = CURRENT_DATE
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql)
        row = result.first()
        return dict(row._mapping) if row else None


# ---------------------------------------------------------------
# Service uptime log
# ---------------------------------------------------------------

async def insert_uptime_log(
    rows: list[dict[str, Any]],
) -> None:
    """Batch-insert uptime/downtime snapshots.

    Each dict must contain: ts, service, up_sec, down_sec.
    """
    if not rows:
        return
    sql = text("""
        INSERT INTO service_uptime_log (ts, service, up_sec, down_sec)
        VALUES (:ts, :service, :up_sec, :down_sec)
    """)
    factory = get_session_factory()
    async with factory() as session:
        await session.execute(sql, rows)
        await session.commit()


async def query_uptime_summary(
    interval: str = "24 hours",
) -> dict[str, tuple[float, float, float]]:
    """Aggregate uptime for each service over the given interval.

    *interval* must be a valid PostgreSQL interval literal
    (e.g. ``'24 hours'``, ``'30 days'``).

    Returns ``{service: (up_sec, down_sec, uptime_pct)}``.
    """
    sql = text(f"""
        SELECT service,
               COALESCE(SUM(up_sec), 0)   AS up_sec,
               COALESCE(SUM(down_sec), 0) AS down_sec
        FROM service_uptime_log
        WHERE ts > NOW() - INTERVAL '{interval}'
        GROUP BY service
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql)
        out: dict[str, tuple[float, float, float]] = {}
        for row in result.all():
            r = row._mapping
            up = float(r["up_sec"])
            dn = float(r["down_sec"])
            total = up + dn
            pct = (up / total * 100) if total > 0 else 0.0
            out[r["service"]] = (up, dn, pct)
        return out


async def query_daily_stats(
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Return daily_stats rows ordered by date descending."""
    clauses = []
    params: dict[str, Any] = {"lim": limit}
    if from_date is not None:
        clauses.append("date >= :from_dt")
        params["from_dt"] = from_date.date() if isinstance(from_date, datetime) else from_date
    if to_date is not None:
        clauses.append("date <= :to_dt")
        params["to_dt"] = to_date.date() if isinstance(to_date, datetime) else to_date

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = text(f"""
        SELECT date, ticks_received, ticks_flushed, candles_upserted,
               redis_published, poller_errors, reconnects, gaps_found,
               poller_uptime_sec,
               api_requests, api_errors, api_latency_sum_ms,
               api_latency_count, api_uptime_sec, updated_at
        FROM daily_stats
        {where}
        ORDER BY date DESC
        LIMIT :lim
    """)
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(sql, params)
        return [dict(r._mapping) for r in result.all()]

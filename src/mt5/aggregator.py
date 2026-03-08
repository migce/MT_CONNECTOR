"""
Candle aggregator — builds higher timeframes from M1 data.

When backfilling, we typically load M1 candles from MT5 and then need
to build M5, M15, H1, H4, D1 from the M1 data. This module provides
that aggregation logic, both:

- **In-database** via SQL ``time_bucket`` (preferred — leverages
  TimescaleDB's native aggregation).
- **In-memory** via pandas (fallback / ad-hoc usage).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog
from sqlalchemy import text

from src.config import Timeframe
from src.db.engine import get_session_factory
from src.db import repository as repo

logger = structlog.get_logger(__name__)


async def aggregate_candles_from_db(
    symbol: str,
    source_tf: Timeframe,
    target_tf: Timeframe,
    dt_from: datetime,
    dt_to: datetime,
) -> int:
    """
    Aggregate candles in the database using SQL ``time_bucket``.

    Reads *source_tf* candles and writes aggregated *target_tf* candles
    via UPSERT.  Returns the number of target candles written.
    """
    bucket = f"{target_tf.seconds} seconds"

    sql = text(f"""
        INSERT INTO candles (time, symbol, timeframe, open, high, low, close,
                             tick_volume, real_volume, spread)
        SELECT
            time_bucket(:bucket, c.time) AS bucket_time,
            c.symbol,
            :target_tf                     AS timeframe,
            (ARRAY_AGG(c.open ORDER BY c.time ASC))[1]  AS open,
            MAX(c.high)                                  AS high,
            MIN(c.low)                                   AS low,
            (ARRAY_AGG(c.close ORDER BY c.time DESC))[1] AS close,
            SUM(c.tick_volume)                           AS tick_volume,
            SUM(c.real_volume)                           AS real_volume,
            MAX(c.spread)                                AS spread
        FROM candles c
        WHERE c.symbol    = :symbol
          AND c.timeframe = :source_tf
          AND c.time >= :dt_from
          AND c.time <  :dt_to
        GROUP BY bucket_time, c.symbol
        ORDER BY bucket_time
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

    params = {
        "bucket": bucket,
        "symbol": symbol,
        "source_tf": source_tf.value,
        "target_tf": target_tf.value,
        "dt_from": dt_from,
        "dt_to": dt_to,
    }

    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            result = await session.execute(sql, params)
            count = result.rowcount  # type: ignore[union-attr]

    logger.info(
        "aggregated_candles",
        symbol=symbol,
        source=source_tf.value,
        target=target_tf.value,
        rows=count,
    )
    return count


async def reaggregate_all(
    symbol: str,
    dt_from: datetime,
    dt_to: datetime,
) -> None:
    """
    Re-aggregate all higher timeframes from M1 for the given symbol
    and time range.  Called after a backfill to keep data consistent.
    """
    from src.config import TF_AGGREGATION_MAP

    for target_tf, source_tf in TF_AGGREGATION_MAP.items():
        await aggregate_candles_from_db(
            symbol, source_tf, target_tf, dt_from, dt_to
        )

    logger.info(
        "reaggregate_all_done",
        symbol=symbol,
        range_from=str(dt_from),
        range_to=str(dt_to),
    )

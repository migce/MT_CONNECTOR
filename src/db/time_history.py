"""Latest N time bars from an indexed, row-bounded source tail.

Never translate trading bars into calendar hours or aggregate an unbounded
hypertable. One extra bucket ensures the oldest returned OHLC is not clipped
by the source-row limit. This proves depth, not uninterrupted market coverage.
"""
from sqlalchemy import text

from src.config import Timeframe
from src.db.heavy_reads import heavy_read_session, validate_source_budget


async def latest_time_bars(symbol, source_tf, bucket_seconds, tf_label, limit, end=None):
    source_seconds = Timeframe(source_tf).seconds
    if bucket_seconds < source_seconds or bucket_seconds % source_seconds or limit < 1:
        raise ValueError('Time bucket must be an exact multiple of its source')
    source_limit = (limit + 1) * (bucket_seconds // source_seconds)
    validate_source_budget(source_limit)
    upper = 'AND time <= :end' if end is not None else ''
    sql = text(f"""
        WITH tail AS MATERIALIZED (
            SELECT time, symbol, open, high, low, close, tick_volume, real_volume, spread
            FROM candles WHERE symbol=:symbol AND timeframe=:source_tf {upper}
            ORDER BY time DESC LIMIT :source_limit
        ), bars AS (
            SELECT time_bucket(make_interval(secs => CAST(:seconds AS double precision)), time) AS time,
                symbol, :tf_label AS timeframe,
                (ARRAY_AGG(open ORDER BY time ASC))[1] AS open,
                MAX(high) AS high, MIN(low) AS low,
                (ARRAY_AGG(close ORDER BY time DESC))[1] AS close,
                SUM(tick_volume)::bigint AS tick_volume,
                SUM(real_volume)::bigint AS real_volume, MAX(spread) AS spread
            FROM tail GROUP BY 1, symbol ORDER BY 1 DESC LIMIT :limit
        ) SELECT * FROM bars ORDER BY time ASC
    """)
    async with heavy_read_session() as session:
        result = await session.execute(sql, dict(symbol=symbol, source_tf=source_tf,
            seconds=bucket_seconds, tf_label=tf_label, source_limit=source_limit, limit=limit, end=end))
        return [dict(row._mapping) for row in result.all()]

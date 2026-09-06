"""Bounded, read-only plans for explicitly requested chart-history repair.

Dates use the Connector's existing broker-clock convention. A plan estimates a
window, never promises a count or treats an empty market session as corruption.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from src.config import Timeframe, custom_timeframe_source, parse_custom_timeframe
from src.db.heavy_reads import HeavyReadTimeout, heavy_read_session
from src.db.symbol_management import get_retention_days


class ChartHistoryRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=64)
    timeframe: str = Field(pattern=r"^[MHDWT][1-9][0-9]{0,5}$")
    required_bars: int = Field(ge=1, le=15000)
    loaded_bars: int = Field(default=0, ge=0, le=15000)
    anchor: datetime | None = None

    @field_validator('anchor')
    @classmethod
    def validate_anchor(cls, value):
        if value is not None and value.year < 1971:
            raise ValueError('History anchor must be on or after 1971')
        return value


async def first_source_time(symbol: str, source: str, timeframe: str | None, end: datetime):
    table, column = ("ticks", "time_msc") if source == "ticks" else ("candles", "time")
    condition = "" if source == "ticks" else "AND timeframe=:timeframe"
    # Indexed edge, no COUNT/SUM over the full hypertable. Shares heavy-read
    # admission, never the trading/fast-quote pool. No native MT5 calls.
    try:
        async with asyncio.timeout(4), heavy_read_session() as session:
            await session.execute(text("SET LOCAL statement_timeout='3s'"))
            return await session.scalar(text(
                f"SELECT {column} FROM {table} WHERE symbol=:symbol {condition} "
                f"AND {column} <= :end ORDER BY {column} ASC LIMIT 1"
            ), {"symbol": symbol, "timeframe": timeframe, "end": end})
    except TimeoutError as exc:
        raise HeavyReadTimeout('History planning timed out') from exc


def bounded_window(body: ChartHistoryRequest, first: datetime | None, now: datetime, retention: int):
    parsed = parse_custom_timeframe(body.timeframe)
    anchor = body.anchor.astimezone(UTC) if body.anchor and body.anchor.tzinfo else body.anchor
    if anchor and anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=UTC)
    end = min(anchor or now, now).replace(second=0, microsecond=0)
    missing = max(1, body.required_bars - body.loaded_bars + 2)
    limited = False
    if parsed.is_tick_bar:
        # Tick count is not elapsed time. Extend one bounded week before the
        # earliest available tick; re-evaluate bars before proposing more.
        if first and first < end:
            end = first
        start = end - timedelta(days=7)
        cutoff = now - timedelta(days=retention) + timedelta(minutes=1)
        if start < cutoff:
            start, limited = cutoff, True
    else:
        # Calendar slack accounts for closures, without assuming exact hours.
        # Clamp numeric offsets BEFORE datetime arithmetic (untrusted large
        # custom periods must never overflow timedelta or datetime).
        cap = 365 * 86400
        seconds = (body.required_bars + 2) * parsed.seconds * 2
        if first and first < end:
            seconds = max(seconds, (end - first).total_seconds() + missing * parsed.seconds * 2)
        limited = seconds > cap
        start = end - timedelta(seconds=min(seconds, cap))
    if start >= end:
        return None
    return start, end, limited


async def chart_history_plan(body: ChartHistoryRequest) -> dict:
    from fastapi import HTTPException
    parsed = parse_custom_timeframe(body.timeframe)
    if parsed.is_tick_bar and parsed.tick_count > 100000:
        raise HTTPException(422, detail={"code": "history_plan_limit"})
    source = "ticks" if parsed.is_tick_bar else "candles"
    source_tf = custom_timeframe_source(parsed)
    now = datetime.now(UTC)
    anchor = body.anchor
    if anchor and anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=UTC)
    end = min(anchor or now, now)
    first = await first_source_time(body.symbol, source, source_tf, end)
    window = bounded_window(body, first, now, await get_retention_days() if parsed.is_tick_bar else 365)
    if window is None:
        raise HTTPException(409, detail={"code": "history_retention_limit"})
    start, end, limited = window
    return {
        "symbol": body.symbol, "timeframe": body.timeframe,
        "target_type": "candles" if body.timeframe in {tf.value for tf in Timeframe} else "custom",
        "source_type": source, "source_timeframe": source_tf,
        "required_bars": body.required_bars, "loaded_bars": body.loaded_bars,
        "from": start.isoformat(), "to": end.isoformat(), "mode": "fill_missing",
        "bounded": True, "limited": limited, "estimated": True,
        "max_window_days": 7 if parsed.is_tick_bar else 365,
    }

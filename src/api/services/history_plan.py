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

# Negative evidence expires: a broker/terminal can make older data available
# later. This is a repeat guard, never an authoritative contract inception date.
AVAILABILITY_RECHECK_HOURS = 6


async def recent_history_attempts(symbol: str, now: datetime) -> list[dict]:
    async with asyncio.timeout(2), heavy_read_session() as session:
        await session.execute(text("SET LOCAL statement_timeout='1s'"))
        result = await session.execute(text("""
            SELECT source_type, source_timeframe, mode, status, range_from, range_to,
                   covered_to, rows_written, started_at, finished_at
            FROM backfill_jobs WHERE symbol=:symbol AND created_at >= :cutoff
            ORDER BY created_at DESC LIMIT 20
        """), {"symbol": symbol, "cutoff": now - timedelta(hours=AVAILABILITY_RECHECK_HOURS)})
        return [dict(row._mapping) for row in result]


def no_extension_attempt(attempts: list[dict], source_tf: str, first: datetime,
                         start: datetime, now: datetime) -> dict | None:
    period = timedelta(seconds=Timeframe(source_tf).seconds)
    for attempt in attempts:
        if attempt['source_type'] != 'candles' or attempt['source_timeframe'] != source_tf:
            continue
        # A newer successful extension invalidates an earlier empty result.
        if attempt['status'] in ('succeeded', 'partial') and attempt['rows_written'] > 0:
            return None
        finished = attempt['finished_at']
        if (attempt['mode'] == 'fill_missing' and attempt['status'] in ('succeeded', 'partial')
                and attempt['rows_written'] == 0 and attempt['started_at'] and finished
                and now - timedelta(hours=AVAILABILITY_RECHECK_HOURS) < finished <= now
                and attempt['range_from'] <= start and attempt['range_from'] < first - period
                and attempt['range_to'] >= first + period
                and attempt['covered_to'] and attempt['covered_to'] >= first + period):
            return attempt
    return None


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
    availability = {
        'status': 'unknown', 'first_source_at': first.isoformat() if first else None,
        'contract_start_confirmed': False, 'checked_at': now.isoformat(),
        'last_attempt_at': None, 'retry_after': None,
    }
    if source == 'candles' and first and body.loaded_bars < body.required_bars:
        try:
            attempt = no_extension_attempt(await recent_history_attempts(body.symbol, now), source_tf, first, start, now)
        except TimeoutError as exc:
            raise HeavyReadTimeout('History availability check timed out') from exc
        if attempt:
            availability.update({
                'status': 'no_additional_history',
                'last_attempt_at': attempt['finished_at'].isoformat(),
                'retry_after': (attempt['finished_at'] + timedelta(hours=AVAILABILITY_RECHECK_HOURS)).isoformat(),
            })
    return {
        "symbol": body.symbol, "timeframe": body.timeframe,
        "target_type": "candles" if body.timeframe in {tf.value for tf in Timeframe} else "custom",
        "source_type": source, "source_timeframe": source_tf,
        "required_bars": body.required_bars, "loaded_bars": body.loaded_bars,
        "from": start.isoformat(), "to": end.isoformat(), "mode": "fill_missing",
        "bounded": True, "limited": limited, "estimated": True,
        "max_window_days": 7 if parsed.is_tick_bar else 365,
        "availability": availability,
    }

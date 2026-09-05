"""Freeze history ranges before the live collector advances sync_state.

Jobs are committed atomically. They retain the original start even if live
watermarks advance afterwards; max timestamp is not a coverage proof.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import text

from src.db.engine import get_engine


def plan_ranges(symbols, settings, states, cutoff, reconnect_from=None):
    jobs = []
    previous = {(r["symbol"], r["data_type"]): r["last_synced_at"] for r in states}
    for symbol in sorted(set(symbols)):
        for tf in settings.timeframes:
            last = previous.get((symbol, tf.value))
            start = (reconnect_from if reconnect_from is not None else last)
            start = start or cutoff - timedelta(days=settings.backfill_days)
            # Refresh settled overlap as well as absent timestamps.
            start = min(start, cutoff - timedelta(hours=settings.candle_settlement_refresh_hours))
            end = datetime.fromtimestamp(
                int(cutoff.timestamp()) // tf.seconds * tf.seconds - tf.seconds, UTC)
            if start <= end:
                jobs.append((symbol, "candles", tf.value, "refresh", start, end))
        start = reconnect_from or previous.get((symbol, "tick"))
        start = start or cutoff - timedelta(hours=4)
        # Retain the whole known gap; the history reader chunks the request.
        # Four hours is only the bootstrap when no cursor exists, not a clamp
        # that silently discards an older known outage.
        if start < cutoff:
            jobs.append((symbol, "ticks", None, "fill_missing", start, cutoff))
    return jobs


async def freeze_ranges(symbols, settings, reconnect_from=None):
    async with asyncio.timeout(15):
        async with get_engine().begin() as conn:
            states = (await conn.execute(text(
                "SELECT symbol,data_type,last_synced_at FROM sync_state"))).mappings().all()
            cutoff = datetime.now(UTC)
            values = [dict(id=uuid4().hex, symbol=s, kind=k, tf=tf, mode=m, start=a, end=b)
                      for s, k, tf, m, a, b in plan_ranges(
                          symbols, settings, states, cutoff, reconnect_from)]
            if values:
                await conn.execute(text("""
                    INSERT INTO backfill_jobs
                      (id,symbol,target_type,timeframe,source_type,source_timeframe,
                       mode,range_from,range_to,status,requested_by)
                    SELECT :id,:symbol,:kind,:tf,:kind,:tf,:mode,:start,:end,'queued','history:auto'
                    WHERE NOT EXISTS (
                      SELECT 1 FROM backfill_jobs WHERE symbol=:symbol
                        AND source_type=:kind AND source_timeframe IS NOT DISTINCT FROM :tf
                        AND mode=:mode AND status IN ('queued','running')
                        AND range_from<=:start AND range_to>=:end)
                """), values)
    return cutoff


class RemoteHistory:
    """Live-side durable submission only. This class cannot call MT5."""

    def __init__(self, settings):
        self.settings = settings
        self.symbols = []
        self.last_seen = None
        self.pending_reconnect = False

    def update_symbols(self, symbols):
        self.symbols = list(symbols)

    def mark_live(self, at):
        if not self.pending_reconnect:
            self.last_seen = datetime.fromtimestamp(at, UTC)

    async def run_initial_backfill(self):
        self.last_seen = await freeze_ranges(self.symbols, self.settings)

    async def run_reconnect_backfill(self):
        self.pending_reconnect = True
        self.last_seen = await freeze_ranges(self.symbols, self.settings, self.last_seen)
        self.pending_reconnect = False

    async def start_scheduled_gap_scan(self):
        # The isolated worker owns scheduling. No local MT5 fallback.
        await asyncio.Event().wait()


class RemoteHistoryListener:
    async def run_forever(self):
        await asyncio.Event().wait()

    async def close(self):
        pass

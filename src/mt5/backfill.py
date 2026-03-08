"""
Historical data backfill and gap detection.

On startup the backfill module:
1. Reads ``sync_state`` to find the last-synced time per (symbol, data_type).
2. Downloads missing candles / ticks from MT5 for the gap period.
3. Periodically runs a scheduled gap scan (every ``GAP_SCAN_INTERVAL_MIN``)
   to detect and fill any holes that might have been missed.

Market hours awareness:
- Forex: Sunday 22:00 UTC → Friday 22:00 UTC (no weekends).
- Crypto: 24/7 (no gaps expected on weekends).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import structlog

from src.config import Settings, Timeframe, get_settings
from src.db import repository as repo
from src.mt5.connection import MT5Connection, run_in_mt5

logger = structlog.get_logger(__name__)

# Maximum bars MT5 returns per single call
_MAX_BARS_PER_CALL = 50_000
_MAX_TICKS_PER_CALL = 100_000


def is_forex_market_open(dt: datetime) -> bool:
    """Return True if *dt* falls within forex market hours.

    Forex market: Sunday 22:00 UTC → Friday 22:00 UTC.
    Weekday 0 = Monday.
    """
    wd = dt.weekday()
    # Saturday (5) is always closed
    if wd == 5:
        return False
    # Sunday (6): open only after 22:00 UTC
    if wd == 6:
        return dt.hour >= 22
    # Friday (4): open only before 22:00 UTC
    if wd == 4:
        return dt.hour < 22
    # Mon-Thu: always open
    return True


class Backfiller:
    """Downloads historical data from MT5 to fill gaps in the database."""

    def __init__(
        self,
        connection: MT5Connection,
        settings: Settings | None = None,
    ) -> None:
        self._conn = connection
        self._settings = settings or get_settings()

    # ------------------------------------------------------------------
    # Initial backfill (called on startup)
    # ------------------------------------------------------------------

    async def run_initial_backfill(self) -> None:
        """Backfill all symbols × timeframes from last sync point."""
        logger.info("backfill_start", backfill_days=self._settings.backfill_days)

        now = datetime.now(timezone.utc)
        default_start = now - timedelta(days=self._settings.backfill_days)

        for symbol in self._settings.symbols:
            # --- Candle backfill ---
            for tf in self._settings.timeframes:
                await self._backfill_candles(symbol, tf, default_start, now)

            # --- Tick backfill ---
            await self._backfill_ticks(symbol, default_start, now)

        logger.info("backfill_complete")

    # ------------------------------------------------------------------
    # Scheduled gap scan
    # ------------------------------------------------------------------

    async def run_gap_scan(self) -> None:
        """
        Detect and repair candle gaps.

        Called periodically by the poller main loop.
        """
        logger.info("gap_scan_start")
        now = datetime.now(timezone.utc)

        for symbol in self._settings.symbols:
            for tf in self._settings.timeframes:
                state = await repo.get_sync_state(symbol, tf.value)
                if state is None:
                    continue

                from_dt = state["last_synced_at"] - timedelta(hours=2)
                gaps = await repo.find_candle_gaps(
                    symbol, tf.value, from_dt, now, tf.seconds
                )

                # Filter out non-market-hours gaps (forex)
                market_gaps = [
                    g for g in gaps if is_forex_market_open(g)
                ]

                if not market_gaps:
                    continue

                logger.warning(
                    "gaps_detected",
                    symbol=symbol,
                    timeframe=tf.value,
                    count=len(market_gaps),
                    first=str(market_gaps[0]),
                    last=str(market_gaps[-1]),
                )

                # Re-download the range that contains gaps
                range_start = market_gaps[0]
                range_end = market_gaps[-1] + timedelta(seconds=tf.seconds)
                await self._backfill_candles(symbol, tf, range_start, range_end)

        logger.info("gap_scan_complete")

    async def start_scheduled_gap_scan(self) -> None:
        """Run gap scan in an infinite loop at the configured interval."""
        interval = self._settings.gap_scan_interval_min * 60
        while True:
            await asyncio.sleep(interval)
            try:
                await self.run_gap_scan()
            except Exception:
                logger.exception("gap_scan_error")

    # ------------------------------------------------------------------
    # On-demand backfill (requested by API via Redis)
    # ------------------------------------------------------------------

    async def on_demand_candles(
        self,
        symbol: str,
        timeframe: str,
        dt_from: datetime,
        dt_to: datetime,
    ) -> int:
        """
        Download candles for an explicit range, **ignoring sync_state**.

        Returns the number of rows inserted/updated.
        """
        try:
            tf = Timeframe(timeframe)
        except ValueError:
            logger.warning("on_demand_invalid_tf", timeframe=timeframe)
            return 0

        logger.info(
            "on_demand_candles_start",
            symbol=symbol,
            timeframe=timeframe,
            range_from=str(dt_from),
            range_to=str(dt_to),
        )

        total = 0
        cursor = dt_from
        while cursor < dt_to:
            bars = await run_in_mt5(
                self._copy_rates_range, symbol, tf.mt5_constant, cursor, dt_to,
            )
            if bars is None or len(bars) == 0:
                break
            rows = self._bars_to_dicts(bars, symbol, tf.value)
            await repo.upsert_candles(rows)
            total += len(rows)
            cursor = rows[-1]["time"] + timedelta(seconds=tf.seconds)
            if len(bars) < _MAX_BARS_PER_CALL:
                break

        logger.info("on_demand_candles_done", symbol=symbol, timeframe=timeframe, rows=total)
        return total

    async def on_demand_ticks(
        self,
        symbol: str,
        dt_from: datetime,
        dt_to: datetime,
    ) -> int:
        """
        Download ticks for an explicit range, **ignoring sync_state**.

        Returns the number of rows inserted.
        """
        logger.info(
            "on_demand_ticks_start",
            symbol=symbol,
            range_from=str(dt_from),
            range_to=str(dt_to),
        )

        total = 0
        cursor = dt_from
        while cursor < dt_to:
            ticks = await run_in_mt5(
                self._copy_ticks_range, symbol, cursor, dt_to,
            )
            if ticks is None or len(ticks) == 0:
                break
            rows = self._ticks_to_dicts(ticks, symbol)
            inserted = await repo.insert_ticks(rows)
            total += inserted
            last_msc = int(ticks[-1]["time_msc"])
            cursor = datetime.fromtimestamp(last_msc / 1000.0, tz=timezone.utc) + timedelta(milliseconds=1)
            if len(ticks) < _MAX_TICKS_PER_CALL:
                break

        logger.info("on_demand_ticks_done", symbol=symbol, rows=total)
        return total

    # ------------------------------------------------------------------
    # Internal: candle backfill
    # ------------------------------------------------------------------

    async def _backfill_candles(
        self,
        symbol: str,
        tf: Timeframe,
        dt_from: datetime,
        dt_to: datetime,
    ) -> None:
        state = await repo.get_sync_state(symbol, tf.value)
        if state and state["last_synced_at"] > dt_from:
            dt_from = state["last_synced_at"]

        if dt_from >= dt_to:
            return

        logger.info(
            "backfill_candles",
            symbol=symbol,
            timeframe=tf.value,
            range_from=str(dt_from),
            range_to=str(dt_to),
        )

        total_inserted = 0
        cursor = dt_from

        while cursor < dt_to:
            bars = await run_in_mt5(
                self._copy_rates_range, symbol, tf.mt5_constant, cursor, dt_to
            )
            if bars is None or len(bars) == 0:
                break

            rows = self._bars_to_dicts(bars, symbol, tf.value)
            await repo.upsert_candles(rows)
            total_inserted += len(rows)

            last_bar_time = rows[-1]["time"]
            await repo.update_sync_state(symbol, tf.value, last_bar_time)

            # Advance cursor past last bar
            cursor = last_bar_time + timedelta(seconds=tf.seconds)

            # Safeguard: if MT5 returned fewer bars than the max, we're done
            if len(bars) < _MAX_BARS_PER_CALL:
                break

        logger.info(
            "backfill_candles_done",
            symbol=symbol,
            timeframe=tf.value,
            rows=total_inserted,
        )

    # ------------------------------------------------------------------
    # Internal: tick backfill
    # ------------------------------------------------------------------

    async def _backfill_ticks(
        self,
        symbol: str,
        dt_from: datetime,
        dt_to: datetime,
    ) -> None:
        state = await repo.get_sync_state(symbol, "tick")
        if state and state["last_synced_at"] > dt_from:
            dt_from = state["last_synced_at"]

        if dt_from >= dt_to:
            return

        logger.info(
            "backfill_ticks",
            symbol=symbol,
            range_from=str(dt_from),
            range_to=str(dt_to),
        )

        total_inserted = 0
        cursor = dt_from

        while cursor < dt_to:
            ticks = await run_in_mt5(
                self._copy_ticks_range, symbol, cursor, dt_to
            )
            if ticks is None or len(ticks) == 0:
                break

            rows = self._ticks_to_dicts(ticks, symbol)
            inserted = await repo.insert_ticks(rows)
            total_inserted += inserted

            # Advance cursor past last tick
            last_msc = int(ticks[-1]["time_msc"])
            last_dt = datetime.fromtimestamp(last_msc / 1000.0, tz=timezone.utc)
            await repo.update_sync_state(symbol, "tick", last_dt, last_msc)

            cursor = last_dt + timedelta(milliseconds=1)

            if len(ticks) < _MAX_TICKS_PER_CALL:
                break

        logger.info(
            "backfill_ticks_done",
            symbol=symbol,
            rows=total_inserted,
        )

    # ------------------------------------------------------------------
    # MT5 calls (executed in single-threaded executor)
    # ------------------------------------------------------------------

    @staticmethod
    def _copy_rates_range(symbol: str, tf_const: int, dt_from: datetime, dt_to: datetime):
        import MetaTrader5 as mt5
        return mt5.copy_rates_range(symbol, tf_const, dt_from, dt_to)

    @staticmethod
    def _copy_ticks_range(symbol: str, dt_from: datetime, dt_to: datetime):
        import MetaTrader5 as mt5
        return mt5.copy_ticks_range(symbol, dt_from, dt_to, mt5.COPY_TICKS_ALL)

    # ------------------------------------------------------------------
    # Converters
    # ------------------------------------------------------------------

    @staticmethod
    def _bars_to_dicts(bars: np.ndarray, symbol: str, timeframe: str) -> list[dict[str, Any]]:
        result = []
        for bar in bars:
            result.append({
                "time": datetime.fromtimestamp(int(bar["time"]), tz=timezone.utc),
                "symbol": symbol,
                "timeframe": timeframe,
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "close": float(bar["close"]),
                "tick_volume": int(bar["tick_volume"]),
                "real_volume": int(bar["real_volume"]),
                "spread": int(bar["spread"]),
            })
        return result

    @staticmethod
    def _ticks_to_dicts(ticks: np.ndarray, symbol: str) -> list[dict[str, Any]]:
        result = []
        for t in ticks:
            msc = int(t["time_msc"])
            result.append({
                "time_msc": datetime.fromtimestamp(msc / 1000.0, tz=timezone.utc),
                "symbol": symbol,
                "bid": float(t["bid"]),
                "ask": float(t["ask"]),
                "last": float(t["last"]),
                "volume": int(t["volume"]),
                "flags": int(t["flags"]),
            })
        return result

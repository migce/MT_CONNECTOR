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
from typing import Any, Awaitable, Callable

import structlog

from src.config import Settings, Timeframe, get_settings
from src.db import repository as repo
from src.metrics import PollerMetrics
from src.mt5.connection import MT5Connection, get_digits, run_in_mt5
from src.mt5.converters import bars_to_dicts, ticks_to_dicts

logger = structlog.get_logger(__name__)

# Maximum bars MT5 returns per single call
_MAX_BARS_PER_CALL = 50_000
_MAX_TICKS_PER_CALL = 100_000
_TICK_PROGRESS_CHUNK = timedelta(hours=4)


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
        self._metrics = PollerMetrics()
        # Active symbols — starts with config, updated dynamically
        self._active_symbols: list[str] = list(self._settings.symbols)

    def update_symbols(self, symbols: list[str]) -> None:
        """Replace the active symbol set used by gap scan / reconnect."""
        self._active_symbols = list(symbols)

    async def _persist_candle_batches(self, rows: list[dict[str, Any]]) -> int:
        """Commit historical candles in bounded, independently retryable batches.

        A large MT5 response must never become one unbounded PostgreSQL
        transaction. Candle UPSERTs are idempotent, so completed batches remain
        valid if a later batch times out or the request is cancelled.
        """
        batch_rows = self._settings.backfill_candle_batch_rows
        affected = 0
        for offset in range(0, len(rows), batch_rows):
            batch = rows[offset : offset + batch_rows]
            affected += await repo.upsert_candles(batch)
            # Give task cancellation a deterministic checkpoint between commits.
            await asyncio.sleep(0)
        return affected

    async def _persist_missing_candle_batches(self, rows: list[dict[str, Any]]) -> int:
        batch_rows = self._settings.backfill_candle_batch_rows
        affected = 0
        for offset in range(0, len(rows), batch_rows):
            affected += await repo.insert_candles(rows[offset : offset + batch_rows])
            await asyncio.sleep(0)
        return affected

    @staticmethod
    def _last_completed_open(reference: datetime, timeframe: Timeframe) -> datetime:
        """Return the open timestamp of the last fully closed native bar."""
        reference = reference.astimezone(timezone.utc)
        epoch_seconds = int(reference.timestamp())
        current_open = epoch_seconds - epoch_seconds % timeframe.seconds
        return datetime.fromtimestamp(
            current_open - timeframe.seconds,
            tz=timezone.utc,
        )

    @staticmethod
    def _closed_tick_repair_range(
        dt_from: datetime,
        dt_to: datetime,
        timeframe: Timeframe,
    ) -> tuple[datetime, datetime] | None:
        """Align an arbitrary request to fully observable candle buckets."""
        interval = timeframe.seconds
        start_seconds = int(dt_from.astimezone(timezone.utc).timestamp())
        end_seconds = int(dt_to.astimezone(timezone.utc).timestamp())
        start_remainder = start_seconds % interval
        if start_remainder:
            start_seconds += interval - start_remainder
        end_seconds -= end_seconds % interval
        if start_seconds >= end_seconds:
            return None
        return (
            datetime.fromtimestamp(start_seconds, tz=timezone.utc),
            datetime.fromtimestamp(end_seconds, tz=timezone.utc),
        )

    async def refresh_settled_candles(
        self,
        symbol: str,
        dt_from: datetime,
        dt_to: datetime,
    ) -> dict[str, int]:
        """Force-refresh completed native bars from base to higher timeframes.

        Existing timestamps are deliberately reread and UPSERTed. This closes
        the gap left by absence-only scanners when a disconnect persisted a
        completed candle before its final OHLCV revision arrived.
        """
        ordered = sorted(set(self._settings.timeframes), key=lambda item: item.seconds)
        rows_by_timeframe: dict[str, int] = {}
        logger.info(
            "settled_candle_refresh_start",
            symbol=symbol,
            range_from=str(dt_from),
            range_to=str(dt_to),
            timeframes=[timeframe.value for timeframe in ordered],
        )
        for timeframe in ordered:
            completed_to = min(
                dt_to.astimezone(timezone.utc),
                self._last_completed_open(dt_to, timeframe),
            )
            if dt_from > completed_to:
                rows_by_timeframe[timeframe.value] = 0
                continue
            self._metrics.set_backfill_phase(
                "settlement",
                f"{symbol} {timeframe.value}",
            )
            rows_by_timeframe[timeframe.value] = await self.on_demand_candles(
                symbol,
                timeframe.value,
                dt_from,
                completed_to,
            )
        logger.info(
            "settled_candle_refresh_done",
            symbol=symbol,
            rows_by_timeframe=rows_by_timeframe,
        )
        return rows_by_timeframe

    # ------------------------------------------------------------------
    # Initial backfill (called on startup)
    # ------------------------------------------------------------------

    async def run_initial_backfill(self) -> None:
        """Backfill all symbols × timeframes from last sync point."""
        logger.info("backfill_start", backfill_days=self._settings.backfill_days)
        self._metrics.set_backfill_phase("initial")

        now = datetime.now(timezone.utc)
        default_start = now - timedelta(days=self._settings.backfill_days)
        settlement_start = max(
            default_start,
            now - timedelta(hours=self._settings.candle_settlement_refresh_hours),
        )

        for symbol in self._settings.symbols:
            # --- Candle backfill ---
            for tf in self._settings.timeframes:
                self._metrics.set_backfill_phase("initial", f"{symbol} {tf.value}")
                await self._backfill_candles(symbol, tf, default_start, now)

            # Existing completed rows may still be partial after a terminal or
            # Poller outage. Refresh the overlap even when no timestamp is absent.
            await self.refresh_settled_candles(symbol, settlement_start, now)

            # --- Tick backfill ---
            self._metrics.set_backfill_phase("initial", f"{symbol} ticks")
            await self._backfill_ticks(symbol, default_start, now)

        self._metrics.set_backfill_phase("")
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
        total_gaps = 0

        for symbol in self._active_symbols:
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
                self._metrics.record_gap_scan(len(market_gaps))
                total_gaps += len(market_gaps)

                # Re-download the range that contains gaps (ignore sync_state)
                range_start = market_gaps[0]
                range_end = market_gaps[-1] + timedelta(seconds=tf.seconds)
                await self.on_demand_candles(symbol, tf.value, range_start, range_end)

        self._metrics.record_gap_scan(total_gaps)
        logger.info("gap_scan_complete")

    async def run_reconnect_backfill(self) -> None:
        """Backfill ALL ticks and candles after a connection gap.

        Uses ``on_demand_*`` methods that **ignore sync_state** so
        that gaps in the middle of an already-synced range are filled.
        The real-time collector may have advanced sync_state past the
        gap, so we cannot rely on ``_backfill_candles``/``_backfill_ticks``
        which skip data before ``last_synced_at``.

        Force-refreshes a bounded completed-candle overlap in ascending native
        timeframe order, including rows whose timestamps already exist.
        For ticks, re-downloads the last ``backfill_days`` range
        (``insert_ticks`` is idempotent via ON CONFLICT).
        """
        logger.info("reconnect_backfill_start")
        self._metrics.set_backfill_phase("reconnect")
        now = datetime.now(timezone.utc)
        default_start = now - timedelta(days=self._settings.backfill_days)
        settlement_start = max(
            default_start,
            now - timedelta(hours=self._settings.candle_settlement_refresh_hours),
        )

        for symbol in self._active_symbols:
            await self.refresh_settled_candles(symbol, settlement_start, now)

            # ── Tick backfill (idempotent) ──
            self._metrics.set_backfill_phase("reconnect", f"{symbol} ticks")
            await self.on_demand_ticks(symbol, default_start, now)

        self._metrics.set_backfill_phase("")
        logger.info("reconnect_backfill_complete")

    async def start_scheduled_gap_scan(self) -> None:
        """Run gap scan in an infinite loop at the configured interval."""
        interval = self._settings.gap_scan_interval_min * 60
        while True:
            await asyncio.sleep(interval)
            try:
                await self.run_gap_scan()
            except Exception:
                logger.exception("gap_scan_error")
                self._metrics.record_error("gap_scan")

    # ------------------------------------------------------------------
    # On-demand backfill (requested by API via Redis)
    # ------------------------------------------------------------------

    async def on_demand_candles(
        self,
        symbol: str,
        timeframe: str,
        dt_from: datetime,
        dt_to: datetime,
        *,
        repair_from_ticks: bool = False,
        preserve_existing: bool = False,
        progress_callback: Callable[[datetime, int], Awaitable[None]] | None = None,
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
            rows = bars_to_dicts(bars, symbol, tf.value)
            affected = (
                await self._persist_missing_candle_batches(rows)
                if preserve_existing else await self._persist_candle_batches(rows)
            )
            total += affected
            cursor = rows[-1]["time"] + timedelta(seconds=tf.seconds)
            if progress_callback is not None:
                await progress_callback(min(cursor, dt_to), total)
            if len(bars) < _MAX_BARS_PER_CALL:
                break

        tick_repair_rows = 0
        if repair_from_ticks:
            repair_range = self._closed_tick_repair_range(dt_from, dt_to, tf)
            if repair_range is not None:
                repair_from, repair_to = repair_range
                # Re-query actual MT5 ticks first. The database may itself have
                # a hole if the whole Connector was offline during the range.
                await self.on_demand_ticks(symbol, repair_from, repair_to)
                tick_repair_rows = await repo.insert_missing_candles_from_ticks(
                    symbol=symbol,
                    timeframe=tf.value,
                    bucket_seconds=tf.seconds,
                    dt_from=repair_from,
                    dt_to=repair_to,
                    spread_scale=10 ** get_digits(symbol),
                )
                if tick_repair_rows:
                    latest = await repo.get_latest_candle_time(symbol, tf.value)
                    if latest is not None:
                        await repo.update_sync_state(symbol, tf.value, latest)
                total += tick_repair_rows

        logger.info(
            "on_demand_candles_done",
            symbol=symbol,
            timeframe=timeframe,
            rows=total,
            tick_repair_rows=tick_repair_rows,
        )
        return total

    async def on_demand_ticks(
        self,
        symbol: str,
        dt_from: datetime,
        dt_to: datetime,
        *,
        refresh_existing: bool = False,
        progress_callback: Callable[[datetime, int], Awaitable[None]] | None = None,
        scan_progress_callback: Callable[[datetime, int], Awaitable[None]] | None = None,
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
        rows_read = 0
        cursor = dt_from
        while cursor < dt_to:
            chunk_to = min(cursor + _TICK_PROGRESS_CHUNK, dt_to)
            try:
                ticks = await asyncio.wait_for(
                    run_in_mt5(self._copy_ticks_range, symbol, cursor, chunk_to),
                    timeout=self._TICKS_IPC_TIMEOUT,
                )
            except TimeoutError as exc:
                raise TimeoutError(
                    f"MT5 tick history chunk timed out after "
                    f"{self._TICKS_IPC_TIMEOUT} seconds"
                ) from exc
            if ticks is None or len(ticks) == 0:
                cursor = chunk_to
                if scan_progress_callback is not None:
                    await scan_progress_callback(cursor, rows_read)
                continue
            rows = ticks_to_dicts(ticks, symbol)
            rows_read += len(rows)
            inserted = (
                await repo.upsert_ticks(rows)
                if refresh_existing else await repo.insert_ticks(rows)
            )
            total += inserted
            last_msc = int(ticks[-1]["time_msc"])
            next_cursor = datetime.fromtimestamp(
                last_msc / 1000.0,
                tz=timezone.utc,
            ) + timedelta(milliseconds=1)
            if next_cursor <= cursor:
                next_cursor = cursor + timedelta(milliseconds=1)
            if progress_callback is not None:
                await progress_callback(min(next_cursor, dt_to), rows_read)
            cursor = (
                chunk_to
                if len(ticks) < _MAX_TICKS_PER_CALL
                else min(next_cursor, chunk_to)
            )
            if scan_progress_callback is not None:
                await scan_progress_callback(cursor, rows_read)

        logger.info(
            "on_demand_ticks_done",
            symbol=symbol,
            rows_read=rows_read,
            rows_written=total,
        )
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

            rows = bars_to_dicts(bars, symbol, tf.value)
            await self._persist_candle_batches(rows)
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

    # Maximum seconds to wait for a single copy_ticks_range IPC call.
    # MT5 can hang indefinitely on large ranges; cap it to keep startup unblocked.
    _TICKS_IPC_TIMEOUT = 20
    # On initial backfill cap tick history to this many hours so we never
    # request a multi-day range that causes IPC hangs.
    _TICKS_BACKFILL_MAX_HOURS = 4

    async def _backfill_ticks(
        self,
        symbol: str,
        dt_from: datetime,
        dt_to: datetime,
    ) -> None:
        state = await repo.get_sync_state(symbol, "tick")
        if state and state["last_synced_at"] > dt_from:
            dt_from = state["last_synced_at"]

        # Cap the lookback so a single copy_ticks_range call is never
        # given a huge range (which causes IPC hangs on Windows).
        min_from = dt_to - timedelta(hours=self._TICKS_BACKFILL_MAX_HOURS)
        if dt_from < min_from:
            logger.info(
                "backfill_ticks_range_capped",
                symbol=symbol,
                original_from=str(dt_from),
                capped_from=str(min_from),
            )
            dt_from = min_from

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
            try:
                ticks = await asyncio.wait_for(
                    run_in_mt5(self._copy_ticks_range, symbol, cursor, dt_to),
                    timeout=self._TICKS_IPC_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "backfill_ticks_ipc_timeout",
                    symbol=symbol,
                    cursor=str(cursor),
                    timeout_sec=self._TICKS_IPC_TIMEOUT,
                )
                break

            if ticks is None or len(ticks) == 0:
                break

            rows = ticks_to_dicts(ticks, symbol)
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

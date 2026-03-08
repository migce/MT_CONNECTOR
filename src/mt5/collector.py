"""
Real-time tick & candle collector.

Runs two async loops:
1. **Tick loop** — polls ``mt5.symbol_info_tick()`` every N ms, buffers
   new ticks and periodically flushes them to the database and Redis.
2. **Candle loop** — polls ``mt5.copy_rates_from_pos()`` every N sec
   for each (symbol, timeframe) pair, UPSERTs into the database and
   publishes updates to Redis.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import numpy as np
import structlog

from src.config import Settings, Timeframe, get_settings
from src.db import repository as repo
from src.metrics import PollerMetrics
from src.mt5.connection import MT5Connection, run_in_mt5
from src.mt5.converters import bars_to_dicts
from src.redis_bus.publisher import RedisPublisher

logger = structlog.get_logger(__name__)

# Max ticks to buffer before forced flush
_TICK_BUFFER_MAX = 5000
# Flush interval (seconds)
_TICK_FLUSH_INTERVAL = 1.0


class Collector:
    """
    Orchestrates real-time data collection from MT5.

    Lifecycle::

        collector = Collector(connection, publisher)
        await collector.start()   # spawns background tasks
        …
        await collector.stop()    # graceful teardown
    """

    def __init__(
        self,
        connection: MT5Connection,
        publisher: RedisPublisher,
        settings: Settings | None = None,
    ) -> None:
        self._conn = connection
        self._pub = publisher
        self._settings = settings or get_settings()
        self._metrics = PollerMetrics()

        # Per-symbol last-seen tick timestamp (ms)
        self._last_tick_msc: dict[str, int] = defaultdict(int)

        # Tick write buffer: list of dicts ready for DB insert
        self._tick_buffer: deque[dict[str, Any]] = deque(maxlen=100_000)
        self._last_flush_ts: float = 0.0

        # Background tasks
        self._tasks: list[asyncio.Task] = []
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._tick_loop(), name="tick_loop"),
            asyncio.create_task(self._candle_loop(), name="candle_loop"),
            asyncio.create_task(self._flush_loop(), name="flush_loop"),
        ]
        logger.info("collector_started",
                     symbols=self._settings.symbols,
                     timeframes=[tf.value for tf in self._settings.timeframes])

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        # Final flush
        await self._flush_tick_buffer()
        logger.info("collector_stopped")

    # ------------------------------------------------------------------
    # Tick polling loop
    # ------------------------------------------------------------------

    async def _tick_loop(self) -> None:
        interval = self._settings.tick_poll_interval_ms / 1000.0
        symbols = self._settings.symbols

        while self._running:
            try:
                for symbol in symbols:
                    tick = await run_in_mt5(self._get_tick, symbol)
                    if tick is None:
                        continue
                    tick_msc = int(tick.time_msc)
                    if tick_msc <= self._last_tick_msc[symbol]:
                        continue  # duplicate

                    self._last_tick_msc[symbol] = tick_msc

                    tick_dict = {
                        "time_msc": datetime.fromtimestamp(
                            tick_msc / 1000.0, tz=timezone.utc
                        ),
                        "symbol": symbol,
                        "bid": float(tick.bid),
                        "ask": float(tick.ask),
                        "last": float(tick.last),
                        "volume": int(tick.volume),
                        "flags": int(tick.flags),
                    }
                    self._tick_buffer.append(tick_dict)
                    self._metrics.record_tick(symbol, tick_dict["bid"], tick_dict["ask"])

                    # Publish to Redis (fire-and-forget)
                    await self._pub.publish_tick(symbol, tick_dict)

                self._metrics.set_tick_buffer_depth(len(self._tick_buffer))
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("tick_loop_error")
                self._metrics.record_error("tick_loop")
                await asyncio.sleep(1.0)

    # ------------------------------------------------------------------
    # Candle polling loop
    # ------------------------------------------------------------------

    async def _candle_loop(self) -> None:
        interval = self._settings.candle_poll_interval_sec
        symbols = self._settings.symbols
        timeframes = self._settings.timeframes

        while self._running:
            try:
                # Collect all (symbol, tf) pairs then process concurrently
                async def _poll_one(symbol: str, tf):
                    bars = await run_in_mt5(
                        self._get_rates, symbol, tf.mt5_constant, 0, 2
                    )
                    if bars is None or len(bars) == 0:
                        return

                    rows = bars_to_dicts(bars, symbol, tf.value)
                    await repo.upsert_candles(rows)
                    self._metrics.record_candle_upsert(len(rows))
                    await self._pub.publish_candle(symbol, tf.value, rows[-1])
                    last_time = rows[-1]["time"]
                    await repo.update_sync_state(symbol, tf.value, last_time)

                tasks = [
                    _poll_one(symbol, tf)
                    for symbol in symbols
                    for tf in timeframes
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("candle_loop_error")
                self._metrics.record_error("candle_loop")
                await asyncio.sleep(2.0)

    # ------------------------------------------------------------------
    # Flush loop — periodic DB write of buffered ticks
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(_TICK_FLUSH_INTERVAL)
                await self._flush_tick_buffer()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("flush_loop_error")
                self._metrics.record_error("flush")

    async def _flush_tick_buffer(self) -> None:
        if not self._tick_buffer:
            return

        # Drain buffer atomically
        batch: list[dict[str, Any]] = []
        while self._tick_buffer:
            batch.append(self._tick_buffer.popleft())

        self._metrics.set_tick_buffer_depth(0)
        _t0 = time.monotonic()
        try:
            inserted = await repo.insert_ticks(batch)
        except Exception:
            # Re-enqueue so ticks are not lost on transient DB errors
            self._tick_buffer.extendleft(reversed(batch))
            self._metrics.set_tick_buffer_depth(len(self._tick_buffer))
            logger.exception("tick_flush_db_error", lost=0, requeued=len(batch))
            raise
        _elapsed_ms = (time.monotonic() - _t0) * 1000
        self._metrics.record_ticks_flushed(inserted, _elapsed_ms)
        logger.debug("ticks_flushed", count=inserted, buffered=len(batch))

        # Update sync state for tick data
        # Group by symbol, track the latest tick_msc per symbol
        latest_by_symbol: dict[str, datetime] = {}
        latest_msc_by_symbol: dict[str, int] = {}
        for row in batch:
            sym = row["symbol"]
            row_msc = int(row["time_msc"].timestamp() * 1000)
            if sym not in latest_msc_by_symbol or row_msc > latest_msc_by_symbol[sym]:
                latest_by_symbol[sym] = row["time_msc"]
                latest_msc_by_symbol[sym] = row_msc

        for sym, ts in latest_by_symbol.items():
            await repo.update_sync_state(
                sym, "tick", ts, latest_msc_by_symbol[sym]
            )

    # ------------------------------------------------------------------
    # MT5 calls (run in single-threaded executor)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_tick(symbol: str):
        import MetaTrader5 as mt5
        return mt5.symbol_info_tick(symbol)

    @staticmethod
    def _get_rates(symbol: str, tf_const: int, start_pos: int, count: int):
        import MetaTrader5 as mt5
        return mt5.copy_rates_from_pos(symbol, tf_const, start_pos, count)

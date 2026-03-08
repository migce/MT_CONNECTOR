"""
MT5 Poller — main entry point.

This is the Windows-native process that:
1. Connects to the MetaTrader 5 terminal.
2. Runs initial backfill for all configured symbols/timeframes.
3. Launches real-time tick & candle collectors.
4. Periodically checks MT5 connectivity (heartbeat) and backfills gaps.
5. Runs a scheduled gap-scan job.

Usage::

    python -m src.poller_main

The process exits gracefully on SIGINT (Ctrl+C) / SIGTERM.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time

import structlog

from src.config import get_settings
from src.db.engine import dispose_engine, get_engine
from src.db.init_timescale import init_timescaledb
from src.logging_config import setup_logging
from src.mt5.backfill import Backfiller
from src.mt5.collector import Collector
from src.mt5.connection import MT5Connection
from src.redis_bus.backfill_manager import BackfillListener
from src.redis_bus.publisher import RedisPublisher

logger = structlog.get_logger(__name__)


async def _heartbeat_loop(
    connection: MT5Connection,
    backfiller: Backfiller,
    interval_sec: int,
) -> None:
    """Periodically check MT5 liveness; backfill on reconnect."""
    while True:
        try:
            await asyncio.sleep(interval_sec)
            gap_occurred = await connection.ensure_connected()
            if gap_occurred:
                logger.warning("mt5_reconnected_backfilling")
                await backfiller.run_initial_backfill()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("heartbeat_error")
            await asyncio.sleep(5)


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)

    logger.info(
        "poller_starting",
        symbols=settings.symbols,
        timeframes=[tf.value for tf in settings.timeframes],
        backfill_days=settings.backfill_days,
    )

    start_time = time.time()

    # --- Database ---
    get_engine(settings)
    try:
        await init_timescaledb()
    except Exception:
        logger.warning("timescaledb_init_skipped_will_retry", exc_info=True)

    # --- Redis ---
    publisher = RedisPublisher(settings)
    try:
        await publisher.connect()
    except Exception:
        logger.error("redis_connect_failed", exc_info=True)
        sys.exit(1)

    # --- MT5 ---
    connection = MT5Connection(settings)
    await connection.connect()
    await connection.select_symbols(settings.symbols)

    # --- Backfill ---
    backfiller = Backfiller(connection, settings)

    # --- On-demand backfill listener (API → Poller via Redis) ---
    # Start BEFORE initial backfill so API requests are served during startup
    backfill_listener = BackfillListener(backfiller, settings)
    await backfill_listener.connect()
    backfill_listener_task = asyncio.create_task(
        backfill_listener.run_forever(),
        name="backfill_listener",
    )

    await backfiller.run_initial_backfill()

    # --- Collector ---
    collector = Collector(connection, publisher, settings)
    await collector.start()

    # --- Background tasks ---
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            connection, backfiller, settings.mt5_heartbeat_interval_sec
        ),
        name="heartbeat",
    )
    gap_scan_task = asyncio.create_task(
        backfiller.start_scheduled_gap_scan(),
        name="gap_scan",
    )

    logger.info(
        "poller_running",
        startup_sec=round(time.time() - start_time, 2),
    )

    # --- Graceful shutdown ---
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("shutdown_signal_received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals
            signal.signal(sig, lambda s, f: _signal_handler())

    await stop_event.wait()

    # --- Cleanup ---
    logger.info("poller_shutting_down")

    heartbeat_task.cancel()
    gap_scan_task.cancel()
    backfill_listener_task.cancel()
    await asyncio.gather(
        heartbeat_task, gap_scan_task, backfill_listener_task,
        return_exceptions=True,
    )

    await collector.stop()
    await connection.shutdown()
    await publisher.close()
    await backfill_listener.close()
    await dispose_engine()

    logger.info("poller_stopped")


if __name__ == "__main__":
    # Windows + Python 3.12+: ProactorEventLoop has issues with asyncpg.
    # Force SelectorEventLoop for reliable DB connections.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

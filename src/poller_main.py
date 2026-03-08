"""
MT5 Poller — main entry point.

This is the Windows-native process that:
1. Connects to the MetaTrader 5 terminal.
2. Runs initial backfill for all configured symbols/timeframes.
3. Launches real-time tick & candle collectors.
4. Periodically checks MT5 connectivity (heartbeat) and backfills gaps.
5. Runs a scheduled gap-scan job.
6. Optionally shows a live Rich terminal dashboard (``--dashboard``).

Usage::

    python -m src.poller_main
    python -m src.poller_main --dashboard

The process exits gracefully on SIGINT (Ctrl+C) / SIGTERM.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time

import structlog

from src.config import get_settings
from src.db.engine import dispose_engine, get_engine
from src.db.init_timescale import init_timescaledb
from src.logging_config import setup_logging
from src.metrics import PollerMetrics
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
    metrics = PollerMetrics()
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
            metrics.record_error("heartbeat")
            await asyncio.sleep(5)


def _monitor_tasks(tasks: dict[str, asyncio.Task]) -> None:
    """Update PollerMetrics with alive/dead status for each task."""
    metrics = PollerMetrics()
    for name, task in tasks.items():
        metrics.set_task_alive(name, not task.done())


async def _task_monitor_loop(tasks: dict[str, asyncio.Task]) -> None:
    """Background loop that updates task-alive metrics every 2 s."""
    while True:
        try:
            _monitor_tasks(tasks)
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            break


async def main(dashboard: bool = False) -> None:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)

    # When dashboard is active, suppress structlog output to avoid
    # clutter underneath the Rich display.
    if dashboard:
        logging.getLogger().setLevel(logging.CRITICAL)
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL),
        )

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

    # --- Named tasks for monitoring ---
    named_tasks: dict[str, asyncio.Task] = {
        "heartbeat": heartbeat_task,
        "gap_scan": gap_scan_task,
        "backfill_listener": backfill_listener_task,
        # collector tasks are internal but we reference them by name
    }
    # Register collector sub-tasks
    for ct in collector._tasks:
        named_tasks[ct.get_name()] = ct

    # --- Task health monitor ---
    monitor_task = asyncio.create_task(
        _task_monitor_loop(named_tasks),
        name="task_monitor",
    )

    # --- Dashboard (optional) ---
    dashboard_task: asyncio.Task | None = None
    if dashboard:
        from src.dashboard import run_dashboard
        dashboard_task = asyncio.create_task(
            run_dashboard(),
            name="dashboard",
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

    if dashboard_task:
        dashboard_task.cancel()
    monitor_task.cancel()
    heartbeat_task.cancel()
    gap_scan_task.cancel()
    backfill_listener_task.cancel()
    cancel_tasks = [monitor_task, heartbeat_task, gap_scan_task, backfill_listener_task]
    if dashboard_task:
        cancel_tasks.insert(0, dashboard_task)
    await asyncio.gather(
        *cancel_tasks,
        return_exceptions=True,
    )

    await collector.stop()
    await connection.shutdown()
    await publisher.close()
    await backfill_listener.close()
    await dispose_engine()

    logger.info("poller_stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MT5 Poller")
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Show live Rich terminal dashboard instead of JSON logs",
    )
    args = parser.parse_args()

    # Windows + Python 3.12+: ProactorEventLoop has issues with asyncpg.
    # Force SelectorEventLoop for reliable DB connections.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main(dashboard=args.dashboard))
    except KeyboardInterrupt:
        pass

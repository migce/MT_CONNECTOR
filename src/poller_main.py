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
import msvcrt
import os
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
from src.redis_bus.pool import close_redis_pool, get_redis_pool
from src.redis_bus.publisher import RedisPublisher

logger = structlog.get_logger(__name__)


async def _heartbeat_loop(
    connection: MT5Connection,
    backfiller: Backfiller,
    interval_sec: int,
) -> None:
    """Periodically check MT5 liveness; gap-fill on reconnect."""
    metrics = PollerMetrics()
    last_seen = time.time()
    while True:
        try:
            await asyncio.sleep(interval_sec)
            gap_occurred = await connection.ensure_connected()
            now = time.time()
            if gap_occurred:
                gap_sec = now - last_seen
                logger.warning(
                    "mt5_reconnected_gap_backfill",
                    gap_seconds=round(gap_sec, 1),
                )
                # Only run a gap-scan for the period we were disconnected
                # instead of a full initial backfill
                await backfiller.run_gap_scan()
            last_seen = now
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


async def _health_checker_loop(api_port: int) -> None:
    """Poll API /stats + DB/Redis every 10 s and update PollerMetrics."""
    import httpx
    from sqlalchemy import text as sa_text

    from src.db.engine import get_engine as _get_engine

    metrics = PollerMetrics()
    api_url = f"http://127.0.0.1:{api_port}/api/v1/stats"

    while True:
        try:
            # ── API health ──────────────────────────────────────────
            api_ok = False
            api_lat = 0.0
            api_data: dict = {}
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    t0 = time.perf_counter()
                    resp = await client.get(api_url)
                    api_lat = (time.perf_counter() - t0) * 1000
                    if resp.status_code == 200:
                        api_ok = True
                        api_data = resp.json()
            except Exception:
                pass

            metrics.update_api_health(
                healthy=api_ok,
                latency_ms=round(api_lat, 1),
                requests_1h=api_data.get("requests_1h", 0),
                requests_12h=api_data.get("requests_12h", 0),
                requests_24h=api_data.get("requests_24h", 0),
                errors_1h=api_data.get("errors_1h", 0),
                avg_latency_ms=api_data.get("avg_latency_ms_1h", 0.0),
            )

            # ── DB health ───────────────────────────────────────────
            db_ok = False
            db_lat = 0.0
            try:
                engine = _get_engine()
                t0 = time.perf_counter()
                async with engine.connect() as conn:
                    await conn.execute(sa_text("SELECT 1"))
                db_lat = (time.perf_counter() - t0) * 1000
                db_ok = True
            except Exception:
                pass

            # ── Redis health ────────────────────────────────────────
            redis_ok = False
            redis_lat = 0.0
            try:
                pool = await get_redis_pool()
                t0 = time.perf_counter()
                await pool.ping()
                redis_lat = (time.perf_counter() - t0) * 1000
                redis_ok = True
            except Exception:
                pass

            metrics.update_infra_health(
                db_ok=db_ok,
                redis_ok=redis_ok,
                db_latency_ms=round(db_lat, 1),
                redis_latency_ms=round(redis_lat, 1),
            )

            # ── Prune old minute-buckets every cycle ────────────────
            metrics.prune_minute_buckets()

            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("health_checker_error")
            await asyncio.sleep(10.0)


# Path for single-instance lock file
_LOCK_FILE = ".poller.lock"


def _acquire_lock() -> object:
    """Acquire an exclusive file lock. Exit immediately if another instance is running."""
    try:
        fh = open(_LOCK_FILE, "w")  # noqa: SIM115
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        fh.write(str(os.getpid()))
        fh.flush()
        return fh
    except (OSError, PermissionError):
        print(
            "\n  ✗ Another poller instance is already running.\n"
            "    Kill it first or delete .poller.lock\n",
            file=sys.stderr,
        )
        sys.exit(1)


def _release_lock(fh: object) -> None:
    """Release the lock file."""
    try:
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[union-attr]
        fh.close()  # type: ignore[union-attr]
        os.remove(_LOCK_FILE)
    except OSError:
        pass


async def main(dashboard: bool = False) -> None:
    lock_fh = _acquire_lock()

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

    # --- Health checker (API / DB / Redis) ---
    health_checker_task = asyncio.create_task(
        _health_checker_loop(settings.api_port),
        name="health_checker",
    )
    named_tasks["health_checker"] = health_checker_task

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
    health_checker_task.cancel()
    heartbeat_task.cancel()
    gap_scan_task.cancel()
    backfill_listener_task.cancel()
    cancel_tasks = [monitor_task, health_checker_task, heartbeat_task, gap_scan_task, backfill_listener_task]
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
    await close_redis_pool()
    await dispose_engine()

    _release_lock(lock_fh)
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

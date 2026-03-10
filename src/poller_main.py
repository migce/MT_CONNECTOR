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

The process exits gracefully on Ctrl+X, SIGINT (Ctrl+C), or SIGTERM,
flushing pending statistics to the database before shutdown.
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
    """Poll API /health + /stats, DB, Redis every 10 s and update PollerMetrics."""
    import httpx
    from sqlalchemy import text as sa_text

    from src.db.engine import get_engine as _get_engine

    metrics = PollerMetrics()
    api_base = f"http://127.0.0.1:{api_port}/api/v1"

    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                # ── API health (use /health — always present) ───────────
                api_ok = False
                api_lat = 0.0
                try:
                    t0 = time.perf_counter()
                    resp = await client.get(f"{api_base}/health")
                    api_lat = (time.perf_counter() - t0) * 1000
                    if resp.status_code == 200:
                        api_ok = True
                except Exception:
                    pass

                # ── API request metrics (use /stats — may 404 on old image)
                api_data: dict = {}
                if api_ok:
                    try:
                        resp = await client.get(f"{api_base}/stats")
                        if resp.status_code == 200:
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
                db_size_gb = 0.0
                try:
                    engine = _get_engine()
                    t0 = time.perf_counter()
                    async with engine.connect() as conn:
                        await conn.execute(sa_text("SELECT 1"))
                    db_lat = (time.perf_counter() - t0) * 1000
                    db_ok = True
                    # query DB size (non-critical)
                    try:
                        async with engine.connect() as conn:
                            row = await conn.execute(
                                sa_text("SELECT pg_database_size(current_database())")
                            )
                            db_size_gb = row.scalar() / (1024 ** 3)
                    except Exception:
                        pass
                except Exception:
                    pass

                # ── Redis health ────────────────────────────────────────
                redis_ok = False
                redis_lat = 0.0
                try:
                    pool = get_redis_pool()
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
                    db_size_gb=round(db_size_gb, 3),
                )

                # ── Publish poller status to Redis for API /health ─────
                try:
                    pool = get_redis_pool()
                    import orjson as _orjson
                    _status = {
                        "mt5_connected": metrics.mt5_connected,
                        "uptime": metrics.uptime_str(),
                        "ticks_total": metrics.ticks_total,
                        "candles_total": metrics.candles_total,
                    }
                    await pool.set(
                        "poller:status",
                        _orjson.dumps(_status),
                        ex=30,  # expires in 30s — stale = poller down
                    )
                except Exception:
                    pass

                # ── Prune old minute-buckets every cycle ────────────────
                metrics.prune_minute_buckets()

                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("health_checker_error")
                await asyncio.sleep(10.0)


# ---------------------------------------------------------------
# Daily stats persistence — flush poller metrics to DB
# ---------------------------------------------------------------

class _PollerStatsFlusher:
    """Tracks delta between flushes and UPSERT-adds to daily_stats."""

    def __init__(self) -> None:
        # Snapshot current metric values so the first flush only captures
        # *new* activity since the flusher was created (avoids re-adding
        # the baseline that was already restored from DB on startup).
        metrics = PollerMetrics()
        with metrics._lock:
            self._last_ticks = metrics.ticks_total
            self._last_flushed = metrics.ticks_flushed_total
            self._last_candles = metrics.candles_total
            self._last_redis = metrics.redis_pub_count
            self._last_errors = sum(metrics.errors.values())
            self._last_reconnects = metrics.reconnect_count
            self._last_gaps = metrics.gaps_found
        self._last_uptime_mono = time.monotonic()

    async def flush(self) -> None:
        """Compute deltas and write to DB."""
        from src.db import repository as repo

        metrics = PollerMetrics()
        now_mono = time.monotonic()

        with metrics._lock:
            cur_ticks = metrics.ticks_total
            cur_flushed = metrics.ticks_flushed_total
            cur_candles = metrics.candles_total
            cur_redis = metrics.redis_pub_count
            cur_errors = sum(metrics.errors.values())
            cur_reconn = metrics.reconnect_count
            cur_gaps = metrics.gaps_found

        d_ticks = cur_ticks - self._last_ticks
        d_flushed = cur_flushed - self._last_flushed
        d_candles = cur_candles - self._last_candles
        d_redis = cur_redis - self._last_redis
        d_errors = cur_errors - self._last_errors
        d_reconn = cur_reconn - self._last_reconnects
        d_gaps = cur_gaps - self._last_gaps
        d_uptime = now_mono - self._last_uptime_mono

        # Update last-seen values
        self._last_ticks = cur_ticks
        self._last_flushed = cur_flushed
        self._last_candles = cur_candles
        self._last_redis = cur_redis
        self._last_errors = cur_errors
        self._last_reconnects = cur_reconn
        self._last_gaps = cur_gaps
        self._last_uptime_mono = now_mono

        # Skip if nothing changed
        if all(v == 0 for v in (d_ticks, d_flushed, d_candles, d_redis,
                                d_errors, d_reconn, d_gaps)) and d_uptime < 1:
            return

        from datetime import datetime, timezone
        today = datetime.now(timezone.utc)

        try:
            await repo.upsert_daily_poller_stats(
                today,
                ticks_received=d_ticks,
                ticks_flushed=d_flushed,
                candles_upserted=d_candles,
                redis_published=d_redis,
                poller_errors=d_errors,
                reconnects=d_reconn,
                gaps_found=d_gaps,
                poller_uptime_sec=round(d_uptime, 1),
            )
        except Exception:
            logger.warning("daily_stats_flush_failed", exc_info=True)


async def _stats_flusher_loop(flusher: _PollerStatsFlusher) -> None:
    """Flush poller stats to daily_stats every 5 minutes."""
    while True:
        try:
            await asyncio.sleep(300.0)  # 5 min
            await flusher.flush()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("stats_flusher_error")
            await asyncio.sleep(60.0)


# ---------------------------------------------------------------
# Service uptime log — flush uptime deltas to DB
# ---------------------------------------------------------------

class _UptimeFlusher:
    """Tracks uptime/downtime deltas and inserts to service_uptime_log."""

    def __init__(self) -> None:
        self._last = PollerMetrics().uptime_snapshot()

    async def flush(self) -> None:
        from src.db import repository as repo
        from datetime import datetime, timezone

        now_ts = datetime.now(timezone.utc)
        current = PollerMetrics().uptime_snapshot()
        rows: list[dict] = []

        for svc in ("mt5", "db", "redis", "api"):
            cur_up, cur_dn = current[svc]
            prev_up, prev_dn = self._last.get(svc, (0.0, 0.0))
            d_up = max(cur_up - prev_up, 0.0)
            d_dn = max(cur_dn - prev_dn, 0.0)
            if d_up > 0 or d_dn > 0:
                rows.append({
                    "ts": now_ts,
                    "service": svc,
                    "up_sec": round(d_up, 2),
                    "down_sec": round(d_dn, 2),
                })

        self._last = current

        if rows:
            try:
                await repo.insert_uptime_log(rows)
            except Exception:
                logger.warning("uptime_log_flush_failed", exc_info=True)


async def _uptime_flusher_loop(flusher: _UptimeFlusher) -> None:
    """Flush uptime deltas to service_uptime_log every 5 minutes."""
    while True:
        try:
            await asyncio.sleep(300.0)  # 5 min
            await flusher.flush()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("uptime_flusher_error")
            await asyncio.sleep(60.0)


async def _uptime_summary_loop() -> None:
    """Refresh cached 24h / 30d uptime summaries from DB every 60 s."""
    from src.db import repository as repo

    metrics = PollerMetrics()
    while True:
        try:
            await asyncio.sleep(60.0)
            data_24h = await repo.query_uptime_summary("24 hours")
            data_30d = await repo.query_uptime_summary("30 days")
            metrics.update_cached_uptime(data_24h, data_30d)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("uptime_summary_query_failed", exc_info=True)
            await asyncio.sleep(30.0)


# ---------------------------------------------------------------
# Keyboard listener — Ctrl+X graceful shutdown (Windows)
# ---------------------------------------------------------------

async def _keyboard_listener(stop_event: asyncio.Event) -> None:
    """Poll for Ctrl+X (``\\x18``) keypress to trigger graceful shutdown."""
    while not stop_event.is_set():
        try:
            if msvcrt.kbhit():
                key = msvcrt.getch()
                if key == b"\x18":          # Ctrl+X
                    logger.info("ctrl_x_shutdown_requested")
                    stop_event.set()
                    return
                elif key == b"\x03":        # Ctrl+C (backup)
                    logger.info("ctrl_c_shutdown_requested")
                    stop_event.set()
                    return
            await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1.0)


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

    # --- Restore today's baseline counters from DB ---
    try:
        from src.db import repository as repo
        baseline = await repo.load_today_poller_stats()
        if baseline:
            metrics = PollerMetrics()
            with metrics._lock:
                metrics.ticks_total = baseline["ticks_received"]
                metrics.ticks_flushed_total = baseline["ticks_flushed"]
                metrics.candles_total = baseline["candles_upserted"]
                metrics.redis_pub_count = baseline["redis_published"]
                metrics.reconnect_count = baseline["reconnects"]
                metrics.gaps_found = baseline["gaps_found"]
                # Restore total errors as a single "previous" category
                prev_errors = baseline["poller_errors"]
                if prev_errors:
                    metrics.errors["previous_session"] = prev_errors
            logger.info(
                "daily_baseline_restored",
                ticks=baseline["ticks_received"],
                candles=baseline["candles_upserted"],
                reconnects=baseline["reconnects"],
                errors=baseline["poller_errors"],
                gaps=baseline["gaps_found"],
            )
    except Exception:
        logger.warning("daily_baseline_load_failed", exc_info=True)

    # --- Redis ---
    publisher = RedisPublisher(settings)
    try:
        await publisher.connect()
    except Exception:
        logger.error("redis_connect_failed", exc_info=True)
        sys.exit(1)

    # --- Restore minute-bucket counters from Redis ---
    try:
        import orjson as _orjson
        pool = get_redis_pool()
        raw = await pool.get("poller:minute_buckets")
        if raw:
            bucket_data = _orjson.loads(raw)
            restored = PollerMetrics().import_minute_buckets(bucket_data)
            logger.info("minute_buckets_restored", entries=restored)
    except Exception:
        logger.warning("minute_buckets_restore_failed", exc_info=True)

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

    # --- Daily stats flusher (persist metrics to DB every 5 min) ---
    stats_flusher = _PollerStatsFlusher()
    stats_flusher_task = asyncio.create_task(
        _stats_flusher_loop(stats_flusher),
        name="stats_flusher",
    )
    named_tasks["stats_flusher"] = stats_flusher_task

    # --- Uptime log flusher (persist uptime deltas every 5 min) ---
    uptime_flusher = _UptimeFlusher()
    uptime_flusher_task = asyncio.create_task(
        _uptime_flusher_loop(uptime_flusher),
        name="uptime_flusher",
    )
    named_tasks["uptime_flusher"] = uptime_flusher_task

    # --- Uptime summary cache (refresh 24h/30d from DB every 60s) ---
    uptime_summary_task = asyncio.create_task(
        _uptime_summary_loop(),
        name="uptime_summary",
    )
    named_tasks["uptime_summary"] = uptime_summary_task

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

    # --- Keyboard listener (Ctrl+X) ---
    kb_task = asyncio.create_task(
        _keyboard_listener(stop_event),
        name="keyboard_listener",
    )

    await stop_event.wait()

    # --- Cleanup ---
    logger.info("poller_shutting_down")

    kb_task.cancel()
    if dashboard_task:
        dashboard_task.cancel()
    monitor_task.cancel()
    health_checker_task.cancel()
    stats_flusher_task.cancel()
    uptime_flusher_task.cancel()
    uptime_summary_task.cancel()
    heartbeat_task.cancel()
    gap_scan_task.cancel()
    backfill_listener_task.cancel()
    cancel_tasks = [
        kb_task, monitor_task, health_checker_task, stats_flusher_task,
        uptime_flusher_task, uptime_summary_task,
        heartbeat_task, gap_scan_task, backfill_listener_task,
    ]
    if dashboard_task:
        cancel_tasks.insert(0, dashboard_task)
    await asyncio.gather(
        *cancel_tasks,
        return_exceptions=True,
    )

    # --- Final daily stats flush before exit ---
    try:
        await stats_flusher.flush()
        logger.info("daily_stats_final_flush_ok")
    except Exception:
        logger.warning("daily_stats_final_flush_failed", exc_info=True)

    # --- Final uptime log flush before exit ---
    try:
        await uptime_flusher.flush()
        logger.info("uptime_log_final_flush_ok")
    except Exception:
        logger.warning("uptime_log_final_flush_failed", exc_info=True)

    # --- Save minute-bucket counters to Redis for next startup ---
    try:
        import orjson as _orjson
        pool = get_redis_pool()
        bucket_data = PollerMetrics().export_minute_buckets()
        await pool.set(
            "poller:minute_buckets",
            _orjson.dumps(bucket_data),
            ex=8 * 86400,  # 8 days TTL (buckets cover max 7 days)
        )
        logger.info("minute_buckets_saved_to_redis")
    except Exception:
        logger.warning("minute_buckets_save_failed", exc_info=True)

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

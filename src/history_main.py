"""Dedicated Windows history service. No Collector, Trader or order interface.

All historical native calls and DB jobs run sequentially here. A native hang
ends only this OS process; its scheduler permits bounded restarts. Live never
falls back to this terminal or shares its executor. Failed work stays visible.
"""
from __future__ import annotations

import asyncio
import os
if os.name == "nt":
    # History is sequential I/O, not a BLAS workload. Avoid large inherited
    # numerical thread pools before importing numpy through Backfiller.
    for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[variable] = "1"
import time
from pathlib import Path
from uuid import uuid4

import orjson
import structlog
from sqlalchemy import text

from src.config import get_settings
from src.db.engine import get_engine
from src.logging_config import setup_logging
from src.history_authority import HistoryAuthority, HistoryAuthorityConflict
from src.mt5.backfill import Backfiller
from src.mt5.connection import _native_budget, fetch_symbol_digits, run_in_mt5
from src.mt5.history_connection import HistoryConnection
from src.redis_bus.backfill_manager import BackfillListener, durable_job_request
from src.redis_bus.pool import get_redis_pool

logger = structlog.get_logger(__name__)
HISTORY_LOCK = 771205069


class HistoryService:
    def __init__(self, settings, *, exit_process=os._exit):
        self.settings = settings
        self.exit_process = exit_process
        self.connection = HistoryConnection(settings, attach_only=True)
        self.backfiller = Backfiller(self.connection, settings)
        self.listener = BackfillListener(self.backfiller, settings)
        self.generation = uuid4().hex
        self.phase = "starting"
        self.connected = False
        self.last_success = None
        self.job_id = None
        self.last_scan = time.monotonic()
        self.reconnect_attempts = 0
        self.retry_delay = 0.0
        self.authority = HistoryAuthority(get_redis_pool)
        self.health_path = None

    async def publish_health(self):
        while True:
            native = _native_budget.status()
            permitted = self.authority.ready
            payload = dict(enabled=True, pid=os.getpid(), generation=self.generation,
                           phase=self.phase if permitted else "reconnecting",
                           connected=self.connected and permitted and not native["overdue"],
                           observed_at=time.time(), last_success=self.last_success,
                           job_id=self.job_id, native=native,
                           reconnect_attempts=self.reconnect_attempts,
                           retry_after_seconds=self.retry_delay,
                           terminal_path=self.settings.history_mt5_path,
                           authority_reason=self.authority.reason)
            if self.health_path is not None:
                try:
                    temporary = self.health_path.with_suffix(".tmp")
                    temporary.write_bytes(orjson.dumps({k: payload[k] for k in (
                        "pid", "generation", "observed_at", "phase", "connected", "authority_reason")}))
                    temporary.replace(self.health_path)
                except OSError:
                    # Windows readers/antivirus may briefly hold the old file.
                    # The supervisor's progress deadline handles persistent
                    # failure; a single sharing violation must not crash us.
                    logger.warning("history_local_health_publish_failed")
            try:
                async with asyncio.timeout(2):
                    await get_redis_pool().set("history:status", orjson.dumps(payload), ex=30)
            except Exception:
                logger.warning("history_health_publish_failed")
            if native["overdue"]:
                logger.critical("history_native_overdue_exit", generation=self.generation)
                self.exit_process(70)
                return
            await asyncio.sleep(2)

    async def verify_connection(self):
        # Keep the same IPC generation while the terminal reconnects to its
        # broker. Every probe is bounded by the native-call watchdog; no work
        # is dequeued while disconnected. Safety drift still fails closed.
        while not await run_in_mt5(self.connection.connection_proof):
            self.connected = False
            self.phase = "reconnecting"
            self.reconnect_attempts += 1
            self.retry_delay = min(2 ** min(self.reconnect_attempts, 5), 30)
            logger.warning("history_broker_reconnecting", attempt=self.reconnect_attempts,
                           retry_after_seconds=self.retry_delay)
            await asyncio.sleep(self.retry_delay)
        if self.reconnect_attempts:
            logger.info("history_broker_reconnected", attempts=self.reconnect_attempts)
        self.reconnect_attempts = 0
        self.retry_delay = 0.0
        self.connected = True
        self.last_success = time.time()

    async def work(self):
        from src.db import symbol_management as sm
        await self.authority.wait()
        # The exclusive DB lease is already held. Never reconcile jobs before
        # live isolation has been positively confirmed.
        async with asyncio.timeout(5):
            async with get_engine().begin() as conn:
                await conn.execute(text("""
                    UPDATE backfill_jobs SET status=CASE WHEN status='cancelling'
                      THEN 'cancelled' ELSE 'failed' END, finished_at=NOW(),updated_at=NOW(),
                      error='History generation interrupted; inspect retained range before retry'
                    WHERE status IN ('running','cancelling')
                """))
        await self.connection.connect()
        self.connected = True
        await self.listener.connect()
        while True:
            await self.authority.wait()
            await self.verify_connection()
            self.phase = "idle"
            async with asyncio.timeout(5):
                jobs = await sm.queued_jobs(limit=1)
            request = durable_job_request(jobs[0]) if jobs else None
            if request is None:
                async with asyncio.timeout(8):
                    item = await get_redis_pool().blpop("backfill:queue", timeout=5)
                if item:
                    request = orjson.loads(item[1])
            if request:
                # The queue wait may have outlived the broker connection.
                await self.verify_connection()
                # Keep requester correlation unchanged. Owner is tracked by the
                # exclusive service lease; interrupted ranges remain in the DB.
                self.phase = "backfill"
                self.job_id = request.get("job_id")
                await self.connection.select_symbols([request["symbol"]])
                await fetch_symbol_digits([request["symbol"]])
                await self.listener._handle_request(request)
                self.job_id = None
            elif time.monotonic() - self.last_scan >= self.settings.gap_scan_interval_min * 60:
                self.phase = "gap_scan"
                async with asyncio.timeout(5):
                    symbols = await sm.active_managed_symbol_names()
                await self.connection.select_symbols(symbols)
                await fetch_symbol_digits(symbols)
                self.backfiller.update_symbols(symbols)
                await self.backfiller.run_gap_scan()
                self.last_scan = time.monotonic()

    async def run(self):
        # Reserved connection holds sole-consumer ownership. Losing it ends the
        # generation before another process can continue historical work.
        engine = get_engine(self.settings.model_copy(update={"db_pool_min": 2, "db_pool_max": 3}))
        async with engine.connect() as lease:
            if not (await lease.execute(text("SELECT pg_try_advisory_lock(:key)"),
                                        {"key": HISTORY_LOCK})).scalar():
                raise HistoryAuthorityConflict("another_history_worker_owns_lease")
            await lease.commit()
            lease_pid = (await lease.execute(text("SELECT pg_backend_pid()"))).scalar()
            await lease.commit()
            async def lease_guard():
                while True:
                    async with asyncio.timeout(3):
                        current_pid = (await lease.execute(text("SELECT pg_backend_pid()"))).scalar()
                        if current_pid != lease_pid:
                            raise HistoryAuthorityConflict("history_lease_changed")
                        await lease.commit()
                    await asyncio.sleep(1)

            # Every native call, including calls within a multi-chunk backfill,
            # must independently pass admission. Already submitted native calls
            # retain their existing deadline; never bypass the native watchdog.
            previous_gate = _native_budget.before_run
            _native_budget.before_run = self.authority.wait
            tasks = [asyncio.create_task(self.work(), name="history_work"),
                     asyncio.create_task(lease_guard(), name="history_lease_guard"),
                     asyncio.create_task(self.authority.monitor(), name="history_authority"),
                     asyncio.create_task(self.publish_health(), name="history_health")]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            # Hard exit is deliberate: cancelling cannot stop a running MT5 DLL
            # call. No other Python process or terminal is killed here.
            self.connected = False
            self.phase = "failed"
            exit_code = 70
            for task in done:
                if not task.cancelled() and task.exception():
                    # Fixed reason codes, not raw exception/log text containing
                    # broker credentials or query parameters.
                    reasons = {
                        "Live Poller has not relinquished history ownership": "live_history_ownership_unconfirmed",
                        "History lease connection changed": "history_lease_changed",
                        "History terminal identity or trading-disable proof failed": "broker_readonly_proof_lost",
                    }
                    reason = reasons.get(str(task.exception()), "other_exception")
                    if isinstance(task.exception(), HistoryAuthorityConflict):
                        reason = str(task.exception())
                        exit_code = 78
                    elif reason == "broker_readonly_proof_lost":
                        exit_code = 78
                    logger.error("history_generation_failed", error_type=type(task.exception()).__name__,
                                 task=task.get_name(), reason=reason)
            self.exit_process(exit_code)
            # Only reached by an injected test exit function; real workers exit
            # the OS process so an uninterruptible DLL cannot survive a restart.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            _native_budget.before_run = previous_gate


def main():
    settings = get_settings()
    if not settings.history_worker_enabled:
        raise SystemExit("History worker is not enabled")
    setup_logging(settings.log_level, settings.log_format)
    from logging.handlers import RotatingFileHandler
    import logging
    logs = Path(__file__).resolve().parents[1] / "logs"
    logs.mkdir(exist_ok=True)
    handler = RotatingFileHandler(logs / "history.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    root_logger = logging.getLogger()
    if root_logger.handlers:
        handler.setFormatter(root_logger.handlers[0].formatter)
    root_logger.addHandler(handler)
    from src.mt5.history_limits import apply_history_memory_limit
    memory_fence = apply_history_memory_limit()
    # OS lock survives neither crash nor reboot; stale PID text is not a lock.
    import msvcrt
    path = Path(__file__).resolve().parents[1] / ".history.lock"
    with path.open("a+b") as lock:
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise HistoryAuthorityConflict("another_history_worker_os_lock") from exc
        service = HistoryService(settings)
        service.health_path = path.parent / ".runtime" / "history-worker-health.json"
        service.health_path.parent.mkdir(exist_ok=True)
        asyncio.run(service.run())


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        logger.critical("history_startup_failed", error_type=type(exc).__name__,
                        frames=[f"{Path(f.filename).name}:{f.lineno}" for f in traceback.extract_tb(exc.__traceback__)])
        os._exit(78 if isinstance(exc, HistoryAuthorityConflict) else 70)

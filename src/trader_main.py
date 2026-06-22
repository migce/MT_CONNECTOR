"""
MT5 Trader — main entry point.

Separate Windows-native process that:
1. Reads enabled trading accounts from the database.
2. Connects each account to its own MT5 terminal instance.
3. Periodically syncs deal history and open positions to the DB.
4. Exposes no market-data collection — that is the poller's job.

Usage::

    python -m src.trader_main

The process exits gracefully on Ctrl+C / SIGINT / SIGTERM.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

import orjson

from src.config import get_settings
from src.db.engine import dispose_engine, get_engine
from src.db.init_timescale import init_timescaledb
from src.db import trading_repository as repo
from src.logging_config import setup_logging
from src.mt5.trading import AccountSession, verify_credentials
from src.mt5.portable import ensure_portable_terminal
from src.redis_bus.pool import get_redis_pool, close_redis_pool

logger = structlog.get_logger(__name__)

# Single-instance lock
_LOCK_FILE = ".trader.lock"
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
_DEAL_SYNC_STATUS_TTL_SEC = 7 * 24 * 3600


def _select_deal_sync_window(
    *,
    now: datetime,
    last_deal_time: datetime | None,
    force_full: bool,
    first_run: bool,
    deep_resync: bool,
    incremental_overlap_days: int,
    startup_catchup_days: int,
    deep_resync_days: int,
) -> tuple[datetime, str]:
    """Choose a conservative MT5 deal-history sync start.

    MT5 history can expose close deals later than open deals already stored
    in the DB.  Startup/deep passes therefore reread a wider window instead
    of relying only on ``max(deals.time)``.
    """
    if force_full:
        return _EPOCH, "force_full"
    if last_deal_time is None:
        return _EPOCH, "initial_full"

    incremental_from = last_deal_time - timedelta(days=incremental_overlap_days)
    if first_run:
        catchup_from = now - timedelta(days=startup_catchup_days)
        return min(incremental_from, catchup_from), "startup_catchup"
    if deep_resync:
        deep_from = now - timedelta(days=deep_resync_days)
        return min(incremental_from, deep_from), "deep_resync"
    return incremental_from, "incremental"


async def _publish_deal_sync_status(
    session: AccountSession,
    *,
    phase: str,
    status: str,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    rows: int = 0,
    upsert_stats: dict[str, int] | None = None,
    latest_deal_time: datetime | None = None,
    latest_deal_ticket: int | None = None,
    error: str | None = None,
) -> None:
    """Publish last deal-sync result for diagnostics and monitor screens."""
    try:
        pool = get_redis_pool()
        payload: dict[str, Any] = {
            "account_id": session.account_id,
            "login": session.login,
            "phase": phase,
            "status": status,
            "rows": rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if date_from is not None:
            payload["date_from"] = date_from.isoformat()
        if date_to is not None:
            payload["date_to"] = date_to.isoformat()
        if latest_deal_time is not None:
            payload["latest_deal_time"] = latest_deal_time.isoformat()
        if latest_deal_ticket is not None:
            payload["latest_deal_ticket"] = latest_deal_ticket
        if upsert_stats is not None:
            payload["upsert"] = upsert_stats
        if error is not None:
            payload["error"] = error

        await pool.set(
            f"trader:deal_sync:{session.account_id}",
            orjson.dumps(payload),
            ex=_DEAL_SYNC_STATUS_TTL_SEC,
        )
    except Exception:
        logger.debug(
            "deal_sync_status_publish_failed",
            account_id=session.account_id,
            exc_info=True,
        )


def _acquire_lock() -> object:
    """Acquire an exclusive file lock. Exit immediately if another instance is running."""
    import msvcrt
    try:
        fh = open(_LOCK_FILE, "w")  # noqa: SIM115
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        fh.write(str(os.getpid()))
        fh.flush()
        return fh
    except (OSError, IOError):
        sys.stderr.write(
            "\n  ✗ Another trader instance is already running.\n"
            "    Kill it first or delete .trader.lock\n",
        )
        sys.exit(1)


def _release_lock(fh: object) -> None:
    import msvcrt
    try:
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[union-attr]
        fh.close()  # type: ignore[union-attr]
        os.remove(_LOCK_FILE)
    except OSError:
        pass


# ---------------------------------------------------------------
# Sync loops
# ---------------------------------------------------------------

async def _sync_deals(
    session: AccountSession,
    interval_sec: int = 60,
) -> None:
    """Fetch deal history with crash-safe catch-up windows.

    First iteration rereads a wider recent window even if the DB already
    has later open deals.  Periodic deep resync repeats that protection.
    Normal iterations still use a smaller overlap for efficiency.

    If the Redis key ``trader:resync:{account_id}`` exists, force a
    full pull from epoch (set via admin sync-history endpoint).
    """
    settings = get_settings()
    lookahead_hours = settings.mt5_history_lookahead_hours
    first_run = True
    last_deep_resync_mono = 0.0
    deep_interval_sec = settings.trader_deep_resync_interval_hours * 3600

    while True:
        phase = "unknown"
        date_from: datetime | None = None
        date_to: datetime | None = None
        try:
            now = datetime.now(timezone.utc)
            date_to = now + timedelta(hours=lookahead_hours)

            # Check for force-resync flag
            force_full = False
            try:
                pool = get_redis_pool()
                key = f"trader:resync:{session.account_id}"
                if await pool.exists(key):
                    await pool.delete(key)
                    force_full = True
                    logger.info(
                        "deals_force_resync_triggered",
                        account_id=session.account_id,
                        login=session.login,
                    )
            except Exception:
                pass  # Redis unavailable — fall through to normal logic

            now_mono = time.monotonic()
            deep_resync = (
                not first_run
                and not force_full
                and deep_interval_sec > 0
                and now_mono - last_deep_resync_mono >= deep_interval_sec
            )
            last_time = await repo.get_last_deal_time(session.account_id)
            date_from, phase = _select_deal_sync_window(
                now=now,
                last_deal_time=last_time,
                force_full=force_full,
                first_run=first_run,
                deep_resync=deep_resync,
                incremental_overlap_days=settings.trader_incremental_overlap_days,
                startup_catchup_days=settings.trader_startup_catchup_days,
                deep_resync_days=settings.trader_deep_resync_days,
            )

            logger.info(
                "deals_sync_request",
                account_id=session.account_id,
                login=session.login,
                force_full=force_full,
                phase=phase,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                lookahead_hours=lookahead_hours,
                last_deal_time=last_time.isoformat() if last_time else None,
            )

            deals = await session.get_deals(date_from, date_to)
            upsert_stats: dict[str, int] | None = None
            latest_deal_time: datetime | None = None
            latest_deal_ticket: int | None = None
            if deals:
                max_deal = max(
                    deals,
                    key=lambda d: (d.get("time") or _EPOCH, int(d.get("ticket", 0))),
                )
                upsert_stats = await repo.upsert_deals(deals)
                latest_deal_time = max_deal.get("time")
                latest_deal_ticket = int(max_deal.get("ticket", 0))
                logger.info(
                    "deals_synced",
                    account_id=session.account_id,
                    login=session.login,
                    phase=phase,
                    count=len(deals),
                    since=date_from.isoformat(),
                    max_deal_time=latest_deal_time.isoformat() if latest_deal_time else None,
                    max_deal_ticket=latest_deal_ticket,
                    upserted_total=upsert_stats["total"],
                    inserted=upsert_stats["inserted"],
                    updated=upsert_stats["updated"],
                )
            else:
                logger.info(
                    "deals_synced_empty",
                    account_id=session.account_id,
                    login=session.login,
                    phase=phase,
                    since=date_from.isoformat(),
                    until=date_to.isoformat(),
                )

            await _publish_deal_sync_status(
                session,
                phase=phase,
                status="ok",
                date_from=date_from,
                date_to=date_to,
                rows=len(deals),
                upsert_stats=upsert_stats,
                latest_deal_time=latest_deal_time,
                latest_deal_ticket=latest_deal_ticket,
            )
            if first_run or deep_resync or force_full:
                last_deep_resync_mono = now_mono
            first_run = False
            await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            break
        except Exception:
            await _publish_deal_sync_status(
                session,
                phase=phase,
                status="error",
                date_from=date_from,
                date_to=date_to,
                error="deal_sync_error",
            )
            logger.exception(
                "deal_sync_error",
                account_id=session.account_id,
            )
            await asyncio.sleep(30)


async def _sync_positions(
    session: AccountSession,
    interval_sec: int = 10,
) -> None:
    """Periodically snapshot open positions."""
    while True:
        try:
            positions = await session.get_positions()
            await repo.sync_positions(session.account_id, positions)
            logger.debug(
                "positions_synced",
                account_id=session.account_id,
                count=len(positions),
            )
            await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception(
                "position_sync_error",
                account_id=session.account_id,
            )
            await asyncio.sleep(15)


async def _sync_account_info(
    session: AccountSession,
    interval_sec: int = 10,
) -> None:
    """Periodically snapshot account balance / equity / margin."""
    while True:
        try:
            info = await session.get_account_info()
            if info:
                row = {
                    "account_id": session.account_id,
                    "balance": info.get("balance", 0),
                    "equity": info.get("equity", 0),
                    "margin": info.get("margin", 0),
                    "margin_free": info.get("margin_free", 0),
                    "margin_level": info.get("margin_level", 0),
                    "leverage": info.get("leverage", 0),
                    "currency": info.get("currency", "USD"),
                    "profit": info.get("profit", 0),
                    "name": info.get("name", ""),
                    "server": info.get("server", ""),
                    "trade_mode": info.get("trade_mode", 0),
                }
                await repo.upsert_account_info(row)
                logger.debug(
                    "account_info_synced",
                    account_id=session.account_id,
                    balance=row["balance"],
                    equity=row["equity"],
                )
            await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception(
                "account_info_sync_error",
                account_id=session.account_id,
            )
            await asyncio.sleep(15)


async def _run_account(session: AccountSession) -> None:
    """Connect to one account and run deal/position sync loops.

    If the initial connection fails, retries every 30 s (in case the
    MT5 terminal isn't ready yet or credentials are temporarily wrong).

    Runs a periodic health check; if the MT5 terminal dies (e.g. closed
    manually), the task exits so that ``_reconcile`` can restart it.
    """
    ok = await session.connect()
    if not ok:
        logger.warning(
            "trader_account_connect_failed",
            account_id=session.account_id,
            login=session.login,
        )
        return

    # Health check coroutine — exits (cancels siblings) when terminal dies
    async def _health_watchdog(
        session: AccountSession,
        interval_sec: int = 15,
    ) -> None:
        while True:
            await asyncio.sleep(interval_sec)
            try:
                alive = await session.check_health()
                if not alive:
                    logger.warning(
                        "mt5_terminal_dead_detected",
                        account_id=session.account_id,
                        login=session.login,
                    )
                    return  # exit → gather completes → task is done
            except Exception:
                logger.exception(
                    "health_check_error",
                    account_id=session.account_id,
                )
                return

    tasks = [
        asyncio.create_task(
            _sync_deals(session),
            name=f"deals-{session.login}",
        ),
        asyncio.create_task(
            _sync_positions(session),
            name=f"positions-{session.login}",
        ),
        asyncio.create_task(
            _sync_account_info(session),
            name=f"account-info-{session.login}",
        ),
        asyncio.create_task(
            _health_watchdog(session),
            name=f"health-{session.login}",
        ),
    ]

    try:
        # Wait until ANY task finishes (health watchdog exits on dead terminal)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # If health watchdog finished, cancel everything else
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await session.disconnect()


# ---------------------------------------------------------------
# Verify listener (Redis RPC)
# ---------------------------------------------------------------

async def _verify_listener() -> None:
    """Listen for credential-verify requests from the API via Redis pub/sub.

    The API publishes a request ID to ``trader:verify:requests``.
    This task reads the request payload from ``trader:verify:{id}:req``,
    runs the verification in a temp subprocess, and writes the result
    to ``trader:verify:{id}:resp``.
    """
    settings = get_settings()
    pool = get_redis_pool()
    pubsub = pool.pubsub()
    await pubsub.subscribe("trader:verify:requests")
    logger.info("verify_listener_started")

    try:
        while True:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            if msg is None:
                await asyncio.sleep(0.1)
                continue

            req_id = msg["data"]
            if isinstance(req_id, bytes):
                req_id = req_id.decode()

            req_key = f"trader:verify:{req_id}:req"
            resp_key = f"trader:verify:{req_id}:resp"

            raw = await pool.get(req_key)
            if raw is None:
                continue

            try:
                payload = orjson.loads(raw)
                logger.info(
                    "verify_request_received",
                    login=payload.get("mt5_login"),
                    server=payload.get("mt5_server"),
                )

                result = await verify_credentials(
                    login=payload["mt5_login"],
                    password=payload["mt5_password"],
                    server=payload["mt5_server"],
                    mt5_path=ensure_portable_terminal(
                        login=payload["mt5_login"],
                        source_path=settings.mt5_path,
                        portable_dir=settings.mt5_portable_dir,
                    ),
                )

                await pool.set(resp_key, orjson.dumps(result), ex=120)
                await pool.delete(req_key)

                logger.info(
                    "verify_request_completed",
                    login=payload.get("mt5_login"),
                    ok=result.get("ok", False),
                )
            except Exception:
                logger.exception("verify_request_error", req_id=req_id)
                error_resp = {
                    "ok": False,
                    "error_code": -99,
                    "error_msg": "Internal verification error",
                }
                await pool.set(resp_key, orjson.dumps(error_resp), ex=120)
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe("trader:verify:requests")
        await pubsub.close()
        logger.info("verify_listener_stopped")


async def _main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)
    logger.info("trader_starting")

    # Init DB
    get_engine(settings)
    try:
        await init_timescaledb()
    except Exception:
        logger.warning("timescaledb_init_skipped", exc_info=True)

    # ------------------------------------------------------------------
    # Mutable state: running sessions & tasks, keyed by account_id
    # ------------------------------------------------------------------
    sessions: dict[int, AccountSession] = {}
    account_tasks: dict[int, asyncio.Task] = {}

    async def _start_account(acct: dict) -> None:
        """Start a sync task for one account."""
        aid = acct["id"]
        if aid in sessions:
            return  # already running

        # Ensure a portable terminal copy exists for this login
        mt5_path = acct["mt5_path"]
        try:
            mt5_path = ensure_portable_terminal(
                login=acct["mt5_login"],
                source_path=settings.mt5_path,
                portable_dir=settings.mt5_portable_dir,
            )
            # Update DB if path was missing or different
            if mt5_path != acct["mt5_path"]:
                await repo.update_account(aid, mt5_path=mt5_path)
        except Exception:
            logger.exception(
                "portable_provision_failed",
                account_id=aid,
                login=acct["mt5_login"],
            )

        s = AccountSession(
            account_id=aid,
            login=acct["mt5_login"],
            password=acct["mt5_password"],
            server=acct["mt5_server"],
            mt5_path=mt5_path,
            mt5_server_time_offset_hours=settings.mt5_server_time_offset_hours,
        )
        sessions[aid] = s
        account_tasks[aid] = asyncio.create_task(
            _run_account(s),
            name=f"account-{acct['mt5_login']}",
        )
        logger.info("account_task_started", account_id=aid, login=acct["mt5_login"])

    async def _stop_account(aid: int) -> None:
        """Stop and clean up one account session."""
        task = account_tasks.pop(aid, None)
        session = sessions.pop(aid, None)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=10)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        if session:
            try:
                await session.disconnect()
            except Exception:
                pass
        logger.info("account_task_stopped", account_id=aid)

    async def _restart_account(acct: dict) -> None:
        """Stop then start — used when credentials/settings changed."""
        await _stop_account(acct["id"])
        await _start_account(acct)

    # ------------------------------------------------------------------
    # Reconcile running state vs DB
    # ------------------------------------------------------------------

    def _acct_fingerprint(acct: dict) -> tuple:
        """Return a comparable fingerprint of credential-relevant fields."""
        return (
            acct["mt5_login"],
            acct["mt5_password"],
            acct["mt5_server"],
            acct["mt5_path"],
        )

    # Keep fingerprints so we know when creds changed
    fingerprints: dict[int, tuple] = {}

    async def _reconcile() -> None:
        """Sync running sessions with the database.

        - New enabled accounts → start.
        - Removed / disabled accounts → stop.
        - Changed credentials → restart.
        """
        try:
            db_accounts = await repo.get_enabled_accounts()
        except Exception:
            logger.exception("reconcile_db_read_error")
            return

        db_map = {a["id"]: a for a in db_accounts}
        db_ids = set(db_map.keys())
        running_ids = set(sessions.keys())

        # Accounts to stop (deleted or disabled)
        for aid in running_ids - db_ids:
            await _stop_account(aid)
            fingerprints.pop(aid, None)

        # Accounts to start or restart
        for aid in db_ids:
            acct = db_map[aid]
            fp = _acct_fingerprint(acct)
            if aid not in running_ids:
                # New account
                fingerprints[aid] = fp
                await _start_account(acct)
            elif fp != fingerprints.get(aid):
                # Credentials changed → restart
                logger.info(
                    "account_credentials_changed",
                    account_id=aid,
                    login=acct["mt5_login"],
                )
                fingerprints[aid] = fp
                await _restart_account(acct)
            else:
                # Check if the task died (connection failure) → restart
                task = account_tasks.get(aid)
                if task and task.done():
                    logger.info(
                        "account_task_died_restarting",
                        account_id=aid,
                        login=acct["mt5_login"],
                    )
                    sessions.pop(aid, None)
                    account_tasks.pop(aid, None)
                    await _start_account(acct)

    # ------------------------------------------------------------------
    # Account watcher — listens for reload signals + periodic poll
    # ------------------------------------------------------------------

    async def _account_watcher() -> None:
        """Watch for account changes via Redis pub/sub + periodic DB poll.

        The API publishes to ``trader:account:reload`` on every
        create / update / delete.  We also poll every 30 s as fallback
        (also catches dead tasks from killed MT5 terminals).
        """
        pool = get_redis_pool()
        pubsub = pool.pubsub()
        await pubsub.subscribe("trader:account:reload")
        logger.info("account_watcher_started")

        POLL_INTERVAL = 30  # seconds (fast enough to catch dead terminals)
        last_poll = time.time()

        try:
            while True:
                # Check for pub/sub notification (non-blocking)
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if msg is not None:
                    logger.info("account_reload_signal_received")
                    await _reconcile()
                    last_poll = time.time()

                # Periodic fallback poll
                if time.time() - last_poll >= POLL_INTERVAL:
                    await _reconcile()
                    last_poll = time.time()

                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("trader:account:reload")
            await pubsub.close()
            logger.info("account_watcher_stopped")

    # ------------------------------------------------------------------
    # Initial load
    # ------------------------------------------------------------------
    await _reconcile()

    # Publish trader status to Redis for API /health
    async def _status_publisher() -> None:
        """Publish trader heartbeat to Redis every 10 s (TTL 30 s)."""
        while True:
            try:
                pool = get_redis_pool()
                deal_sync: dict[str, Any] = {}
                for account_id in sessions:
                    raw = await pool.get(f"trader:deal_sync:{account_id}")
                    if raw:
                        try:
                            deal_sync[str(account_id)] = orjson.loads(raw)
                        except Exception:
                            pass
                status = {
                    "running": True,
                    "accounts": len(sessions),
                    "uptime_sec": round(time.time() - _start_ts, 1),
                    "deal_sync": deal_sync,
                }
                await pool.set("trader:status", orjson.dumps(status), ex=30)
            except Exception:
                pass
            await asyncio.sleep(10)

    _start_ts = time.time()
    status_task = asyncio.create_task(_status_publisher(), name="trader-status")
    verify_task = asyncio.create_task(_verify_listener(), name="verify-listener")
    watcher_task = asyncio.create_task(_account_watcher(), name="account-watcher")

    # Graceful shutdown on signal
    stop = asyncio.Event()

    def _signal_handler():
        logger.info("trader_shutdown_requested")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows: signal handlers via asyncio are limited
            signal.signal(sig, lambda *_: _signal_handler())

    await stop.wait()

    # Cancel all tasks
    status_task.cancel()
    verify_task.cancel()
    watcher_task.cancel()
    for t in list(account_tasks.values()):
        t.cancel()
    all_tasks = [status_task, verify_task, watcher_task] + list(account_tasks.values())
    await asyncio.gather(*all_tasks, return_exceptions=True)

    # Disconnect all sessions
    for s in list(sessions.values()):
        try:
            await s.disconnect()
        except Exception:
            pass

    # Clear trader status in Redis
    try:
        pool = get_redis_pool()
        await pool.delete("trader:status")
    except Exception:
        pass
    await close_redis_pool()
    await dispose_engine()
    logger.info("trader_stopped")


def main() -> None:
    lock_fh = _acquire_lock()
    try:
        asyncio.run(_main())
    finally:
        _release_lock(lock_fh)


if __name__ == "__main__":
    main()

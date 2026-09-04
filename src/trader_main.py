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
import os
import signal
import sys
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import orjson
import structlog

from src.config import get_settings
from src.db import trading_repository as repo
from src.db.engine import dispose_engine, get_engine
from src.db.init_timescale import init_timescaledb
from src.logging_config import setup_logging
from src.mt5.portable import ensure_portable_terminal
from src.mt5.trading import AccountSession, verify_credentials
from src.redis_bus.pool import close_redis_pool, get_redis_pool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = structlog.get_logger(__name__)

# Single-instance lock
_LOCK_FILE = ".trader.lock"
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
_DEAL_SYNC_STATUS_TTL_SEC = 7 * 24 * 3600
_POSITION_SYNC_STATUS_TTL_SEC = 120


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


def _position_event_candidates(
    previous: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build deterministic lifecycle deltas between two broker snapshots."""
    previous_by_ticket = {int(row["ticket"]): row for row in previous}
    current_by_ticket = {int(row["ticket"]): row for row in current}
    events: list[dict[str, Any]] = []

    for ticket, old in previous_by_ticket.items():
        new = current_by_ticket.get(ticket)
        old_volume = float(old.get("volume") or 0)
        new_volume = float(new.get("volume") or 0) if new else 0.0
        if new is None:
            event_type = "closed"
        elif new_volume < old_volume - 1e-9:
            event_type = "partial_close"
        elif new_volume > old_volume + 1e-9:
            event_type = "volume_increased"
        else:
            continue
        events.append({
            "event_type": event_type,
            "position_ticket": ticket,
            "position_identifier": int(old.get("identifier") or ticket),
            "symbol": str(old.get("symbol") or ""),
            "position_type": int(old.get("type", -1)),
            "magic": int(old.get("magic") or 0),
            "volume_before": old_volume,
            "volume_after": new_volume,
            "previous": old,
            "current": new,
        })

    for ticket, new in current_by_ticket.items():
        if ticket in previous_by_ticket:
            continue
        events.append({
            "event_type": "opened",
            "position_ticket": ticket,
            "position_identifier": int(new.get("identifier") or ticket),
            "symbol": str(new.get("symbol") or ""),
            "position_type": int(new.get("type", -1)),
            "magic": int(new.get("magic") or 0),
            "volume_before": 0.0,
            "volume_after": float(new.get("volume") or 0),
            "previous": None,
            "current": new,
        })
    return events


def _latest_position_exit_deal(deals: list[dict[str, Any]]) -> dict[str, Any] | None:
    exit_rows = [
        row for row in deals
        if int(row.get("entry", -1)) in {1, 2, 3}
        and int(row.get("type", -1)) in {0, 1}
    ]
    if not exit_rows:
        return None
    return max(
        exit_rows,
        key=lambda row: (
            int(row.get("time_msc") or 0),
            int(row.get("ticket") or 0),
        ),
    )


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


async def _publish_position_sync_status(
    session: AccountSession,
    *,
    positions: list[dict[str, Any]],
    started_at: datetime,
    interval_sec: float,
) -> None:
    """Publish proof that one complete MT5 position snapshot was persisted."""
    try:
        completed_at = datetime.now(timezone.utc)
        payload = {
            "account_id": session.account_id,
            "login": session.login,
            "status": "ok",
            "position_count": len(positions),
            "tickets": sorted(
                int(position["ticket"])
                for position in positions
                if position.get("ticket") is not None
            ),
            "started_at": started_at.isoformat(),
            "last_success_at": completed_at.isoformat(),
            "duration_ms": round(
                max(0.0, (completed_at - started_at).total_seconds()) * 1000,
                1,
            ),
            "interval_sec": float(interval_sec),
        }
        await get_redis_pool().set(
            f"trader:position_sync:{session.account_id}",
            orjson.dumps(payload),
            ex=_POSITION_SYNC_STATUS_TTL_SEC,
        )
    except Exception:
        logger.debug(
            "position_sync_status_publish_failed",
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
    interval_sec: float | None = None,
) -> None:
    """Snapshot positions and persist durable lifecycle events."""
    settings = get_settings()
    interval_sec = interval_sec or settings.trader_position_sync_interval_sec
    while True:
        try:
            started_at = datetime.now(timezone.utc)
            previous = await repo.query_positions(session.account_id)
            positions = await session.get_positions()
            observed_at = datetime.now(timezone.utc)
            for event in _position_event_candidates(previous, positions):
                event_time = observed_at
                event_time_msc: int | None = None
                close_deal_ticket: int | None = None
                exit_deal: dict[str, Any] | None = None
                if event["event_type"] in {"closed", "partial_close"}:
                    deals = await session.get_position_deals(event["position_identifier"])
                    exit_deal = _latest_position_exit_deal(deals)
                    if exit_deal is None:
                        # A missing snapshot alone is not sufficient to trigger
                        # money-moving behavior in consumers.
                        event["event_type"] = f"{event['event_type']}_unconfirmed"
                    else:
                        event_time = exit_deal.get("time") or observed_at
                        event_time_msc = int(exit_deal.get("time_msc") or 0) or None
                        close_deal_ticket = int(exit_deal.get("ticket") or 0) or None
                        # Make the authoritative exit deal visible to API
                        # consumers before publishing the lifecycle event.
                        # This removes the normal bulk-history sync delay from
                        # close-triggered workflows.
                        await repo.upsert_deals([exit_deal])
                elif event["event_type"] == "opened":
                    current = event.get("current") or {}
                    event_time = current.get("time") or observed_at

                identity = close_deal_ticket or event_time_msc or int(event_time.timestamp() * 1000)
                dedupe_key = (
                    f"{session.account_id}:{event['position_ticket']}:"
                    f"{event['event_type']}:{identity}:"
                    f"{event['volume_before']:.8f}:{event['volume_after']:.8f}"
                )
                await repo.insert_broker_position_event({
                    "dedupe_key": dedupe_key,
                    "account_id": session.account_id,
                    "event_type": event["event_type"],
                    "position_ticket": event["position_ticket"],
                    "position_identifier": event["position_identifier"],
                    "symbol": event["symbol"],
                    "position_type": event["position_type"],
                    "magic": event["magic"],
                    "volume_before": event["volume_before"],
                    "volume_after": event["volume_after"],
                    "event_time": event_time,
                    "event_time_msc": event_time_msc,
                    "close_deal_ticket": close_deal_ticket,
                    "payload": {
                        "previous": event.get("previous"),
                        "current": event.get("current"),
                        "close_deal": exit_deal,
                    },
                })
            await repo.sync_positions(session.account_id, positions)
            await _publish_position_sync_status(
                session,
                positions=positions,
                started_at=started_at,
                interval_sec=interval_sec,
            )
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


def _attempt_time(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return fallback


async def _trade_command_dispatcher(sessions: dict[int, AccountSession]) -> None:
    """Claim persisted close commands and execute them in account actors."""
    settings = get_settings()
    poll_sec = settings.trader_command_poll_interval_ms / 1000.0
    await repo.requeue_stale_claimed_commands(settings.trader_command_claim_timeout_sec)
    logger.info("trade_command_dispatcher_started")

    while True:
        try:
            if not settings.trading_execution_enabled:
                await asyncio.sleep(max(1.0, poll_sec))
                continue

            await repo.expire_trade_commands()
            allowed_ids = settings.trading_account_allowlist
            ready_account_ids = [
                account_id
                for account_id, session in sessions.items()
                if account_id in allowed_ids and session.connected
            ]
            command = await repo.claim_next_trade_command(ready_account_ids)
            if command is None:
                await asyncio.sleep(poll_sec)
                continue

            command_id = str(command["id"])
            session = sessions.get(int(command["account_id"]))
            if session is None or not session.connected:
                await repo.retry_trade_command(
                    command_id,
                    result={},
                    error="account_session_not_ready",
                    delay_sec=settings.trader_close_retry_delay_sec,
                )
                continue

            started_at = datetime.now(timezone.utc)
            result = await session.close_position(
                command_id=command_id,
                position_ticket=int(command["position_ticket"]),
                expected_position_identifier=command.get("expected_position_identifier"),
                expected_symbol=str(command["expected_symbol"]),
                expected_type=int(command["expected_type"]),
                expected_magic=command.get("expected_magic"),
                max_volume=float(command["max_volume"]),
                deviation_points=settings.trader_close_deviation_points,
                send_attempts=settings.trader_close_send_attempts,
                reconcile_timeout_sec=settings.trader_close_reconcile_timeout_sec,
            )
            finished_at = datetime.now(timezone.utc)
            for attempt in result.get("attempts", []):
                await repo.append_trade_attempt(
                    command_id=command_id,
                    attempt_no=int(command["attempt_count"]),
                    phase=str(attempt.get("phase") or "unknown"),
                    retcode=attempt.get("retcode"),
                    message=attempt.get("message"),
                    request_payload=attempt.get("request") or {},
                    result_payload=attempt.get("result") or {},
                    started_at=_attempt_time(attempt.get("started_at"), started_at),
                    finished_at=_attempt_time(attempt.get("finished_at"), finished_at),
                )

            outcome = str(result.get("status") or "unknown")
            error = str(result.get("error") or "") or None
            if outcome in {"confirmed", "already_satisfied"}:
                await repo.finish_trade_command(
                    command_id,
                    status=outcome,
                    result=result,
                    error=None,
                )
            elif bool(result.get("retryable")):
                expires_at = command.get("expires_at")
                if expires_at is not None and expires_at <= finished_at:
                    await repo.finish_trade_command(
                        command_id,
                        status="expired",
                        result=result,
                        error=error or "command_expired",
                    )
                else:
                    await repo.retry_trade_command(
                        command_id,
                        result=result,
                        error=error or outcome,
                        delay_sec=settings.trader_close_retry_delay_sec,
                    )
            else:
                await repo.finish_trade_command(
                    command_id,
                    status=outcome if outcome in {"rejected", "unknown"} else "unknown",
                    result=result,
                    error=error or outcome,
                )
            logger.info(
                "trade_command_processed",
                command_id=command_id,
                account_id=command["account_id"],
                outcome=outcome,
                retryable=bool(result.get("retryable")),
            )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("trade_command_dispatcher_error")
            await asyncio.sleep(max(1.0, poll_sec))

    logger.info("trade_command_dispatcher_stopped")


async def _run_account_watcher(
    reconcile: Callable[[], Awaitable[None]],
    *,
    poll_interval_sec: float = 30.0,
    retry_delay_sec: float = 2.0,
    message_timeout_sec: float = 1.0,
    loop_sleep_sec: float = 0.5,
) -> None:
    """Reconcile account actors continuously, surviving Redis restarts.

    Redis pub/sub is only a low-latency wake-up path.  The periodic database
    poll remains authoritative, so losing the subscription must never disable
    dead-account recovery.  Any Redis/pub-sub failure closes that subscription,
    waits briefly, and establishes a new one.
    """
    reconnects = 0
    try:
        while True:
            pubsub = None
            try:
                pool = get_redis_pool()
                pubsub = pool.pubsub()
                await pubsub.subscribe("trader:account:reload")
                logger.info(
                    "account_watcher_started" if reconnects == 0 else "account_watcher_reconnected",
                    reconnects=reconnects,
                )
                last_poll = time.monotonic()

                while True:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=message_timeout_sec,
                    )
                    if msg is not None:
                        logger.info("account_reload_signal_received")
                        await reconcile()
                        last_poll = time.monotonic()

                    if time.monotonic() - last_poll >= poll_interval_sec:
                        await reconcile()
                        last_poll = time.monotonic()

                    await asyncio.sleep(loop_sleep_sec)
            except asyncio.CancelledError:
                raise
            except Exception:
                reconnects += 1
                logger.exception(
                    "account_watcher_connection_error",
                    reconnects=reconnects,
                    retry_delay_sec=retry_delay_sec,
                )
                await asyncio.sleep(retry_delay_sec)
            finally:
                if pubsub is not None:
                    with suppress(Exception):
                        await pubsub.unsubscribe("trader:account:reload")
                    with suppress(Exception):
                        await pubsub.aclose()
    finally:
        logger.info("account_watcher_stopped")


def _account_health_snapshot(
    sessions: dict[int, AccountSession],
    account_tasks: dict[int, asyncio.Task],
) -> tuple[dict[str, dict[str, bool]], list[int]]:
    """Return truthful per-account actor health for status publication."""
    states: dict[str, dict[str, bool]] = {}
    degraded: list[int] = []
    for account_id in sorted(set(sessions) | set(account_tasks)):
        session = sessions.get(account_id)
        task = account_tasks.get(account_id)
        task_alive = task is not None and not task.done()
        connected = bool(session is not None and session.connected)
        healthy = task_alive and connected
        states[str(account_id)] = {
            "connected": connected,
            "task_alive": task_alive,
            "healthy": healthy,
        }
        if not healthy:
            degraded.append(account_id)
    return states, degraded


def _stop_when_critical_task_exits(task: asyncio.Task, stop: asyncio.Event) -> None:
    """Request a clean Trader exit so the outer supervisor can restart it."""
    if stop.is_set():
        return
    error: str | None = None
    if task.cancelled():
        error = "cancelled"
    else:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            error = "cancelled"
        else:
            if exc is not None:
                error = repr(exc)
    logger.critical(
        "critical_background_task_stopped",
        task=task.get_name(),
        error=error,
    )
    stop.set()


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
                account_health, degraded_account_ids = _account_health_snapshot(
                    sessions,
                    account_tasks,
                )
                status = {
                    "running": True,
                    "accounts": len(sessions),
                    "accounts_healthy": len(account_health) - len(degraded_account_ids),
                    "degraded_account_ids": degraded_account_ids,
                    "account_health": account_health,
                    "uptime_sec": round(time.time() - _start_ts, 1),
                    "deal_sync": deal_sync,
                    "trading_execution_enabled": settings.trading_execution_enabled,
                    "trading_account_allowlist": sorted(settings.trading_account_allowlist),
                }
                await pool.set("trader:status", orjson.dumps(status), ex=30)
            except Exception:
                pass
            await asyncio.sleep(10)

    _start_ts = time.time()
    stop = asyncio.Event()
    status_task = asyncio.create_task(_status_publisher(), name="trader-status")
    verify_task = asyncio.create_task(_verify_listener(), name="verify-listener")
    watcher_task = asyncio.create_task(
        _run_account_watcher(_reconcile),
        name="account-watcher",
    )
    command_task = asyncio.create_task(
        _trade_command_dispatcher(sessions),
        name="trade-command-dispatcher",
    )
    watcher_task.add_done_callback(lambda task: _stop_when_critical_task_exits(task, stop))

    # Graceful shutdown on signal
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
    command_task.cancel()
    for t in list(account_tasks.values()):
        t.cancel()
    all_tasks = [status_task, verify_task, watcher_task, command_task] + list(account_tasks.values())
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

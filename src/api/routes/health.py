"""
REST endpoint: ``/api/v1/health``

Service health check — reports MT5 / DB / Redis connectivity,
uptime, and active symbol count.

MT5 status is read from a Redis key (``poller:status``) that the
Windows poller refreshes every 10 s with a 30 s TTL.
"""

from __future__ import annotations

import time
import asyncio

import orjson
from fastapi import APIRouter
from sqlalchemy import text

from src.api.schemas import HealthResponse, ServiceUptimeEntry, UptimeResponse
from src.config import get_settings
from src.db.engine import get_engine
from src.redis_bus.pool import get_redis_pool

router = APIRouter(prefix="/api/v1", tags=["health"])

# Set once when the module is first imported (≈ app startup).
_start_time: float = time.time()
_history_cache: dict = {}


def _fresh_history(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    try:
        # Same bounded cross-host clock tolerance as position snapshots.
        return -2 <= time.time() - float(data.get("observed_at", 0)) < 30
    except (TypeError, ValueError, OverflowError):
        return False


async def _read_history_status() -> dict:
    global _history_cache
    try:
        async with asyncio.timeout(1):
            for attempt in range(3):
                raw = await get_redis_pool().get("history:status")
                data = orjson.loads(raw) if raw else {}
                if _fresh_history(data):
                    if (not _fresh_history(_history_cache) or
                            float(data["observed_at"]) >= float(_history_cache["observed_at"])):
                        _history_cache = data
                    break
                if attempt < 2:
                    await asyncio.sleep(0.05)
    except Exception:
        pass
    # Do not turn a recently verified proof into an outage merely because an
    # allkeys-lru cache evicted it between two GETs. Never extend its 30s age.
    return dict(_history_cache) if _fresh_history(_history_cache) else {}


@router.get("/history/status", summary="Isolated history generation and progress")
async def history_status() -> dict:
    from fastapi import HTTPException
    if not get_settings().history_worker_enabled:
        return {"enabled": False, "phase": "disabled"}
    try:
        data = await _read_history_status()
        if not data:
            raise ValueError("stale")
        return data
    except Exception:
        raise HTTPException(503, "Isolated history status unavailable") from None


def _trader_health_from_payload(data: object) -> tuple[bool, int, int, list[int]]:
    """Interpret process and per-account Trader health with legacy fallback."""
    if not isinstance(data, dict):
        return False, 0, 0, []
    running = bool(data.get("running", False))
    total = int(data.get("accounts") or 0)
    healthy_raw = data.get("accounts_healthy")
    if healthy_raw is None:
        return running, total, total if running else 0, []
    healthy = int(healthy_raw)
    degraded = [int(value) for value in (data.get("degraded_account_ids") or [])]
    connected = running and healthy == total and not degraded
    return connected, total, healthy, degraded


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health check",
    description=(
        "Reports connectivity of all subsystems:\n\n"
        "- **mt5_connected** — `true` if the Windows poller is running and "
        "connected to the MT5 terminal (status relayed via Redis with 30 s TTL)\n"
        "- **db_connected** — TimescaleDB reachable\n"
        "- **redis_connected** — Redis reachable\n"
        "- **status** — `ok` when DB is up, `degraded` otherwise\n\n"
        "Use this endpoint for liveness probes and monitoring dashboards."
    ),
)
async def health_check() -> HealthResponse:
    settings = get_settings()

    # DB check
    db_ok = False
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    # Redis check (reuses shared pool)
    redis_ok = False
    r = None
    try:
        r = get_redis_pool()
        await r.ping()
        redis_ok = True
    except Exception:
        pass

    # MT5 status from poller (via Redis key with 30s TTL)
    mt5_ok = False
    trader_ok = False
    trader_accounts_total = 0
    trader_accounts_healthy = 0
    trader_degraded_account_ids: list[int] = []
    if redis_ok and r is not None:
        try:
            raw = await r.get("poller:status")
            if raw is not None:
                poller_data = orjson.loads(raw)
                mt5_ok = bool(poller_data.get("mt5_connected", False))
        except Exception:
            pass
        try:
            raw_t = await r.get("trader:status")
            if raw_t is not None:
                trader_data = orjson.loads(raw_t)
                (
                    trader_ok,
                    trader_accounts_total,
                    trader_accounts_healthy,
                    trader_degraded_account_ids,
                ) = _trader_health_from_payload(trader_data)
        except Exception:
            pass

    history = {}
    if settings.history_worker_enabled and redis_ok and r is not None:
        history = await _read_history_status()
    return HealthResponse(
        history_enabled=settings.history_worker_enabled,
        history_connected=bool(history.get("connected")),
        history_phase=str(history.get("phase", "unavailable" if settings.history_worker_enabled else "disabled")),
        history_terminal_path=settings.history_mt5_path if settings.history_worker_enabled else None,
        status="ok" if db_ok else "degraded",
        mt5_connected=mt5_ok,
        trader_connected=trader_ok,
        trader_accounts_total=trader_accounts_total,
        trader_accounts_healthy=trader_accounts_healthy,
        trader_degraded_account_ids=trader_degraded_account_ids,
        db_connected=db_ok,
        redis_connected=redis_ok,
        uptime_sec=round(time.time() - _start_time, 1),
        symbols_active=len(settings.symbols),
    )


@router.get(
    "/uptime",
    response_model=UptimeResponse,
    summary="Service uptime summary (24 h / 30 d)",
    description=(
        "Returns cumulative uptime / downtime for every monitored service "
        "(MT5, TimescaleDB, Redis, API) over the last **24 hours** and "
        "**30 days**.\n\n"
        "Data is sourced from the `service_uptime_log` hypertable which "
        "the poller flushes every 5 minutes."
    ),
)
async def uptime_summary() -> UptimeResponse:
    from src.db import repository as repo

    def _to_entries(d: dict[str, tuple[float, float, float]]) -> list[ServiceUptimeEntry]:
        return [
            ServiceUptimeEntry(
                service=svc,
                up_sec=round(up, 2),
                down_sec=round(dn, 2),
                uptime_pct=round(pct, 2),
            )
            for svc, (up, dn, pct) in sorted(d.items())
        ]

    try:
        data_24h = await repo.query_uptime_summary("24 hours")
        data_30d = await repo.query_uptime_summary("30 days")
    except Exception:
        data_24h, data_30d = {}, {}

    return UptimeResponse(
        period_24h=_to_entries(data_24h),
        period_30d=_to_entries(data_30d),
    )

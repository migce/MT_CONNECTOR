"""
REST endpoint: ``/api/v1/health``

Service health check — reports MT5 / DB / Redis connectivity,
uptime, and active symbol count.

MT5 status is read from a Redis key (``poller:status``) that the
Windows poller refreshes every 10 s with a 30 s TTL.
"""

from __future__ import annotations

import time

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

    return HealthResponse(
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

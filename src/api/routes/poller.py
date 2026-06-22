"""
REST endpoint: ``/api/v1/poller/status``

Full poller dashboard snapshot — same data the Rich terminal dashboard renders,
exposed as JSON via a Redis key that the poller refreshes every 2 s with a 10 s TTL.
"""

from __future__ import annotations

import orjson
from fastapi import APIRouter, HTTPException

from src.redis_bus.pool import get_redis_pool

router = APIRouter(prefix="/api/v1/poller", tags=["poller"])


@router.get(
    "/status",
    summary="Full poller dashboard snapshot",
    description=(
        "Returns the complete poller status — the same data rendered by the "
        "Rich terminal dashboard:\n\n"
        "- **MT5 connection** state, uptime, reconnect count\n"
        "- **Tick stats**: total, flushed, rate (t/s), peak, buffer depth, "
        "time-window counts (1h / 12h / 24h / 7d)\n"
        "- **Candle stats**: total, redis pub count, flush avg/last ms, "
        "time-window counts\n"
        "- **Errors**: total, per-category breakdown, last error info\n"
        "- **Tasks**: alive/dead status of each async task\n"
        "- **Backfill**: current phase, on-demand log\n"
        "- **Infrastructure**: DB / Redis healthy, latency, DB size\n"
        "- **API stats**: healthy, latency, request/error counts\n"
        "- **Session uptime**: per-service up/down seconds and %\n"
        "- **Live prices**: bid, ask, tick count, age per symbol\n\n"
        "The poller writes this snapshot to Redis every **2 s** with a **10 s TTL**. "
        "If the poller is down the key expires and this endpoint returns 503."
    ),
)
async def poller_status() -> dict:
    try:
        r = get_redis_pool()
        raw = await r.get("poller:status")
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Poller snapshot unavailable — Redis connection error",
        )
    if raw is None:
        raise HTTPException(
            status_code=503,
            detail="Poller snapshot unavailable — poller may be offline",
        )
    try:
        return orjson.loads(raw)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Poller snapshot unavailable — corrupt data",
        )

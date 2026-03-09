"""
REST endpoints: ``/api/v1/stats`` and ``/api/v1/stats/daily``

``/stats`` — live API request metrics (in-memory).
``/stats/daily`` — historical daily statistics persisted in DB.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from src.api.middleware.request_metrics import ApiMetrics
from src.db import repository as repo

router = APIRouter(prefix="/api/v1", tags=["stats"])


@router.get(
    "/stats",
    summary="API request statistics",
    description=(
        "Returns API request metrics for monitoring:\n\n"
        "| Field | Description |\n"
        "|---|---|\n"
        "| `total_requests` | All-time request count |\n"
        "| `total_errors` | All-time error count (HTTP 4xx/5xx) |\n"
        "| `requests_1h` / `requests_12h` / `requests_24h` | Windowed request counts |\n"
        "| `errors_1h` | Errors in the last hour |\n"
        "| `avg_latency_ms_1h` | Average response latency (ms) in the last hour |\n"
        "| `uptime_sec` | API server uptime in seconds |\n"
    ),
)
async def get_stats() -> dict:
    return ApiMetrics().snapshot()


# ---------------------------------------------------------------
# Daily statistics (persisted in DB)
# ---------------------------------------------------------------

class DailyStatsRow(BaseModel):
    date: date
    ticks_received: int = 0
    ticks_flushed: int = 0
    candles_upserted: int = 0
    redis_published: int = 0
    poller_errors: int = 0
    reconnects: int = 0
    gaps_found: int = 0
    poller_uptime_sec: float = 0.0
    api_requests: int = 0
    api_errors: int = 0
    api_avg_latency_ms: float = 0.0
    api_uptime_sec: float = 0.0

    model_config = {"from_attributes": True}


@router.get(
    "/stats/daily",
    response_model=list[DailyStatsRow],
    summary="Historical daily statistics",
    description=(
        "Returns per-day aggregated statistics for both the poller and the API.\n\n"
        "Data is persisted to the database every 5 minutes and on shutdown, "
        "so counters survive restarts.\n\n"
        "| Field | Description |\n"
        "|---|---|\n"
        "| `ticks_received` | Total ticks received from MT5 that day |\n"
        "| `ticks_flushed` | Ticks written to DB |\n"
        "| `candles_upserted` | Candle upserts to DB |\n"
        "| `redis_published` | Messages published to Redis |\n"
        "| `poller_errors` | Total poller errors |\n"
        "| `reconnects` | MT5 reconnection count |\n"
        "| `gaps_found` | Data gaps detected |\n"
        "| `poller_uptime_sec` | Poller uptime (seconds) |\n"
        "| `api_requests` | Total API HTTP requests |\n"
        "| `api_errors` | API errors (5xx) |\n"
        "| `api_avg_latency_ms` | Average API response latency (ms) |\n"
        "| `api_uptime_sec` | API uptime (seconds) |\n"
    ),
)
async def get_daily_stats(
    from_dt: Optional[datetime] = Query(
        default=None,
        alias="from",
        description="Start date (ISO 8601). Inclusive.",
    ),
    to_dt: Optional[datetime] = Query(
        default=None,
        alias="to",
        description="End date (ISO 8601). Inclusive.",
    ),
    limit: int = Query(
        default=30,
        ge=1,
        le=365,
        description="Max rows to return.",
    ),
) -> list[DailyStatsRow]:
    rows = await repo.query_daily_stats(
        from_date=from_dt,
        to_date=to_dt,
        limit=limit,
    )
    result = []
    for r in rows:
        avg_lat = 0.0
        if r.get("api_latency_count", 0) > 0:
            avg_lat = round(r["api_latency_sum_ms"] / r["api_latency_count"], 2)
        result.append(DailyStatsRow(
            date=r["date"],
            ticks_received=r["ticks_received"],
            ticks_flushed=r["ticks_flushed"],
            candles_upserted=r["candles_upserted"],
            redis_published=r["redis_published"],
            poller_errors=r["poller_errors"],
            reconnects=r["reconnects"],
            gaps_found=r["gaps_found"],
            poller_uptime_sec=round(r["poller_uptime_sec"], 1),
            api_requests=r["api_requests"],
            api_errors=r["api_errors"],
            api_avg_latency_ms=avg_lat,
            api_uptime_sec=round(r["api_uptime_sec"], 1),
        ))
    return result

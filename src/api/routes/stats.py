"""
REST endpoint: ``/api/v1/stats``

Returns API request metrics — total counts, windowed counts (1h/12h/24h),
error rates, and average latency.
"""

from __future__ import annotations

from fastapi import APIRouter

from src.api.middleware.request_metrics import ApiMetrics

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

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
        "Returns request counts for 1h / 12h / 24h windows, "
        "error counts, and average latency."
    ),
)
async def get_stats() -> dict:
    return ApiMetrics().snapshot()

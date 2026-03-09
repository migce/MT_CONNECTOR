"""
REST endpoint: ``/api/v1/ticks/{symbol}``

Query raw historical tick data.

If the requested ``from`` date is before our stored ticks, an on-demand
backfill is triggered automatically via the MT5 poller.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query

from src.api.schemas import PaginatedResponse, TickResponse
from src.api.services.backfill_helper import maybe_backfill_ticks
from src.api.services.validation import backfill_limiter, validate_symbol

router = APIRouter(prefix="/api/v1", tags=["ticks"])


@router.get(
    "/ticks/{symbol}",
    response_model=PaginatedResponse[TickResponse],
    summary="Get historical ticks",
    description=(
        "Retrieve raw tick data for a given symbol. "
        "Results are ordered by time ascending. "
        "If the requested range is not yet in the database, the system "
        "automatically fetches it from MetaTrader 5.\n\n"
        "The response includes `has_more` / `next_from` pagination metadata "
        "so you can iterate through large datasets page by page."
    ),
)
async def get_ticks(
    symbol: str,
    from_dt: Optional[datetime] = Query(
        default=None,
        alias="from",
        description="Start datetime (ISO 8601). Inclusive.",
    ),
    to_dt: Optional[datetime] = Query(
        default=None,
        alias="to",
        description="End datetime (ISO 8601). Inclusive.",
    ),
    limit: int = Query(
        default=5000,
        ge=1,
        le=50000,
        description="Maximum number of ticks to return.",
    ),
) -> PaginatedResponse[TickResponse]:
    # Validate symbol exists in configuration
    symbol = validate_symbol(symbol)

    # Rate-limit backfill triggers
    await backfill_limiter.check(symbol)

    # Fetch limit + 1 to detect whether more rows exist
    rows = await maybe_backfill_ticks(
        symbol=symbol,
        dt_from=from_dt,
        dt_to=to_dt,
        limit=limit + 1,
    )

    has_more = len(rows) > limit
    if has_more:
        next_from = rows[limit]["time_msc"].isoformat()
        rows = rows[:limit]
    else:
        next_from = None

    data = [TickResponse(**r) for r in rows]
    return PaginatedResponse(
        data=data,
        count=len(data),
        has_more=has_more,
        next_from=next_from,
    )

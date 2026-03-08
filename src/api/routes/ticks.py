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

from src.api.schemas import TickResponse
from src.api.services.backfill_helper import maybe_backfill_ticks

router = APIRouter(prefix="/api/v1", tags=["ticks"])


@router.get(
    "/ticks/{symbol}",
    response_model=list[TickResponse],
    summary="Get historical ticks",
    description=(
        "Retrieve raw tick data for a given symbol. "
        "Results are ordered by time ascending. "
        "If the requested range is not yet in the database, the system "
        "automatically fetches it from MetaTrader 5."
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
        le=100000,
        description="Maximum number of ticks to return.",
    ),
) -> list[TickResponse]:
    rows = await maybe_backfill_ticks(
        symbol=symbol.upper(),
        dt_from=from_dt,
        dt_to=to_dt,
        limit=limit,
    )
    return [TickResponse(**r) for r in rows]

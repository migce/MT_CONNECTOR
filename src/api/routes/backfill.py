"""
REST endpoint: ``POST /api/v1/backfill``

Admin endpoint to explicitly trigger on-demand historical data download
from MT5 for a given symbol, timeframe, and date range.

Bypasses the automatic backfill cooldown so that operators can force
a deep historical preload.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.api.services.validation import validate_symbol
from src.config import Timeframe

router = APIRouter(prefix="/api/v1", tags=["backfill"])

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------

class BackfillRequest(BaseModel):
    symbol: str = Field(..., description="Instrument symbol (e.g. EURUSD)")
    timeframe: str = Field(
        default="M1",
        description="Candle timeframe: M1, M5, M15, H1, H4, D1",
    )
    from_dt: datetime = Field(
        ...,
        alias="from",
        description="Start datetime (ISO 8601, inclusive)",
    )
    to_dt: datetime = Field(
        ...,
        alias="to",
        description="End datetime (ISO 8601, inclusive)",
    )
    repair_from_ticks: bool = Field(
        default=False,
        description=(
            "Fill still-missing closed candles from actual source ticks after "
            "the native MT5 candle request completes. Existing candles are preserved."
        ),
    )

    model_config = {"populate_by_name": True}


class BackfillResponse(BaseModel):
    status: str
    symbol: str
    timeframe: str
    from_dt: datetime = Field(..., alias="from")
    to_dt: datetime = Field(..., alias="to")
    rows: int = 0
    error: str | None = None

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------

@router.post(
    "/backfill",
    response_model=BackfillResponse,
    summary="Trigger on-demand historical backfill",
    description=(
        "Explicitly request the MT5 poller to download historical "
        "candles for a given symbol, timeframe, and date range. "
        "Use this to preload data that is not yet in the database.\n\n"
        "The request blocks until the poller responds (up to 120 s)."
    ),
)
async def trigger_backfill(body: BackfillRequest) -> BackfillResponse:
    symbol = validate_symbol(body.symbol)

    tf_str = body.timeframe.upper()
    try:
        Timeframe(tf_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{body.timeframe}'. "
                   f"Allowed: {[t.value for t in Timeframe]}",
        )

    dt_from = body.from_dt
    dt_to = body.to_dt

    if not dt_from.tzinfo:
        dt_from = dt_from.replace(tzinfo=timezone.utc)
    if not dt_to.tzinfo:
        dt_to = dt_to.replace(tzinfo=timezone.utc)

    if dt_from >= dt_to:
        raise HTTPException(
            status_code=400,
            detail="'from' must be before 'to'.",
        )

    from src.api.app import get_backfill_requester

    requester = get_backfill_requester()
    if requester is None:
        raise HTTPException(
            status_code=503,
            detail="Backfill requester is not connected. "
                   "Is the MT5 poller running?",
        )

    logger.info(
        "admin_backfill_trigger",
        symbol=symbol,
        timeframe=tf_str,
        dt_from=str(dt_from),
        dt_to=str(dt_to),
    )

    result = await requester.request_and_wait(
        symbol=symbol,
        data_type="candles",
        dt_from=dt_from,
        dt_to=dt_to,
        timeframe=tf_str,
        timeout=120.0,
        repair_from_ticks=body.repair_from_ticks,
    )

    if result is None:
        return BackfillResponse(
            status="timeout",
            symbol=symbol,
            timeframe=tf_str,
            from_dt=dt_from,
            to_dt=dt_to,
            rows=0,
            error="Poller did not respond within 120 s.",
        )

    return BackfillResponse(
        status=result.get("status", "unknown"),
        symbol=symbol,
        timeframe=tf_str,
        from_dt=dt_from,
        to_dt=dt_to,
        rows=result.get("rows", 0),
        error=result.get("error"),
    )

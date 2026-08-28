"""
Trading API — deal history and open positions.

Read-only endpoints that expose data synced by the trader process.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import orjson
from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import (
    AccountInfoResponse,
    DealResponse,
    PaginatedResponse,
    PositionResponse,
)
from src.config import get_settings
from src.db import trading_repository as repo
from src.redis_bus.pool import get_redis_pool

router = APIRouter(prefix="/api/v1/trading", tags=["trading"])


@router.get(
    "/deals/{account_id}",
    response_model=PaginatedResponse[DealResponse],
)
async def get_deals(
    account_id: int,
    symbol: str | None = Query(None, description="Filter by symbol"),
    from_dt: datetime | None = Query(
        None,
        alias="from",
        description="Start of date range (ISO-8601). Default: 30 days ago.",
    ),
    to_dt: datetime | None = Query(
        None,
        alias="to",
        description="End of date range (ISO-8601). Default: now.",
    ),
    limit: int = Query(1000, ge=1, le=50000),
):
    """Retrieve closed deals for a trading account."""
    settings = get_settings()
    now = datetime.now(timezone.utc)
    if to_dt is None:
        to_dt = now + timedelta(hours=settings.mt5_history_lookahead_hours)
    if from_dt is None:
        from_dt = now - timedelta(days=30)

    rows = await repo.query_deals(
        account_id=account_id,
        date_from=from_dt,
        date_to=to_dt,
        symbol=symbol,
        limit=limit + 1,
    )

    has_more = len(rows) > limit
    data = rows[:limit]

    next_from = None
    if has_more and data:
        next_from = data[-1]["time"].isoformat()

    return PaginatedResponse(
        data=data,
        count=len(data),
        has_more=has_more,
        next_from=next_from,
    )


@router.get(
    "/positions/{account_id}",
    response_model=list[PositionResponse],
)
async def get_positions(
    account_id: int,
    symbol: str | None = Query(None, description="Filter by symbol"),
):
    """Retrieve currently open positions for a trading account."""
    return await repo.query_positions(
        account_id=account_id,
        symbol=symbol,
    )


@router.get(
    "/positions/{account_id}/status",
    summary="Latest successful open-position snapshot",
)
async def get_position_sync_status(account_id: int):
    """Return Trader-owned proof of a complete MT5 position poll and DB write."""
    raw = await get_redis_pool().get(f"trader:position_sync:{account_id}")
    if raw is None:
        raise HTTPException(
            status_code=503,
            detail=(
                f"No recent position snapshot for account_id={account_id}. "
                "The trader position loop may be delayed or unavailable."
            ),
        )
    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise HTTPException(status_code=503, detail="Position snapshot status is invalid.") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=503, detail="Position snapshot status is invalid.")
    return payload


@router.get(
    "/account-info/{account_id}",
    response_model=AccountInfoResponse,
    summary="Account balance / equity / margin snapshot",
)
async def get_account_info(account_id: int):
    """Return the latest balance, equity, leverage, and margin for an account."""
    info = await repo.get_account_info(account_id)
    if info is None:
        raise HTTPException(
            status_code=404,
            detail=f"No account info for account_id={account_id}. "
                   "The trader process may not have synced yet.",
        )
    return info


@router.get(
    "/account-info",
    response_model=list[AccountInfoResponse],
    summary="All accounts balance / equity / margin",
)
async def get_all_account_info():
    """Return the latest balance, equity, leverage, and margin for all accounts."""
    return await repo.get_all_account_info()

"""Protected reconciliation of Monitor-required MT5 account sessions."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from src.api.auth import require_internal_api
from src.api.routes.accounts import _notify_trader_reload
from src.api.schemas import AccountSessionDemandRequest, AccountSessionDemandResponse
from src.db import trading_repository as repo

router = APIRouter(prefix="/api/v1/internal/account-sessions", tags=["internal"])


@router.put(
    "/desired",
    response_model=AccountSessionDemandResponse,
    summary="Replace the complete Monitor-required account-session set",
)
async def replace_desired_account_sessions(
    body: AccountSessionDemandRequest,
    _internal_service: Annotated[str, Depends(require_internal_api)],
) -> AccountSessionDemandResponse:
    try:
        result = await repo.reconcile_account_session_demand(
            account_ids=body.account_ids,
            source_updated_at=body.source_updated_at,
            snapshot_id=body.snapshot_id,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    if result["applied"] and result["changed"]:
        await _notify_trader_reload()
    return AccountSessionDemandResponse(**result)

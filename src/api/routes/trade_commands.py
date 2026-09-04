"""Protected close-only trade command and broker-event APIs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from src.api.auth import require_internal_api
from src.api.schemas import (
    BrokerPositionEventResponse,
    TradeCommandCreate,
    TradeCommandDetail,
    TradeCommandResponse,
)
from src.config import get_settings
from src.db import trading_repository as repo

router = APIRouter(prefix="/api/v1", tags=["trade-commands"])
InternalCaller = Annotated[str, Depends(require_internal_api)]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _same_command(existing: dict, body: TradeCommandCreate) -> bool:
    return all((
        existing.get("account_id") == body.account_id,
        existing.get("action") == body.action,
        existing.get("position_ticket") == body.position_ticket,
        existing.get("expected_position_identifier") == body.expected_position_identifier,
        existing.get("expected_symbol") == body.expected_symbol.strip(),
        existing.get("expected_type") == body.expected_type,
        existing.get("expected_magic") == body.expected_magic,
        abs(float(existing.get("max_volume") or 0) - body.max_volume) < 1e-9,
        existing.get("reason") == body.reason.strip(),
        existing.get("correlation_id") == body.correlation_id,
    ))


@router.get("/trade-commands/readiness")
async def trade_readiness(_caller: InternalCaller):
    settings = get_settings()
    return {
        "execution_enabled": settings.trading_execution_enabled,
        "account_allowlist": sorted(settings.trading_account_allowlist),
        "mode": "close_only",
    }


@router.post(
    "/trade-commands",
    response_model=TradeCommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_trade_command(
    body: TradeCommandCreate,
    response: Response,
    caller: InternalCaller,
):
    settings = get_settings()
    if not settings.trading_execution_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Connector trading execution is disabled.",
        )
    if body.account_id not in settings.trading_account_allowlist:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account {body.account_id} is not enabled for trading execution.",
        )

    account = await repo.get_account(body.account_id)
    if account is None or not account.get("enabled"):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Enabled trading account was not found.")

    now = datetime.now(timezone.utc)
    requested_at = _utc(body.requested_at) if body.requested_at else now
    expires_at = (
        _utc(body.expires_at)
        if body.expires_at
        else requested_at + timedelta(seconds=settings.trader_command_default_ttl_sec)
    )
    if expires_at <= now:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "expires_at must be in the future.")

    row, created = await repo.create_trade_command(
        command_id=body.command_id,
        account_id=body.account_id,
        position_ticket=body.position_ticket,
        expected_position_identifier=body.expected_position_identifier,
        expected_symbol=body.expected_symbol.strip(),
        expected_type=body.expected_type,
        expected_magic=body.expected_magic,
        max_volume=body.max_volume,
        reason=body.reason.strip(),
        correlation_id=body.correlation_id,
        requested_by=caller,
        requested_at=requested_at,
        expires_at=expires_at,
    )
    if not created:
        if not _same_command(row, body):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "command_id is already bound to a different trade command.",
            )
        response.status_code = status.HTTP_200_OK
    return row


@router.get("/trade-commands/{command_id}", response_model=TradeCommandDetail)
async def get_trade_command(command_id: UUID, _caller: InternalCaller):
    row = await repo.get_trade_command(command_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Trade command was not found.")
    row["attempts"] = await repo.list_trade_attempts(command_id)
    return row


@router.get("/broker-events", response_model=list[BrokerPositionEventResponse])
async def get_broker_events(
    _caller: InternalCaller,
    after_id: int = Query(0, ge=0),
    account_id: int | None = Query(None, ge=1),
    limit: int = Query(1000, ge=1, le=5000),
):
    return await repo.query_broker_position_events(
        after_id=after_id,
        account_id=account_id,
        limit=limit,
    )

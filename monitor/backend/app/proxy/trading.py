"""
Proxy: trading routes (deals, positions, accounts).
Users can only access their bound accounts.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_current_user, require_admin
from app.db import get_session
from app.db.models import User, UserAccount
from app.proxy import get_mt5_client

router = APIRouter()


async def _get_user_account_ids(user: User, session: AsyncSession) -> set[int]:
    """Return set of MT5 account IDs the user has access to."""
    if user.role == "admin":
        return set()  # empty means "all" for admin
    result = await session.execute(
        select(UserAccount.account_id).where(UserAccount.user_id == user.id)
    )
    return {row[0] for row in result.all()}


def _check_account_access(account_id: int, allowed_ids: set[int], is_admin: bool) -> None:
    """Raise 403 if user doesn't have access to this account."""
    if is_admin:
        return  # admin sees everything
    if account_id not in allowed_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this account")


async def _forward_get(path: str, request: Request) -> JSONResponse:
    client = get_mt5_client()
    resp = await client.get(path, params=dict(request.query_params))
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


# ── Accounts list (filtered by user access) ─────────────────────────
@router.get("/accounts")
async def list_accounts(
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    client = get_mt5_client()
    resp = await client.get("/api/v1/admin/accounts")
    all_accounts = resp.json()

    if user.role == "admin":
        return JSONResponse(content=all_accounts, status_code=resp.status_code)

    # filter to user's bound accounts
    allowed = await _get_user_account_ids(user, session)
    filtered = [a for a in all_accounts if a.get("id") in allowed]
    return JSONResponse(content=filtered)


# ── Deals ────────────────────────────────────────────────────────────
@router.get("/deals/{account_id}")
async def deals(
    account_id: int,
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    allowed = await _get_user_account_ids(user, session)
    _check_account_access(account_id, allowed, user.role == "admin")
    return await _forward_get(f"/api/v1/trading/deals/{account_id}", request)


# ── Positions ────────────────────────────────────────────────────────
@router.get("/positions/{account_id}")
async def positions(
    account_id: int,
    request: Request,
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    allowed = await _get_user_account_ids(user, session)
    _check_account_access(account_id, allowed, user.role == "admin")
    return await _forward_get(f"/api/v1/trading/positions/{account_id}", request)


# ── Admin: account CRUD (proxy) ─────────────────────────────────────
@router.post("/accounts")
async def create_account(
    request: Request,
    _admin: Annotated[User, Depends(require_admin)],
):
    client = get_mt5_client()
    body = await request.json()
    resp = await client.post("/api/v1/admin/accounts", json=body)
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


@router.patch("/accounts/{account_id}")
async def update_account(
    account_id: int,
    request: Request,
    _admin: Annotated[User, Depends(require_admin)],
):
    client = get_mt5_client()
    body = await request.json()
    resp = await client.patch(f"/api/v1/admin/accounts/{account_id}", json=body)
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


@router.delete("/accounts/{account_id}")
async def delete_account(
    account_id: int,
    _admin: Annotated[User, Depends(require_admin)],
):
    client = get_mt5_client()
    resp = await client.delete(f"/api/v1/admin/accounts/{account_id}")
    return JSONResponse(content=None if resp.status_code == 204 else resp.json(), status_code=resp.status_code)

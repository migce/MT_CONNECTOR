"""
Proxy: market data routes (candles, ticks, symbols, spread, coverage).
Authenticated users get data for their bound accounts/symbols.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from app.auth.dependencies import get_current_user
from app.db.models import User
from app.proxy import get_mt5_client

router = APIRouter()


async def _forward_get(path: str, request: Request) -> JSONResponse:
    """Forward a GET to the MT5 API with original query params."""
    client = get_mt5_client()
    resp = await client.get(path, params=dict(request.query_params))
    return JSONResponse(content=resp.json(), status_code=resp.status_code)


# ── Symbols ──────────────────────────────────────────────────────────
@router.get("/symbols")
async def symbols(
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get("/api/v1/symbols", request)


# ── Candles ──────────────────────────────────────────────────────────
@router.get("/candles/{symbol}")
async def candles(
    symbol: str,
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get(f"/api/v1/candles/{symbol}", request)


@router.get("/candles/custom/{symbol}")
async def custom_candles(
    symbol: str,
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get(f"/api/v1/candles/custom/{symbol}", request)


# ── Ticks ────────────────────────────────────────────────────────────
@router.get("/ticks/{symbol}")
async def ticks(
    symbol: str,
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get(f"/api/v1/ticks/{symbol}", request)


# ── Spread ───────────────────────────────────────────────────────────
@router.get("/spread/{symbol}")
async def spread(
    symbol: str,
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get(f"/api/v1/spread/{symbol}", request)


# ── Coverage ─────────────────────────────────────────────────────────
@router.get("/coverage")
async def coverage(
    request: Request,
    _user: Annotated[User, Depends(get_current_user)],
):
    return await _forward_get("/api/v1/coverage", request)

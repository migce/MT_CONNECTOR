"""
WebSocket proxy: forwards tick/candle streams from MT5 API
with authentication and access control.
"""
from __future__ import annotations

import asyncio
import json

import websockets
from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from jose import JWTError

from app.auth import decode_access_token
from app.config import get_settings

router = APIRouter()


def _ws_url(path: str) -> str:
    """Convert MT5 API base URL to a WebSocket URL."""
    settings = get_settings()
    base = settings.mt5_api_url.replace("http://", "ws://").replace("https://", "wss://")
    return f"{base}{path}"


async def _authenticate(token: str | None) -> dict | None:
    """Validate JWT token from query param. Returns payload or None."""
    if not token:
        return None
    try:
        return decode_access_token(token)
    except JWTError:
        return None


async def _proxy_ws(client_ws: WebSocket, upstream_path: str):
    """Bidirectional WebSocket proxy."""
    url = _ws_url(upstream_path)
    try:
        async with websockets.connect(url) as upstream:
            async def client_to_upstream():
                try:
                    while True:
                        data = await client_ws.receive_text()
                        await upstream.send(data)
                except WebSocketDisconnect:
                    pass

            async def upstream_to_client():
                try:
                    async for msg in upstream:
                        await client_ws.send_text(msg)
                except websockets.ConnectionClosed:
                    pass

            await asyncio.gather(client_to_upstream(), upstream_to_client())
    except (websockets.ConnectionClosed, WebSocketDisconnect, ConnectionRefusedError):
        pass


@router.websocket("/ws/ticks/{symbol}")
async def ws_ticks(
    websocket: WebSocket,
    symbol: str,
    token: str | None = Query(default=None),
):
    payload = await _authenticate(token)
    if not payload:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    await websocket.accept()
    await _proxy_ws(websocket, f"/ws/ticks/{symbol}")


@router.websocket("/ws/candles/{symbol}/{timeframe}")
async def ws_candles(
    websocket: WebSocket,
    symbol: str,
    timeframe: str,
    token: str | None = Query(default=None),
):
    payload = await _authenticate(token)
    if not payload:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    await websocket.accept()
    await _proxy_ws(websocket, f"/ws/candles/{symbol}/{timeframe}")

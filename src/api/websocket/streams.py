"""
WebSocket endpoints for real-time data streaming.

Endpoints:
  - ``/ws/ticks/{symbol}``                — stream ticks
  - ``/ws/candles/{symbol}/{timeframe}``  — stream candle updates

Each client connection spawns a background task that reads from Redis
Pub/Sub and pushes messages to the WebSocket.
"""

from __future__ import annotations

import asyncio
from typing import Any

import orjson
import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.api.websocket.manager import ws_manager
from src.config import get_settings
from src.redis_bus.subscriber import RedisSubscriber

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["websocket"])


async def _redis_to_ws_pump(
    ws: WebSocket,
    channel: str,
    heartbeat_sec: int,
) -> None:
    """
    Subscribe to a Redis channel and forward messages to a WebSocket.

    Sends a heartbeat ping every *heartbeat_sec* seconds to keep the
    connection alive through proxies / load-balancers.
    """
    sub = RedisSubscriber()
    await sub.connect()
    await sub.subscribe(channel)

    try:
        last_ping = asyncio.get_event_loop().time()

        async for ch_name, data in sub.listen():
            try:
                await ws.send_text(orjson.dumps(data).decode())
            except Exception:
                break

            # Heartbeat
            now = asyncio.get_event_loop().time()
            if now - last_ping > heartbeat_sec:
                try:
                    await ws.send_json({"event": "ping"})
                except Exception:
                    break
                last_ping = now
    finally:
        await sub.close()


# ---------------------------------------------------------------
# /ws/ticks/{symbol}
# ---------------------------------------------------------------

@router.websocket("/ws/ticks/{symbol}")
async def ws_ticks(ws: WebSocket, symbol: str) -> None:
    """Stream raw ticks for *symbol* in real time."""
    await ws.accept()
    symbol = symbol.upper()
    channel = f"tick:{symbol}"

    await ws_manager.subscribe(channel, ws)
    logger.info("ws_tick_connected", symbol=symbol)

    settings = get_settings()
    pump_task = asyncio.create_task(
        _redis_to_ws_pump(ws, channel, settings.ws_heartbeat_sec)
    )

    try:
        # Keep the connection open; read and discard client messages
        while True:
            data = await ws.receive_text()
            # Client can send {"action":"ping"} — we reply with pong
            try:
                msg = orjson.loads(data)
                if msg.get("action") == "ping":
                    await ws.send_json({"event": "pong"})
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        pump_task.cancel()
        await ws_manager.unsubscribe(channel, ws)
        logger.info("ws_tick_disconnected", symbol=symbol)


# ---------------------------------------------------------------
# /ws/candles/{symbol}/{timeframe}
# ---------------------------------------------------------------

@router.websocket("/ws/candles/{symbol}/{timeframe}")
async def ws_candles(ws: WebSocket, symbol: str, timeframe: str) -> None:
    """Stream candle updates for *symbol* / *timeframe* in real time."""
    await ws.accept()
    symbol = symbol.upper()
    timeframe = timeframe.upper()
    channel = f"candle:{symbol}:{timeframe}"

    await ws_manager.subscribe(channel, ws)
    logger.info("ws_candle_connected", symbol=symbol, timeframe=timeframe)

    settings = get_settings()
    pump_task = asyncio.create_task(
        _redis_to_ws_pump(ws, channel, settings.ws_heartbeat_sec)
    )

    try:
        while True:
            data = await ws.receive_text()
            try:
                msg = orjson.loads(data)
                if msg.get("action") == "ping":
                    await ws.send_json({"event": "pong"})
            except Exception:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        pump_task.cancel()
        await ws_manager.unsubscribe(channel, ws)
        logger.info("ws_candle_disconnected", symbol=symbol, timeframe=timeframe)

"""
WebSocket endpoints for real-time data streaming.

Endpoints:
  - ``/ws/ticks/{symbol}``                — stream ticks
  - ``/ws/candles/{symbol}/{timeframe}``  — stream candle updates

A single shared Redis subscriber per channel fans messages out to all
connected WebSocket clients via the ConnectionManager.
"""

from __future__ import annotations

import asyncio
from typing import Any

import orjson
import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from src.api.websocket.manager import ws_manager
from src.config import Timeframe, get_settings, is_standard_timeframe, parse_custom_timeframe
from src.redis_bus.subscriber import RedisSubscriber

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["websocket"])


# ---------------------------------------------------------------
# Origin validation helper
# ---------------------------------------------------------------

def _check_origin(ws: WebSocket) -> bool:
    """
    Validate the WebSocket ``Origin`` header against configured
    CORS origins.  Returns True if allowed, False otherwise.
    """
    settings = get_settings()
    cors_raw = settings.cors_origins
    if cors_raw == "*":
        return True  # wildcard — accept all
    allowed = {o.strip().rstrip("/") for o in cors_raw.split(",") if o.strip()}
    origin = (ws.headers.get("origin") or "").rstrip("/")
    if not origin:
        return True  # no origin header (non-browser client)
    return origin in allowed


def _validate_ws_symbol(symbol: str) -> str | None:
    """Return upper-cased symbol if it's in the configured list, else None."""
    symbol = symbol.upper()
    allowed = get_settings().symbols
    return symbol if symbol in allowed else None


def _validate_ws_timeframe(tf: str) -> bool:
    """Return True if *tf* is a valid standard or custom timeframe."""
    tf = tf.upper()
    if is_standard_timeframe(tf):
        return True
    try:
        parse_custom_timeframe(tf)
        return True
    except ValueError:
        return False

# ---------------------------------------------------------------
# Shared per-channel Redis pump
# ---------------------------------------------------------------

_channel_tasks: dict[str, asyncio.Task] = {}
_channel_refcount: dict[str, int] = {}
_pump_lock = asyncio.Lock()


async def _shared_redis_pump(channel: str) -> None:
    """
    Single Redis subscriber for *channel*.  Broadcasts every message
    to all WebSocket clients registered in ws_manager.
    """
    sub = RedisSubscriber()
    await sub.connect()
    await sub.subscribe(channel)

    try:
        async for _ch_name, data in sub.listen():
            await ws_manager.broadcast(channel, data)
    except asyncio.CancelledError:
        pass
    finally:
        await sub.close()


async def _ensure_pump(channel: str) -> None:
    """Start the shared pump for *channel* if not already running."""
    async with _pump_lock:
        _channel_refcount[channel] = _channel_refcount.get(channel, 0) + 1
        if channel not in _channel_tasks or _channel_tasks[channel].done():
            _channel_tasks[channel] = asyncio.create_task(
                _shared_redis_pump(channel), name=f"pump:{channel}"
            )


async def _release_pump(channel: str) -> None:
    """Decrement ref-count; stop pump when no clients remain."""
    async with _pump_lock:
        _channel_refcount[channel] = _channel_refcount.get(channel, 1) - 1
        if _channel_refcount[channel] <= 0:
            _channel_refcount.pop(channel, None)
            task = _channel_tasks.pop(channel, None)
            if task and not task.done():
                task.cancel()


# ---------------------------------------------------------------
# Heartbeat helper
# ---------------------------------------------------------------

async def _heartbeat(ws: WebSocket, interval: int) -> None:
    """Send periodic ping frames to keep the connection alive."""
    try:
        while True:
            await asyncio.sleep(interval)
            await ws.send_json({"event": "ping"})
    except (asyncio.CancelledError, Exception):
        pass


# ---------------------------------------------------------------
# /ws/ticks/{symbol}
# ---------------------------------------------------------------

@router.websocket("/ws/ticks/{symbol}")
async def ws_ticks(ws: WebSocket, symbol: str) -> None:
    """Stream raw ticks for *symbol* in real time."""
    # Validate origin before accepting
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    # Validate symbol
    validated = _validate_ws_symbol(symbol)
    if validated is None:
        await ws.close(code=4004, reason=f"Unknown symbol: {symbol}")
        return
    symbol = validated

    await ws.accept()
    channel = f"tick:{symbol}"

    await ws_manager.subscribe(channel, ws)
    await _ensure_pump(channel)
    logger.info("ws_tick_connected", symbol=symbol)

    settings = get_settings()
    hb_task = asyncio.create_task(_heartbeat(ws, settings.ws_heartbeat_sec))

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
        hb_task.cancel()
        await ws_manager.unsubscribe(channel, ws)
        await _release_pump(channel)
        logger.info("ws_tick_disconnected", symbol=symbol)


# ---------------------------------------------------------------
# /ws/candles/{symbol}/{timeframe}
# ---------------------------------------------------------------

@router.websocket("/ws/candles/{symbol}/{timeframe}")
async def ws_candles(ws: WebSocket, symbol: str, timeframe: str) -> None:
    """Stream candle updates for *symbol* / *timeframe* in real time."""
    # Validate origin before accepting
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    # Validate symbol
    validated = _validate_ws_symbol(symbol)
    if validated is None:
        await ws.close(code=4004, reason=f"Unknown symbol: {symbol}")
        return
    symbol = validated

    # Validate timeframe
    timeframe = timeframe.upper()
    if not _validate_ws_timeframe(timeframe):
        await ws.close(code=4004, reason=f"Invalid timeframe: {timeframe}")
        return

    await ws.accept()
    channel = f"candle:{symbol}:{timeframe}"

    await ws_manager.subscribe(channel, ws)
    await _ensure_pump(channel)
    logger.info("ws_candle_connected", symbol=symbol, timeframe=timeframe)

    settings = get_settings()
    hb_task = asyncio.create_task(_heartbeat(ws, settings.ws_heartbeat_sec))

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
        hb_task.cancel()
        await ws_manager.unsubscribe(channel, ws)
        await _release_pump(channel)
        logger.info("ws_candle_disconnected", symbol=symbol, timeframe=timeframe)

"""
WebSocket connection manager.

Tracks active WebSocket connections per channel and handles
broadcasting messages from Redis to all connected clients.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

import orjson
import structlog
from fastapi import WebSocket

logger = structlog.get_logger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections grouped by logical channel.

    A channel is a string like ``"tick:EURUSD"`` or ``"candle:EURUSD:M1"``.
    """

    def __init__(self) -> None:
        # channel → set of active WebSocket connections
        self._subscriptions: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def subscribe(self, channel: str, ws: WebSocket) -> None:
        async with self._lock:
            self._subscriptions[channel].add(ws)
        logger.debug("ws_subscribe", channel=channel, clients=len(self._subscriptions[channel]))

    async def unsubscribe(self, channel: str, ws: WebSocket) -> None:
        async with self._lock:
            self._subscriptions[channel].discard(ws)
            if not self._subscriptions[channel]:
                del self._subscriptions[channel]
        logger.debug("ws_unsubscribe", channel=channel)

    async def unsubscribe_all(self, ws: WebSocket) -> None:
        """Remove *ws* from every channel it was subscribed to."""
        async with self._lock:
            empty: list[str] = []
            for channel, sockets in self._subscriptions.items():
                sockets.discard(ws)
                if not sockets:
                    empty.append(channel)
            for ch in empty:
                del self._subscriptions[ch]

    async def broadcast(self, channel: str, data: dict[str, Any]) -> None:
        """Send *data* as JSON to all subscribers of *channel*."""
        sockets = self._subscriptions.get(channel)
        if not sockets:
            return

        payload = orjson.dumps(data).decode()
        stale: list[WebSocket] = []

        for ws in list(sockets):
            try:
                await ws.send_text(payload)
            except Exception:
                stale.append(ws)

        # Clean up broken connections
        if stale:
            async with self._lock:
                for ws in stale:
                    sockets.discard(ws)

    @property
    def channel_count(self) -> int:
        return len(self._subscriptions)

    def client_count(self, channel: str) -> int:
        return len(self._subscriptions.get(channel, set()))


# Module-level singleton
ws_manager = ConnectionManager()

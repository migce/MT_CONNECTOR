"""
Redis Pub/Sub — Publisher.

The MT5 poller publishes new ticks and candle updates to Redis channels.
API WebSocket handlers subscribe to these channels and forward to clients.

Channel naming:
  - ``tick:{SYMBOL}``           — e.g. ``tick:EURUSD``
  - ``candle:{SYMBOL}:{TF}``   — e.g. ``candle:EURUSD:M1``
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from src.config import Settings, get_settings

logger = structlog.get_logger(__name__)


def _json_serializer(obj: Any) -> Any:
    """Support datetime serialization for orjson."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Cannot serialize {type(obj)}")


class RedisPublisher:
    """Publishes tick / candle events to Redis Pub/Sub channels."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Create the Redis connection."""
        self._redis = aioredis.Redis(
            host=self._settings.redis_host,
            port=self._settings.redis_port,
            password=self._settings.redis_password,
            db=self._settings.redis_db,
            decode_responses=False,
            retry_on_error=[ConnectionError, TimeoutError],
            socket_connect_timeout=5,
            socket_keepalive=True,
        )
        # Verify connectivity
        await self._redis.ping()
        logger.info(
            "redis_publisher_connected",
            host=self._settings.redis_host,
            port=self._settings.redis_port,
        )

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
            logger.info("redis_publisher_closed")

    async def publish_tick(self, symbol: str, tick: dict[str, Any]) -> None:
        """Publish a tick event to ``tick:{symbol}``."""
        if self._redis is None:
            return
        channel = f"tick:{symbol}"
        payload = orjson.dumps(tick, default=_json_serializer)
        try:
            await self._redis.publish(channel, payload)
        except Exception:
            logger.warning("redis_publish_tick_error", symbol=symbol, exc_info=True)

    async def publish_candle(
        self, symbol: str, timeframe: str, candle: dict[str, Any]
    ) -> None:
        """Publish a candle update to ``candle:{symbol}:{timeframe}``."""
        if self._redis is None:
            return
        channel = f"candle:{symbol}:{timeframe}"
        payload = orjson.dumps(candle, default=_json_serializer)
        try:
            await self._redis.publish(channel, payload)
        except Exception:
            logger.warning(
                "redis_publish_candle_error",
                symbol=symbol,
                timeframe=timeframe,
                exc_info=True,
            )

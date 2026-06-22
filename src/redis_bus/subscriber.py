"""
Redis Pub/Sub — Subscriber.

Used by the FastAPI WebSocket layer to receive real-time tick / candle
events published by the MT5 poller process.

Provides an async generator interface that yields messages from subscribed
Redis channels.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

import orjson
import redis.asyncio as aioredis
import structlog

from src.config import Settings, get_settings
from src.redis_bus.pool import get_redis_pool

logger = structlog.get_logger(__name__)


class RedisSubscriber:
    """
    Subscribe to one or more Redis Pub/Sub channels and yield parsed
    JSON messages asynchronously.

    Usage::

        sub = RedisSubscriber()
        await sub.connect()
        await sub.subscribe("tick:EURUSD")

        async for channel, msg in sub.listen():
            print(channel, msg)

        await sub.close()
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._redis: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None  # type: ignore[type-arg]

    async def connect(self) -> None:
        self._redis = get_redis_pool(self._settings)
        self._pubsub = self._redis.pubsub()
        logger.debug("redis_subscriber_connected")

    async def subscribe(self, *channels: str) -> None:
        """Subscribe to one or more channels."""
        if self._pubsub is None:
            raise RuntimeError("Call connect() first")
        await self._pubsub.subscribe(*channels)
        logger.debug("redis_subscribed", channels=channels)

    async def unsubscribe(self, *channels: str) -> None:
        if self._pubsub is not None:
            await self._pubsub.unsubscribe(*channels)

    async def listen(self) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """
        Async generator that yields ``(channel_name, parsed_message)``
        tuples indefinitely.

        Uses native Redis async iteration for minimal latency.
        """
        if self._pubsub is None:
            raise RuntimeError("Call connect() first")

        async for message in self._pubsub.listen():
            if message["type"] != "message":
                continue

            channel = (
                message["channel"].decode()
                if isinstance(message["channel"], bytes)
                else message["channel"]
            )
            try:
                data = orjson.loads(message["data"])
            except Exception:
                logger.warning("redis_subscriber_parse_error", exc_info=True)
                continue
            yield channel, data

    async def listen_raw(self) -> AsyncIterator[tuple[str, str]]:
        """
        Yield ``(channel_name, raw_payload_str)`` without JSON parsing.

        Optimal for forwarding to WebSocket clients where the message
        is already JSON-serialised by the publisher.
        """
        if self._pubsub is None:
            raise RuntimeError("Call connect() first")

        async for message in self._pubsub.listen():
            if message["type"] != "message":
                continue

            channel = (
                message["channel"].decode()
                if isinstance(message["channel"], bytes)
                else message["channel"]
            )
            raw = message["data"]
            payload = raw.decode() if isinstance(raw, bytes) else raw
            yield channel, payload

    async def close(self) -> None:
        if self._pubsub is not None:
            await self._pubsub.close()
            self._pubsub = None
        # Pool lifecycle is managed centrally; just drop the reference.
        self._redis = None
        logger.debug("redis_subscriber_closed")

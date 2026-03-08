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
        """
        if self._pubsub is None:
            raise RuntimeError("Call connect() first")

        while True:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    await asyncio.sleep(0.01)
                    continue

                if message["type"] != "message":
                    continue

                channel = (
                    message["channel"].decode()
                    if isinstance(message["channel"], bytes)
                    else message["channel"]
                )
                data = orjson.loads(message["data"])
                yield channel, data

            except asyncio.CancelledError:
                break
            except Exception:
                logger.warning("redis_subscriber_listen_error", exc_info=True)
                await asyncio.sleep(0.5)

    async def close(self) -> None:
        if self._pubsub is not None:
            await self._pubsub.close()
            self._pubsub = None
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
        logger.debug("redis_subscriber_closed")

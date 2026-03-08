"""
Redis-based on-demand backfill protocol.

Communication between the API (Docker) and the MT5 Poller (Windows):

1. **API** pushes a JSON request onto Redis list ``backfill:queue``
   and subscribes to ``backfill:done:{request_id}`` for the response.
2. **Poller** pops requests from the list, downloads the data from MT5,
   writes it to DB, and publishes a completion message on
   ``backfill:done:{request_id}``.

Request payload::

    {
        "request_id": "<uuid>",
        "symbol":     "EURUSD",
        "data_type":  "candles",      // or "ticks"
        "timeframe":  "M1",           // null for ticks
        "from":       "2024-01-01T00:00:00+00:00",
        "to":         "2024-01-15T00:00:00+00:00"
    }

Response payload::

    {
        "request_id": "<uuid>",
        "status":     "ok",           // or "error"
        "rows":       1234,
        "error":      null
    }
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from src.config import Settings, get_settings

logger = structlog.get_logger(__name__)

QUEUE_KEY = "backfill:queue"
DONE_CHANNEL_PREFIX = "backfill:done:"
# De-duplication key prefix — key alive while a request is in-flight
INFLIGHT_PREFIX = "backfill:inflight:"
INFLIGHT_TTL = 120  # seconds


def _inflight_key(symbol: str, data_type: str, timeframe: str | None) -> str:
    tf_part = timeframe or "tick"
    return f"{INFLIGHT_PREFIX}{symbol}:{data_type}:{tf_part}"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _serialize_dt(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError


def make_request(
    symbol: str,
    data_type: str,
    dt_from: datetime,
    dt_to: datetime,
    timeframe: str | None = None,
) -> dict[str, Any]:
    """Create a backfill request payload."""
    return {
        "request_id": uuid.uuid4().hex,
        "symbol": symbol,
        "data_type": data_type,
        "timeframe": timeframe,
        "from": dt_from.isoformat(),
        "to": dt_to.isoformat(),
    }


# -----------------------------------------------------------------------
# API-side: send request and wait for response
# -----------------------------------------------------------------------


class BackfillRequester:
    """
    Used by the **API** process to request on-demand backfill from the
    poller and wait for completion.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._redis: aioredis.Redis | None = None

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
        await self._redis.ping()
        logger.info("backfill_requester_connected")

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    async def request_and_wait(
        self,
        symbol: str,
        data_type: str,
        dt_from: datetime,
        dt_to: datetime,
        timeframe: str | None = None,
        timeout: float = 60.0,
    ) -> dict[str, Any] | None:
        """
        Send a backfill request and wait up to *timeout* seconds for the
        poller to respond.

        Returns the response dict, or ``None`` on timeout.
        De-duplicates identical in-flight requests.
        """
        if self._redis is None:
            logger.warning("backfill_requester_not_connected")
            return None

        inflight = _inflight_key(symbol, data_type, timeframe)

        # Check if an identical request is already in-flight
        existing_id = await self._redis.get(inflight)
        if existing_id is not None:
            # Wait for the existing request to finish
            req_id = existing_id.decode() if isinstance(existing_id, bytes) else existing_id
            logger.info("backfill_dedup_waiting", request_id=req_id)
            return await self._wait_for_response(req_id, timeout)

        req = make_request(symbol, data_type, dt_from, dt_to, timeframe)
        req_id = req["request_id"]

        # Mark in-flight (NX = only if not exists, race-safe)
        was_set = await self._redis.set(inflight, req_id, nx=True, ex=INFLIGHT_TTL)
        if not was_set:
            # Another request slipped in between our GET and SET — wait for it
            existing_id = await self._redis.get(inflight)
            if existing_id:
                rid = existing_id.decode() if isinstance(existing_id, bytes) else existing_id
                return await self._wait_for_response(rid, timeout)

        # Push request onto the queue
        payload = orjson.dumps(req, default=_serialize_dt)
        await self._redis.rpush(QUEUE_KEY, payload)
        logger.info("backfill_request_sent", request_id=req_id, symbol=symbol, data_type=data_type)

        return await self._wait_for_response(req_id, timeout)

    async def _wait_for_response(
        self, request_id: str, timeout: float
    ) -> dict[str, Any] | None:
        """Subscribe to the done channel and wait."""
        assert self._redis is not None
        channel = f"{DONE_CHANNEL_PREFIX}{request_id}"
        pubsub = self._redis.pubsub()
        try:
            await pubsub.subscribe(channel)
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    logger.warning("backfill_wait_timeout", request_id=request_id)
                    return None
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(remaining, 1.0),
                )
                if msg is None:
                    await asyncio.sleep(0.05)
                    continue
                if msg["type"] == "message":
                    data = orjson.loads(msg["data"])
                    logger.info("backfill_response_received", request_id=request_id, status=data.get("status"))
                    return data
        except Exception:
            logger.exception("backfill_wait_error", request_id=request_id)
            return None
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()


# -----------------------------------------------------------------------
# Poller-side: listen for requests and process them
# -----------------------------------------------------------------------


class BackfillListener:
    """
    Used by the **Poller** process to listen for on-demand backfill
    requests from the API and execute them.
    """

    def __init__(
        self,
        backfiller: Any,  # src.mt5.backfill.Backfiller (avoid circular import)
        settings: Settings | None = None,
    ) -> None:
        self._backfiller = backfiller
        self._settings = settings or get_settings()
        self._redis: aioredis.Redis | None = None

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
        await self._redis.ping()
        logger.info("backfill_listener_connected")

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    async def run_forever(self) -> None:
        """
        Blocking loop: BLPOP from the backfill queue, process each request,
        publish the result.
        """
        assert self._redis is not None
        logger.info("backfill_listener_started")

        while True:
            try:
                # BLPOP blocks for up to 5 s, then loops (so we can be cancelled)
                item = await self._redis.blpop(QUEUE_KEY, timeout=5)
                if item is None:
                    continue  # timeout, no request pending

                _key, raw = item
                req = orjson.loads(raw)
                await self._handle_request(req)

            except asyncio.CancelledError:
                logger.info("backfill_listener_cancelled")
                break
            except Exception:
                logger.exception("backfill_listener_error")
                await asyncio.sleep(1)

    async def _handle_request(self, req: dict[str, Any]) -> None:
        request_id = req["request_id"]
        symbol = req["symbol"]
        data_type = req["data_type"]
        timeframe = req.get("timeframe")
        dt_from = datetime.fromisoformat(req["from"])
        dt_to = datetime.fromisoformat(req["to"])

        logger.info(
            "backfill_on_demand_start",
            request_id=request_id,
            symbol=symbol,
            data_type=data_type,
            timeframe=timeframe,
        )

        response: dict[str, Any] = {"request_id": request_id, "status": "ok", "rows": 0, "error": None}

        try:
            if data_type == "candles" and timeframe:
                rows = await self._backfiller.on_demand_candles(
                    symbol, timeframe, dt_from, dt_to,
                )
            elif data_type == "ticks":
                rows = await self._backfiller.on_demand_ticks(
                    symbol, dt_from, dt_to,
                )
            else:
                raise ValueError(f"Unknown data_type={data_type}")
            response["rows"] = rows
        except Exception as exc:
            logger.exception("backfill_on_demand_error", request_id=request_id)
            response["status"] = "error"
            response["error"] = str(exc)

        # Clear in-flight marker
        inflight = _inflight_key(symbol, data_type, timeframe)
        await self._redis.delete(inflight)

        # Publish done
        channel = f"{DONE_CHANNEL_PREFIX}{request_id}"
        await self._redis.publish(channel, orjson.dumps(response))
        logger.info(
            "backfill_on_demand_done",
            request_id=request_id,
            status=response["status"],
            rows=response["rows"],
        )

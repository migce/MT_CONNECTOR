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
from datetime import UTC, datetime, timedelta
from collections.abc import Callable
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from src.config import Settings, get_settings
from src.metrics import PollerMetrics
from src.redis_bus.pool import get_redis_pool

logger = structlog.get_logger(__name__)

QUEUE_KEY = "backfill:queue"
DONE_CHANNEL_PREFIX = "backfill:done:"
# De-duplication key prefix — key alive while a request is in-flight
INFLIGHT_PREFIX = "backfill:inflight:"
INFLIGHT_TTL = 120  # seconds


class BackfillJobCancelledError(Exception):
    """Raised at a safe chunk boundary when an operator cancels a job."""


def _inflight_key(
    symbol: str,
    data_type: str,
    timeframe: str | None,
    repair_from_ticks: bool = False,
) -> str:
    tf_part = timeframe or "tick"
    repair_part = ":tick-repair" if repair_from_ticks else ""
    return f"{INFLIGHT_PREFIX}{symbol}:{data_type}:{tf_part}{repair_part}"


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
    repair_from_ticks: bool = False,
) -> dict[str, Any]:
    """Create a backfill request payload."""
    return {
        "request_id": uuid.uuid4().hex,
        "symbol": symbol,
        "data_type": data_type,
        "timeframe": timeframe,
        "from": dt_from.isoformat(),
        "to": dt_to.isoformat(),
        "repair_from_ticks": repair_from_ticks,
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
        self._redis = get_redis_pool(self._settings)
        await self._redis.ping()
        logger.info("backfill_requester_connected")

    async def close(self) -> None:
        # Pool lifecycle is managed centrally.
        self._redis = None

    async def enqueue_job(self, job: dict[str, Any]) -> str:
        """Queue a durable Symbol Management job without waiting for it."""
        if self._redis is None:
            raise RuntimeError("Backfill requester is not connected")
        request_id = uuid.uuid4().hex
        payload = {
            "request_id": request_id,
            "job_id": job["id"],
            "symbol": job["symbol"],
            "data_type": job["source_type"],
            "timeframe": job.get("source_timeframe"),
            "target_type": job["target_type"],
            "target_timeframe": job.get("timeframe"),
            "mode": job["mode"],
            "from": job["range_from"].isoformat(),
            "to": job["range_to"].isoformat(),
        }
        from src.db import symbol_management as sm
        await sm.update_job(job["id"], request_id=request_id)
        await self._redis.rpush(QUEUE_KEY, orjson.dumps(payload))
        logger.info("symbol_management_job_queued", job_id=job["id"], request_id=request_id)
        return request_id

    async def request_and_wait(
        self,
        symbol: str,
        data_type: str,
        dt_from: datetime,
        dt_to: datetime,
        timeframe: str | None = None,
        timeout: float = 60.0,
        repair_from_ticks: bool = False,
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

        inflight = _inflight_key(
            symbol,
            data_type,
            timeframe,
            repair_from_ticks,
        )

        # Check if an identical request is already in-flight
        existing_id = await self._redis.get(inflight)
        if existing_id is not None:
            # Wait for the existing request to finish
            req_id = existing_id.decode() if isinstance(existing_id, bytes) else existing_id
            logger.info("backfill_dedup_waiting", request_id=req_id)
            return await self._wait_for_response(req_id, timeout)

        req = make_request(
            symbol,
            data_type,
            dt_from,
            dt_to,
            timeframe,
            repair_from_ticks,
        )
        req_id = req["request_id"]

        # Mark in-flight (NX = only if not exists, race-safe)
        was_set = await self._redis.set(inflight, req_id, nx=True, ex=INFLIGHT_TTL)
        if not was_set:
            # Another request slipped in between our GET and SET — wait for it
            existing_id = await self._redis.get(inflight)
            if existing_id:
                rid = existing_id.decode() if isinstance(existing_id, bytes) else existing_id
                return await self._wait_for_response(rid, timeout)
            # The winner already finished and the key was deleted.
            # Retry the whole SET NX to become the new owner.
            was_set = await self._redis.set(inflight, req_id, nx=True, ex=INFLIGHT_TTL)
            if not was_set:
                # Extremely unlikely third-party race — just fall through
                # and wait for whoever won.
                existing_id = await self._redis.get(inflight)
                if existing_id:
                    rid = existing_id.decode() if isinstance(existing_id, bytes) else existing_id
                    return await self._wait_for_response(rid, timeout)
                logger.warning("backfill_dedup_race_unresolved", symbol=symbol)
                return None

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
            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
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
        fatal_history_timeout: Callable[[], None] | None = None,
    ) -> None:
        self._backfiller = backfiller
        self._settings = settings or get_settings()
        self._redis: aioredis.Redis | None = None
        self._metrics = PollerMetrics()
        self._fatal_history_timeout = fatal_history_timeout

    async def connect(self) -> None:
        self._redis = get_redis_pool(self._settings)
        await self._redis.ping()
        logger.info("backfill_listener_connected")

    async def close(self) -> None:
        # Pool lifecycle is managed centrally.
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
        import time as _time

        request_id = req["request_id"]
        job_id = req.get("job_id")
        symbol = req["symbol"]
        data_type = req["data_type"]
        timeframe = req.get("timeframe")
        repair_from_ticks = bool(req.get("repair_from_ticks", False))
        dt_from = datetime.fromisoformat(req["from"])
        dt_to = datetime.fromisoformat(req["to"])
        work_from = dt_from
        recovered_covered_to: datetime | None = None
        recovered_rows_read = 0

        if job_id:
            from src.db import symbol_management as sm
            job = await sm.get_job(job_id)
            # Redis is at-least-once here: after a poller restart the durable
            # queued rows are re-enqueued and an older message may still exist.
            # The single listener accepts only a job that is still queued.
            if job is None or job["status"] != "queued":
                return
            recovered_covered_to = job.get("covered_to")
            recovered_rows_read = max(int(job.get("rows_read") or 0), 0)
            if (
                req.get("mode") != "refresh"
                and recovered_covered_to is not None
                and dt_from <= recovered_covered_to < dt_to
            ):
                work_from = recovered_covered_to + timedelta(milliseconds=1)
            await sm.update_job(
                job_id,
                status="running",
                started_at=datetime.now(UTC),
                progress=max(float(job.get("progress") or 0), 0),
                error=None,
            )

        logger.info(
            "backfill_on_demand_start",
            request_id=request_id,
            symbol=symbol,
            data_type=data_type,
            timeframe=timeframe,
        )

        self._metrics.set_backfill_phase("on_demand", f"{symbol} {timeframe or 'ticks'}")

        response: dict[str, Any] = {"request_id": request_id, "status": "ok", "rows": 0, "error": None}
        _t0 = _time.monotonic()
        last_covered_to: datetime | None = recovered_covered_to
        last_processed_to: datetime | None = None
        last_rows_read = recovered_rows_read

        async def _job_progress(covered_to: datetime, rows: int) -> None:
            nonlocal last_covered_to, last_rows_read
            last_covered_to = covered_to
            last_rows_read = max(last_rows_read, rows)
            if not job_id:
                return
            from src.db import symbol_management as sm
            current = await sm.get_job(job_id)
            if current and current["status"] == "cancelling":
                raise BackfillJobCancelledError()
            duration = max((dt_to - dt_from).total_seconds(), 1.0)
            progress = min(max((covered_to - dt_from).total_seconds() / duration, 0.0), 0.99)
            await sm.update_job(
                job_id,
                progress=progress,
                covered_to=covered_to,
                rows_read=rows,
            )

        async def _job_scan_progress(processed_to: datetime, rows: int) -> None:
            nonlocal last_processed_to, last_rows_read
            last_processed_to = processed_to
            last_rows_read = max(last_rows_read, rows)
            if not job_id:
                return
            from src.db import symbol_management as sm
            current = await sm.get_job(job_id)
            if current and current["status"] == "cancelling":
                raise BackfillJobCancelledError()
            duration = max((dt_to - dt_from).total_seconds(), 1.0)
            progress = min(max((processed_to - dt_from).total_seconds() / duration, 0.0), 0.99)
            await sm.update_job(
                job_id,
                progress=progress,
                covered_to=last_covered_to,
                rows_read=last_rows_read,
            )

        fatal_history_timeout = False
        try:
            if data_type == "candles" and timeframe:
                candle_options: dict[str, Any] = {
                    "repair_from_ticks": repair_from_ticks,
                }
                if job_id:
                    candle_options.update({
                        "preserve_existing": req.get("mode") != "refresh",
                        "progress_callback": _job_progress,
                    })
                rows = await self._backfiller.on_demand_candles(
                    symbol, timeframe, work_from, dt_to, **candle_options
                )
            elif data_type == "ticks":
                tick_options: dict[str, Any] = {}
                if job_id:
                    tick_options.update({
                        "refresh_existing": req.get("mode") == "refresh",
                        "progress_callback": _job_progress,
                        "scan_progress_callback": _job_scan_progress,
                    })
                rows = await self._backfiller.on_demand_ticks(
                    symbol, work_from, dt_to, **tick_options
                )
            else:
                raise ValueError(f"Unknown data_type={data_type}")
            response["rows"] = rows
            if job_id and req.get("target_type") == "custom":
                from src.config import parse_custom_timeframe
                from src.db import symbol_management as sm
                target = str(req.get("target_timeframe") or "")
                parsed = parse_custom_timeframe(target)
                bindings = await sm.symbol_bindings(symbol)
                binding = next((item for item in bindings if item["timeframe"] == target), None)
                if binding and binding["enabled"] and binding["mode"] == "materialized":
                    if parsed.is_tick_bar:
                        raise ValueError("Tick bars are virtual-only")
                    materialized = await sm.materialize_timeframe(
                        symbol=symbol,
                        timeframe=target,
                        bucket_seconds=parsed.seconds,
                        source_timeframe=str(req.get("timeframe") or "M1"),
                        dt_from=dt_from,
                        dt_to=dt_to,
                        refresh=req.get("mode") == "refresh",
                    )
                    response["rows"] += materialized
            if job_id:
                from src.db import symbol_management as sm
                tolerance_seconds = 5.0
                if data_type == "candles" and timeframe:
                    from src.config import Timeframe
                    tolerance_seconds = Timeframe(timeframe).seconds
                covered = bool(
                    last_covered_to
                    and last_covered_to >= dt_to - timedelta(seconds=tolerance_seconds)
                )
                terminal_status = "succeeded" if covered else "partial"
                await sm.update_job(
                    job_id,
                    status=terminal_status,
                    progress=1 if covered else (
                        min(
                            max(
                                (
                                    (last_covered_to - dt_from).total_seconds()
                                    if last_covered_to else 0
                                ) / max((dt_to - dt_from).total_seconds(), 1),
                                0,
                            ),
                            0.99,
                        )
                    ),
                    covered_to=last_covered_to,
                    rows_read=max(last_rows_read, response["rows"], 0),
                    rows_written=max(response["rows"], 0),
                    error=None if covered else (
                        "Broker returned only part of the requested range"
                        if last_covered_to else
                        "Broker returned no data for the requested range"
                    ),
                    finished_at=datetime.now(UTC),
                )
        except BackfillJobCancelledError:
            response["status"] = "cancelled"
            if job_id:
                from src.db import symbol_management as sm
                await sm.update_job(
                    job_id,
                    status="cancelled",
                    finished_at=datetime.now(UTC),
                )
        except Exception as exc:
            from src.mt5.backfill import MT5HistoryCallTimeoutError

            logger.exception("backfill_on_demand_error", request_id=request_id)
            response["status"] = "error"
            response["error"] = str(exc)
            fatal_history_timeout = isinstance(exc, MT5HistoryCallTimeoutError)
            self._metrics.record_error("backfill")
            if job_id:
                from src.db import symbol_management as sm
                await sm.update_job(
                    job_id,
                    status="failed",
                    error=str(exc),
                    finished_at=datetime.now(UTC),
                )

        _elapsed = _time.monotonic() - _t0
        self._metrics.record_on_demand(
            symbol=symbol,
            data_type=data_type,
            timeframe=timeframe,
            rows=response["rows"],
            status=response["status"],
            elapsed_sec=_elapsed,
        )
        self._metrics.set_backfill_phase("")

        # Clear in-flight marker
        inflight = _inflight_key(
            symbol,
            data_type,
            timeframe,
            repair_from_ticks,
        )
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
        if fatal_history_timeout and self._fatal_history_timeout is not None:
            self._fatal_history_timeout()

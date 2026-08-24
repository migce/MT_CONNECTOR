"""
WebSocket endpoints for real-time data streaming.

Endpoints:
  - ``/ws/ticks/{symbol}``                — stream ticks
  - ``/ws/candles/{symbol}/{timeframe}``  — stream candle updates

Standard timeframes use a shared Redis pump.  Custom timeframes
(M2, H6, T100, I100, A100, V7M15, …) are aggregated server-side from the source
channel (M1/H1 candles or raw ticks).
"""

from __future__ import annotations

import asyncio
from typing import Any

import orjson
import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.api.websocket.aggregator import CandleAggregator, TickBarAggregator
from src.api.websocket.manager import ws_manager
from src.config import get_settings, is_standard_timeframe, parse_custom_timeframe
from src.information_bars import InformationBarBuilder, InformationBarConfig
from src.information_bars_a3c_v7 import (
    A3CV7Builder,
    a3c_v7_visual_preset_config,
)
from src.information_bars_v2 import InformationBarV2Builder, InformationBarV2Config
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
    """Return upper-cased symbol if it's available on the MT5 broker, else None."""
    from src.api.symbol_registry import is_symbol_available
    symbol = symbol.upper()
    return symbol if is_symbol_available(symbol) else None


def _validate_ws_timeframe(tf: str) -> bool:
    """Return True if *tf* is a valid standard or custom timeframe."""
    tf = tf.upper()
    if is_standard_timeframe(tf):
        return True
    try:
        parsed = parse_custom_timeframe(tf)
        if parsed.is_tick_bar:
            return parsed.tick_count >= 2
        if parsed.is_information_bar:
            return parsed.information_budget >= 2
        if parsed.is_adaptive_target_bar:
            return parsed.adaptive_target_ticks >= 2
        if parsed.is_a3c_v7_bar:
            return parsed.a3c_v7_analog_minutes in {5, 15, 30, 60}
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

    Automatically reconnects on Redis errors.
    """
    import structlog
    _logger = structlog.get_logger("ws.pump")
    retry_delay = 1.0
    max_retry_delay = 30.0

    while True:
        sub = RedisSubscriber()
        try:
            await sub.connect()
            await sub.subscribe(channel)
            retry_delay = 1.0  # reset on successful connect

            async for _ch_name, payload in sub.listen_raw():
                await ws_manager.broadcast_raw(channel, payload)

        except asyncio.CancelledError:
            break
        except Exception:
            _logger.warning(
                "redis_pump_reconnect",
                channel=channel,
                retry_in=retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)
        finally:
            try:
                await sub.close()
            except Exception:
                pass


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
    """
    Stream raw ticks for *symbol* in real time.

    Every bid/ask change is pushed as a JSON message (~50 ms resolution).
    The server sends `{"event": "ping"}` heartbeats every 30 s.
    Clients may send `{"action": "ping"}` and receive `{"event": "pong"}`.

    **Connect:** `ws://<server-ip>:9000/ws/ticks/EURUSD`

    **Message format:**
    ```json
    {"time_msc": "…", "symbol": "EURUSD", "bid": 1.0856,
     "ask": 1.0858, "last": 0.0, "volume": 0, "flags": 6}
    ```
    """
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

def _choose_source_tf(bucket_seconds: int) -> str:
    """Pick the coarsest stored TF that divides evenly into the bucket."""
    if bucket_seconds >= 3600 and bucket_seconds % 3600 == 0:
        return "H1"
    return "M1"


async def _aggregated_candle_pump(
    ws: WebSocket,
    symbol: str,
    source_channel: str,
    aggregator: CandleAggregator,
) -> None:
    """Subscribe to a source candle channel, aggregate, and forward."""
    _logger = structlog.get_logger("ws.agg_candle")
    retry_delay = 1.0
    max_retry_delay = 30.0

    while True:
        sub = RedisSubscriber()
        try:
            await sub.connect()
            await sub.subscribe(source_channel)
            retry_delay = 1.0

            async for _ch, data in sub.listen():
                completed, current = aggregator.update(data)
                if completed is not None:
                    await ws.send_text(orjson.dumps(completed).decode())
                await ws.send_text(orjson.dumps(current).decode())

        except asyncio.CancelledError:
            break
        except WebSocketDisconnect:
            break
        except Exception:
            _logger.warning(
                "agg_candle_pump_reconnect",
                channel=source_channel,
                retry_in=retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)
        finally:
            try:
                await sub.close()
            except Exception:
                pass


async def _aggregated_tick_bar_pump(
    ws: WebSocket,
    symbol: str,
    aggregator: (
        TickBarAggregator
        | InformationBarBuilder
        | InformationBarV2Builder
        | A3CV7Builder
    ),
) -> None:
    """Subscribe to ticks, aggregate fixed/adaptive event bars, and forward."""
    _logger = structlog.get_logger("ws.agg_tick")
    tick_channel = f"tick:{symbol}"
    retry_delay = 1.0
    max_retry_delay = 30.0

    while True:
        sub = RedisSubscriber()
        try:
            await sub.connect()
            await sub.subscribe(tick_channel)
            retry_delay = 1.0

            async for _ch, data in sub.listen():
                completed, current = aggregator.update(data)
                if completed is not None:
                    await ws.send_text(orjson.dumps(completed).decode())
                if current is not None and current is not completed:
                    await ws.send_text(orjson.dumps(current).decode())

        except asyncio.CancelledError:
            break
        except WebSocketDisconnect:
            break
        except Exception:
            _logger.warning(
                "agg_tick_pump_reconnect",
                retry_in=retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)
        finally:
            try:
                await sub.close()
            except Exception:
                pass


@router.websocket("/ws/candles/{symbol}/{timeframe}")
async def ws_candles(ws: WebSocket, symbol: str, timeframe: str) -> None:
    """
    Stream candle OHLCV updates for *symbol* / *timeframe* in real time.

    **Standard TFs** (M1–D1): shared Redis pump, zero overhead.
    **Custom time-based** (M2, H6, …): server aggregates from M1/H1.
    **Tick bars** (T100, T500, …): server aggregates from raw ticks.
    **Information bars** (I100, I500, …): research-only adaptive tick clock.
    **Adaptive v2 bars** (A100, A500, …): frozen target-tick clock.
    **A3C-v7 presets** (V7M5, V7M15, V7M30, V7M60): dual clock.

    Heartbeats every 30 s. Clients may send ``{"action": "ping"}``.
    """
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    validated = _validate_ws_symbol(symbol)
    if validated is None:
        await ws.close(code=4004, reason=f"Unknown symbol: {symbol}")
        return
    symbol = validated

    timeframe = timeframe.upper()
    if not _validate_ws_timeframe(timeframe):
        await ws.close(code=4004, reason=f"Invalid timeframe: {timeframe}")
        return

    await ws.accept()
    settings = get_settings()
    hb_task = asyncio.create_task(_heartbeat(ws, settings.ws_heartbeat_sec))

    try:
        if is_standard_timeframe(timeframe):
            # ---- Standard TF: shared pump (existing logic) ----
            channel = f"candle:{symbol}:{timeframe}"
            await ws_manager.subscribe(channel, ws)
            await _ensure_pump(channel)
            logger.info("ws_candle_connected", symbol=symbol, timeframe=timeframe)

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
                await ws_manager.unsubscribe(channel, ws)
                await _release_pump(channel)

        else:
            # ---- Custom TF: per-connection aggregation ----
            ctf = parse_custom_timeframe(timeframe)

            if ctf.is_tick_bar:
                agg = TickBarAggregator(ctf.tick_count, ctf.raw)
                pump_task = asyncio.create_task(
                    _aggregated_tick_bar_pump(ws, symbol, agg)
                )
            elif ctf.is_information_bar:
                agg = InformationBarBuilder(
                    InformationBarConfig(budget=ctf.information_budget)
                )
                pump_task = asyncio.create_task(
                    _aggregated_tick_bar_pump(ws, symbol, agg)
                )
            elif ctf.is_adaptive_target_bar:
                agg = InformationBarV2Builder(
                    InformationBarV2Config(
                        neutral_ticks=ctf.adaptive_target_ticks
                    )
                )
                pump_task = asyncio.create_task(
                    _aggregated_tick_bar_pump(ws, symbol, agg)
                )
            elif ctf.is_a3c_v7_bar:
                agg = A3CV7Builder(a3c_v7_visual_preset_config(ctf.raw))
                pump_task = asyncio.create_task(
                    _aggregated_tick_bar_pump(ws, symbol, agg)
                )
            else:
                source_tf = _choose_source_tf(ctf.seconds)
                source_channel = f"candle:{symbol}:{source_tf}"
                agg = CandleAggregator(ctf.seconds, ctf.raw)
                pump_task = asyncio.create_task(
                    _aggregated_candle_pump(ws, symbol, source_channel, agg)
                )

            logger.info(
                "ws_custom_candle_connected",
                symbol=symbol,
                timeframe=timeframe,
                source=(
                    "ticks"
                    if (
                        ctf.is_tick_bar
                        or ctf.is_information_bar
                        or ctf.is_adaptive_target_bar
                        or ctf.is_a3c_v7_bar
                    )
                    else source_tf
                ),
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
                try:
                    await pump_task
                except (asyncio.CancelledError, Exception):
                    pass
    finally:
        hb_task.cancel()
        logger.info("ws_candle_disconnected", symbol=symbol, timeframe=timeframe)


# ---------------------------------------------------------------
# Trading WS — DB-polling pumps
# ---------------------------------------------------------------
# The trader process writes account-info and positions to the DB
# every ~10 s.  These pumps poll the DB and push changes to
# connected WebSocket clients.  No trader-side changes required.
# ---------------------------------------------------------------

_trading_pump_tasks: dict[str, asyncio.Task] = {}
_trading_pump_refcount: dict[str, int] = {}
_trading_pump_lock = asyncio.Lock()


# Money fields in account-info that need rounding
_ACCT_MONEY_FIELDS = frozenset({
    "balance", "equity", "margin", "margin_free", "margin_level", "profit",
    "open_volume_lots",
})


async def _db_poll_account_info(account_id: int, channel: str) -> None:
    """Poll DB for account-info changes and broadcast to WS clients."""
    from src.api.digits import normalize_money
    from src.db import trading_repository as repo

    _logger = structlog.get_logger("ws.trading.account_info")
    prev: dict | None = None

    while True:
        try:
            row = await repo.get_account_info(account_id)
            if row and row != prev:
                prev = row
                # Convert datetime for JSON serialization + round money
                payload = {}
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        payload[k] = v.isoformat()
                    elif k in _ACCT_MONEY_FIELDS and isinstance(v, float):
                        payload[k] = normalize_money(v)
                    else:
                        payload[k] = v
                # Derived boolean field
                payload["has_open_positions"] = payload.get("open_positions_count", 0) > 0
                await ws_manager.broadcast(channel, payload)
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception:
            _logger.warning("db_poll_account_info_error",
                            account_id=account_id, exc_info=True)
            await asyncio.sleep(10)


# Price fields in positions that need symbol-digit rounding
_POS_PRICE_FIELDS = frozenset({
    "price_open", "price_current", "sl", "tp",
})
_POS_MONEY_FIELDS = frozenset({"swap", "profit"})


async def _db_poll_positions(account_id: int, channel: str) -> None:
    """Poll DB for position changes and broadcast to WS clients."""
    from src.api.digits import get_digits, normalize_money, normalize_price
    from src.db import trading_repository as repo

    _logger = structlog.get_logger("ws.trading.positions")
    prev_tickets: set[tuple] | None = None

    while True:
        try:
            rows = await repo.query_positions(account_id)
            # Build a fingerprint to detect changes
            fingerprint = {
                (r["ticket"], r.get("volume"), r.get("price_current"),
                 r.get("profit"), r.get("sl"), r.get("tp"))
                for r in rows
            }
            if fingerprint != prev_tickets:
                prev_tickets = fingerprint
                # Serialize positions with price normalization
                positions = []
                for r in rows:
                    symbol = r.get("symbol", "")
                    d = get_digits(symbol)
                    p = {}
                    for k, v in r.items():
                        if hasattr(v, "isoformat"):
                            p[k] = v.isoformat()
                        elif k in _POS_PRICE_FIELDS and isinstance(v, float):
                            p[k] = normalize_price(v, d)
                        elif k in _POS_MONEY_FIELDS and isinstance(v, float):
                            p[k] = normalize_money(v)
                        elif k == "volume" and isinstance(v, float):
                            p[k] = normalize_price(v, 2)
                        else:
                            p[k] = v
                    positions.append(p)
                await ws_manager.broadcast(channel, {
                    "account_id": account_id,
                    "positions": positions,
                })
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception:
            _logger.warning("db_poll_positions_error",
                            account_id=account_id, exc_info=True)
            await asyncio.sleep(10)


async def _ensure_trading_pump(
    channel: str, coro_factory: Any
) -> None:
    """Start a DB-polling pump for *channel* if not already running."""
    async with _trading_pump_lock:
        _trading_pump_refcount[channel] = (
            _trading_pump_refcount.get(channel, 0) + 1
        )
        if channel not in _trading_pump_tasks or _trading_pump_tasks[channel].done():
            _trading_pump_tasks[channel] = asyncio.create_task(
                coro_factory(), name=f"trading-pump:{channel}"
            )


async def _release_trading_pump(channel: str) -> None:
    """Decrement ref-count; stop pump when no clients remain."""
    async with _trading_pump_lock:
        _trading_pump_refcount[channel] = (
            _trading_pump_refcount.get(channel, 1) - 1
        )
        if _trading_pump_refcount[channel] <= 0:
            _trading_pump_refcount.pop(channel, None)
            task = _trading_pump_tasks.pop(channel, None)
            if task and not task.done():
                task.cancel()


# ---------------------------------------------------------------
# /ws/trading/account-info  — batch (all accounts)
# ---------------------------------------------------------------

_BATCH_ACCOUNT_INFO_CHANNEL = "trading:account-info:all"


async def _db_poll_all_account_info(channel: str) -> None:
    """Poll DB for all account-info rows and broadcast on change."""
    from src.api.digits import normalize_money
    from src.db import trading_repository as repo

    _logger = structlog.get_logger("ws.trading.account_info_all")
    prev: list[dict] | None = None

    while True:
        try:
            rows = await repo.get_all_account_info()
            if rows != prev:
                prev = rows
                accounts = []
                for row in rows:
                    payload: dict[str, Any] = {}
                    for k, v in row.items():
                        if hasattr(v, "isoformat"):
                            payload[k] = v.isoformat()
                        elif k in _ACCT_MONEY_FIELDS and isinstance(v, float):
                            payload[k] = normalize_money(v)
                        else:
                            payload[k] = v
                    payload["has_open_positions"] = payload.get("open_positions_count", 0) > 0
                    accounts.append(payload)
                await ws_manager.broadcast(channel, {
                    "event": "account_info",
                    "accounts": accounts,
                })
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception:
            _logger.warning("db_poll_all_account_info_error", exc_info=True)
            await asyncio.sleep(10)


@router.websocket("/ws/trading/account-info")
async def ws_all_account_info(ws: WebSocket) -> None:
    """
    Stream account balance / equity / margin for **all** accounts.

    The server polls the DB every ~5 s and pushes the full list on change.
    Each message includes position summary fields.
    Heartbeats every 30 s.  Clients may send ``{"action": "ping"}``.

    **Connect:** `ws://<server-ip>:9000/ws/trading/account-info`

    **Message format:**
    ```json
    {"event": "account_info", "accounts": [
      {"account_id": 1, "balance": 10000.0, "equity": 10050.5,
       "open_positions_count": 3, "open_volume_lots": 0.15,
       "has_open_positions": true, ...},
      ...
    ]}
    ```
    """
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    await ws.accept()
    channel = _BATCH_ACCOUNT_INFO_CHANNEL

    await ws_manager.subscribe(channel, ws)
    await _ensure_trading_pump(
        channel,
        lambda: _db_poll_all_account_info(channel),
    )
    logger.info("ws_all_account_info_connected")

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
        await _release_trading_pump(channel)
        logger.info("ws_all_account_info_disconnected")


# ---------------------------------------------------------------
# /ws/trading/account-info/{account_id}
# ---------------------------------------------------------------

@router.websocket("/ws/trading/account-info/{account_id}")
async def ws_account_info(ws: WebSocket, account_id: int) -> None:
    """
    Stream account balance / equity / margin updates in real time.

    The server polls the DB every ~5 s and pushes changes.
    Heartbeats every 30 s.  Clients may send ``{"action": "ping"}``.

    **Connect:** `ws://<server-ip>:9000/ws/trading/account-info/1`

    **Message format:**
    ```json
    {"account_id": 1, "balance": 10000.0, "equity": 10050.5,
     "margin": 120.0, "margin_free": 9930.5, "margin_level": 8375.4,
     "leverage": 100, "currency": "USD", "profit": 50.5,
     "name": "John", "server": "Demo", "trade_mode": 0}
    ```
    """
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    await ws.accept()
    channel = f"trading:account-info:{account_id}"

    await ws_manager.subscribe(channel, ws)
    await _ensure_trading_pump(
        channel,
        lambda: _db_poll_account_info(account_id, channel),
    )
    logger.info("ws_account_info_connected", account_id=account_id)

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
        await _release_trading_pump(channel)
        logger.info("ws_account_info_disconnected", account_id=account_id)


# ---------------------------------------------------------------
# /ws/trading/positions/{account_id}
# ---------------------------------------------------------------

@router.websocket("/ws/trading/positions/{account_id}")
async def ws_positions(ws: WebSocket, account_id: int) -> None:
    """
    Stream open position snapshots in real time.

    The server polls the DB every ~5 s and pushes on change.
    Heartbeats every 30 s.  Clients may send ``{"action": "ping"}``.

    **Connect:** `ws://<server-ip>:9000/ws/trading/positions/1`

    **Message format:**
    ```json
    {"account_id": 1, "positions": [
      {"ticket": 123456, "symbol": "EURUSD", "type": 0,
       "volume": 0.01, "price_open": 1.0856, "price_current": 1.0861,
       "profit": 5.0, "sl": 1.0800, "tp": 1.0900, ...}
    ]}
    ```
    """
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    await ws.accept()
    channel = f"trading:positions:{account_id}"

    await ws_manager.subscribe(channel, ws)
    await _ensure_trading_pump(
        channel,
        lambda: _db_poll_positions(account_id, channel),
    )
    logger.info("ws_positions_connected", account_id=account_id)

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
        await _release_trading_pump(channel)
        logger.info("ws_positions_disconnected", account_id=account_id)


# ---------------------------------------------------------------
# /ws/poller/status  — dedicated per-connection Redis poll
# ---------------------------------------------------------------

async def _poller_status_sender(
    ws: WebSocket, initial_raw: bytes | None = None
) -> None:
    """Read ``poller:status`` from Redis every ~2 s and push to *ws*.

    Uses a **dedicated** Redis connection so it never competes with
    the shared pub/sub pool for connections.
    """
    import redis.asyncio as aioredis
    from src.config import get_settings as _gs

    _logger = structlog.get_logger("ws.poller.status")
    _settings = _gs()
    conn: aioredis.Redis | None = None
    prev_raw: bytes | None = initial_raw  # skip duplicate of initial snapshot

    try:
        # Dedicated single connection — not from the shared pool
        conn = aioredis.Redis(
            host=_settings.redis_host,
            port=_settings.redis_port,
            password=_settings.redis_password,
            db=_settings.redis_db,
            decode_responses=False,
            single_connection_client=True,
            socket_connect_timeout=5,
        )

        while True:
            try:
                raw = await conn.get("poller:status")
                if raw is not None and raw != prev_raw:
                    prev_raw = raw
                    await ws.send_bytes(raw)  # already JSON
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            except Exception:
                _logger.warning("poller_status_sender_error", exc_info=True)
                await asyncio.sleep(5)
    finally:
        if conn is not None:
            await conn.aclose()


@router.websocket("/ws/poller/status")
async def ws_poller_status(ws: WebSocket) -> None:
    """
    Stream the full poller dashboard snapshot in real time.

    Behaviour:
    1. On connect — immediately sends the current snapshot.
    2. Every ~2 s — sends an updated snapshot (if changed).
    3. Every 30 s — heartbeat ``{"event": "ping"}``.

    Clients may send ``{"action": "ping"}`` to receive
    ``{"event": "pong"}``.

    **Connect:** ``ws://<server-ip>:9000/ws/poller/status``
    """
    if not _check_origin(ws):
        await ws.close(code=4003, reason="Origin not allowed")
        return

    await ws.accept()

    # ── Send initial snapshot immediately ──────────────────────
    initial_raw: bytes | None = None
    try:
        from src.redis_bus.pool import get_redis_pool
        r = get_redis_pool()
        initial_raw = await r.get("poller:status")
        if initial_raw is not None:
            await ws.send_bytes(initial_raw)
    except Exception:
        logger.warning("ws_poller_initial_snapshot_failed", exc_info=True)

    logger.info("ws_poller_status_connected")

    settings = get_settings()
    hb_task = asyncio.create_task(_heartbeat(ws, settings.ws_heartbeat_sec))
    sender_task = asyncio.create_task(_poller_status_sender(ws, initial_raw))

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
        sender_task.cancel()
        hb_task.cancel()
        logger.info("ws_poller_status_disconnected")

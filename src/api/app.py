"""
FastAPI application factory.

Creates and configures the ASGI application with:
- REST routers (candles, ticks, symbols, health)
- WebSocket routers (real-time ticks, candles)
- Startup / shutdown lifecycle hooks (DB pool, etc.)
- CORS middleware
- OpenAPI metadata
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.middleware.request_metrics import RequestMetricsMiddleware
from src.api.routes import candles, coverage, custom_candles, health, stats, symbols, ticks
from src.api.websocket import streams
from src.config import get_settings
from src.db.engine import dispose_engine, get_engine
from src.db.init_timescale import init_timescaledb
from src.logging_config import setup_logging
from src.redis_bus.backfill_manager import BackfillRequester
from src.redis_bus.pool import close_redis_pool

logger = structlog.get_logger(__name__)

# Weak reference to the app — set during lifespan, used by get_backfill_requester()
_app_ref: FastAPI | None = None


def get_backfill_requester() -> BackfillRequester | None:
    """Return the BackfillRequester stored on app.state (available after startup)."""
    if _app_ref is not None:
        return getattr(_app_ref.state, "backfill_requester", None)
    return None


# ---------------------------------------------------------------
# API daily stats flusher (runs as background asyncio task)
# ---------------------------------------------------------------

class _ApiStatsFlusher:
    """Tracks delta between flushes and UPSERT-adds API stats to daily_stats."""

    def __init__(self) -> None:
        self._last_requests = 0
        self._last_errors = 0
        self._last_latency_sum = 0.0
        self._last_latency_cnt = 0
        self._last_uptime_mono = time.monotonic()

    async def flush(self) -> None:
        from src.api.middleware.request_metrics import ApiMetrics
        from src.db import repository as repo

        m = ApiMetrics()
        now_mono = time.monotonic()

        with m._lock:
            cur_req = m.total_requests
            cur_err = m.total_errors
            cur_lat_sum = m.total_latency_sum_ms
            cur_lat_cnt = m.total_latency_count

        d_req = cur_req - self._last_requests
        d_err = cur_err - self._last_errors
        d_lat_sum = cur_lat_sum - self._last_latency_sum
        d_lat_cnt = cur_lat_cnt - self._last_latency_cnt
        d_uptime = now_mono - self._last_uptime_mono

        self._last_requests = cur_req
        self._last_errors = cur_err
        self._last_latency_sum = cur_lat_sum
        self._last_latency_cnt = cur_lat_cnt
        self._last_uptime_mono = now_mono

        if d_req == 0 and d_err == 0 and d_uptime < 1:
            return

        from datetime import datetime, timezone
        today = datetime.now(timezone.utc)

        try:
            await repo.upsert_daily_api_stats(
                today,
                api_requests=d_req,
                api_errors=d_err,
                api_latency_sum_ms=round(d_lat_sum, 2),
                api_latency_count=d_lat_cnt,
                api_uptime_sec=round(d_uptime, 1),
            )
        except Exception:
            logger.warning("daily_api_stats_flush_failed", exc_info=True)


async def _api_stats_flusher_loop(flusher: _ApiStatsFlusher) -> None:
    """Background task: flush API stats to daily_stats every 5 min."""
    while True:
        try:
            await asyncio.sleep(300.0)  # 5 min
            await flusher.flush()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("api_stats_flusher_error", exc_info=True)
            await asyncio.sleep(60.0)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Application lifecycle: startup → yield → shutdown."""
    global _app_ref
    _app_ref = app
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)
    logger.info("api_starting", version="1.0.0")

    # Warm up the DB connection pool
    get_engine(settings)

    # Ensure TimescaleDB schema exists
    try:
        await init_timescaledb()
    except Exception:
        logger.warning("timescaledb_init_skipped", exc_info=True)

    # Connect backfill requester (for on-demand MT5 downloads)
    try:
        requester = BackfillRequester(settings)
        await requester.connect()
        app.state.backfill_requester = requester
    except Exception:
        logger.warning("backfill_requester_connect_failed", exc_info=True)
        app.state.backfill_requester = None

    # Start API daily stats flusher
    api_flusher = _ApiStatsFlusher()
    flusher_task = asyncio.create_task(
        _api_stats_flusher_loop(api_flusher),
        name="api_stats_flusher",
    )

    yield

    # Shutdown

    # Final API stats flush
    try:
        await api_flusher.flush()
        logger.info("daily_api_stats_final_flush_ok")
    except Exception:
        logger.warning("daily_api_stats_final_flush_failed", exc_info=True)

    flusher_task.cancel()
    try:
        await flusher_task
    except asyncio.CancelledError:
        pass

    requester = getattr(app.state, "backfill_requester", None)
    if requester is not None:
        await requester.close()
        app.state.backfill_requester = None
    await close_redis_pool()
    await dispose_engine()
    _app_ref = None
    logger.info("api_stopped")


# ---------------------------------------------------------------------------
# OpenAPI rich description (Markdown) — rendered in Swagger UI and ReDoc
# ---------------------------------------------------------------------------

_OPENAPI_DESCRIPTION = """\
## Overview

Production-grade **REST + WebSocket** API for MetaTrader 5 market data.

* **Historical data** — candles (OHLCV) and raw ticks stored in TimescaleDB
* **Real-time streaming** — live ticks and candle updates via WebSocket
* **Custom timeframes** — build M2, M3, H6, H12, T100… on-the-fly
* **Auto-backfill** — missing data is fetched from MT5 automatically

---

## Network Access

The API listens on **`0.0.0.0:9000`** and is reachable from any machine on
the local network. Replace `localhost` with the server IP when connecting
from another computer.

| Resource | URL |
|---|---|
| REST API | `http://<server-ip>:9000/api/v1/…` |
| WebSocket ticks | `ws://<server-ip>:9000/ws/ticks/{symbol}` |
| WebSocket candles | `ws://<server-ip>:9000/ws/candles/{symbol}/{timeframe}` |
| This page | `http://<server-ip>:9000/docs` |

> **Firewall**: make sure TCP port **9000** is open for inbound connections
> in Windows Firewall.

---

## WebSocket — Real-Time Quotes

WebSocket endpoints are **not interactive** in Swagger UI. Use a WebSocket
client (browser JS, Python `websockets`, `websocat`, etc.).

### `/ws/ticks/{symbol}` — live tick stream

Every price change (bid/ask) is pushed in real time (~50 ms resolution).

**JavaScript:**
```js
const ws = new WebSocket("ws://<server-ip>:9000/ws/ticks/EURUSD");
ws.onmessage = (e) => {
  const tick = JSON.parse(e.data);
  if (tick.event === "ping") return; // heartbeat
  console.log(`EURUSD bid=${tick.bid} ask=${tick.ask}`);
};
```

**Python:**
```python
import asyncio, json, websockets

async def stream():
    uri = "ws://<server-ip>:9000/ws/ticks/EURUSD"
    async for ws in websockets.connect(uri):
        try:
            async for msg in ws:
                tick = json.loads(msg)
                if tick.get("event") == "ping":
                    continue
                print(f"bid={tick['bid']}  ask={tick['ask']}")
        except websockets.ConnectionClosed:
            continue  # auto-reconnect

asyncio.run(stream())
```

**Tick message:**
```json
{"time_msc": "…", "symbol": "EURUSD", "bid": 1.0856, "ask": 1.0858,
 "last": 0.0, "volume": 0, "flags": 6}
```

### `/ws/candles/{symbol}/{timeframe}` — live candle updates

The current (incomplete) bar updates with each new tick; a new bar appears
when the candle closes.

**JavaScript:**
```js
const ws = new WebSocket("ws://<server-ip>:9000/ws/candles/EURUSD/M1");
ws.onmessage = (e) => {
  const c = JSON.parse(e.data);
  if (c.event === "ping") return;
  console.log(`${c.timeframe} O=${c.open} H=${c.high} L=${c.low} C=${c.close}`);
};
```

**Candle message:**
```json
{"time": "…", "symbol": "EURUSD", "timeframe": "M1",
 "open": 1.0856, "high": 1.0872, "low": 1.0843, "close": 1.0861,
 "tick_volume": 85, "real_volume": 0, "spread": 1}
```

Supported timeframes: `M1 M5 M15 H1 H4 D1` and custom (`M2`, `H6`, …).

### Multiple symbols

Open **one WebSocket per symbol/timeframe** combination:
```js
["EURUSD", "GBPUSD", "XAUUSD"].forEach(sym => {
  const ws = new WebSocket(`ws://<server-ip>:9000/ws/ticks/${sym}`);
  ws.onmessage = (e) => {
    const t = JSON.parse(e.data);
    if (t.event !== "ping")
      console.log(`${t.symbol} bid=${t.bid} ask=${t.ask}`);
  };
});
```

### Heartbeat

The server sends `{"event": "ping"}` every **30 s**. Clients may also send
`{"action": "ping"}` and will receive `{"event": "pong"}`.

---

## Quick Start — `curl` Examples

```bash
# Health check
curl http://<server-ip>:9000/api/v1/health

# List symbols
curl http://<server-ip>:9000/api/v1/symbols

# H1 candles
curl "http://<server-ip>:9000/api/v1/candles/EURUSD?timeframe=H1&limit=100"

# Custom 2-minute candles
curl "http://<server-ip>:9000/api/v1/candles/custom/EURUSD?timeframe=M2&limit=50"

# 500-tick bars
curl "http://<server-ip>:9000/api/v1/candles/custom/EURUSD?timeframe=T500&price=mid&limit=200"

# Raw ticks
curl "http://<server-ip>:9000/api/v1/ticks/EURUSD?limit=1000"

# Data coverage
curl http://<server-ip>:9000/api/v1/coverage

# API stats
curl http://<server-ip>:9000/api/v1/stats

# Daily historical stats (last 30 days)
curl http://<server-ip>:9000/api/v1/stats/daily
```

---

## Python Client SDK

Built-in async and sync clients — see
[`src/api/client.py`](https://github.com/migce/MT_CONNECTOR/blob/main/src/api/client.py).

```python
from src.api.client import MT5Client, MT5ClientSync

# Async
client = MT5Client("http://<server-ip>:9000")
candles = await client.get_candles("EURUSD", "H1", limit=100)
async for tick in client.stream_ticks("EURUSD"):
    print(tick["bid"], tick["ask"])
await client.close()

# Sync
client = MT5ClientSync("http://<server-ip>:9000")
candles = client.get_candles("EURUSD", "H1", limit=100)
client.close()
```
"""

_OPENAPI_TAGS: list[dict] = [
    {
        "name": "candles",
        "description": (
            "Historical OHLCV candle data for standard timeframes "
            "(M1, M5, M15, H1, H4, D1). Missing data is auto-backfilled "
            "from MetaTrader 5."
        ),
    },
    {
        "name": "custom-candles",
        "description": (
            "Non-standard timeframe candles built on-the-fly: "
            "**Time-based** (M2, M3, H2, H6, H12, D2, W1…) from stored candles, "
            "**Tick bars** (T100, T500, T1000…) from raw ticks. "
            "Standard TFs are auto-redirected to pre-computed data."
        ),
    },
    {
        "name": "ticks",
        "description": (
            "Raw historical tick data (bid, ask, last, volume, flags). "
            "Missing data is auto-backfilled from MT5."
        ),
    },
    {
        "name": "symbols",
        "description": "List of symbols currently tracked by the connector.",
    },
    {
        "name": "health",
        "description": (
            "Liveness & readiness: MT5 connection status (relayed from "
            "the Windows poller via Redis), TimescaleDB and Redis connectivity, "
            "uptime, and active symbol count."
        ),
    },
    {
        "name": "coverage",
        "description": (
            "Data availability statistics per symbol × timeframe and ticks: "
            "first/last bar, row counts, sync timestamps. Use this to "
            "understand what historical data is loaded."
        ),
    },
    {
        "name": "stats",
        "description": (
            "API request metrics: total requests, errors, windowed counters "
            "(1 h / 12 h / 24 h) and average response latency."
        ),
    },
    {
        "name": "websocket",
        "description": (
            "**Real-time streaming** via WebSocket.\n\n"
            "- `/ws/ticks/{symbol}` — every bid/ask change\n"
            "- `/ws/candles/{symbol}/{timeframe}` — live OHLCV updates\n\n"
            "See the **WebSocket — Real-Time Quotes** section at the top "
            "of this page for connection examples and message formats."
        ),
    },
]


def create_app() -> FastAPI:
    """Build and return the FastAPI ASGI application."""

    app = FastAPI(
        title="MT5 Connector API",
        description=_OPENAPI_DESCRIPTION,
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        openapi_tags=_OPENAPI_TAGS,
        lifespan=_lifespan,
    )

    # ---- CORS ----
    # When CORS_ORIGINS env var is set, restrict to those origins;
    # otherwise allow all (development mode, no credentials).
    settings = get_settings()
    cors_origins_raw = settings.cors_origins
    if cors_origins_raw == "*":
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # ---- Request metrics middleware ----
    app.add_middleware(RequestMetricsMiddleware)

    # ---- REST routes ----
    app.include_router(custom_candles.router)  # must precede candles (path overlap)
    app.include_router(candles.router)
    app.include_router(ticks.router)
    app.include_router(symbols.router)
    app.include_router(health.router)
    app.include_router(coverage.router)
    app.include_router(stats.router)

    # ---- WebSocket routes ----
    app.include_router(streams.router)

    return app

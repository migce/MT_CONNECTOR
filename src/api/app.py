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
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

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

## Pagination

All list endpoints (candles, ticks, custom candles) return a **paginated
envelope** instead of a bare JSON array:

```json
{
  "data": [ ... ],
  "count": 1000,
  "has_more": true,
  "next_from": "2026-03-08T12:00:00+00:00"
}
```

| Field | Type | Description |
|---|---|---|
| `data` | array | Items for the current page |
| `count` | int | Number of items in `data` |
| `has_more` | bool | `true` if additional rows exist beyond the limit |
| `next_from` | string / null | ISO-8601 timestamp — pass as `from` for the next page |

### Iterating through pages

```bash
# Page 1
curl "http://<server-ip>:9000/api/v1/candles/EURUSD?timeframe=H1&limit=500"
# → { "data": [...], "has_more": true, "next_from": "2026-02-15T10:00:00+00:00" }

# Page 2 — use next_from as the `from` parameter
curl "http://<server-ip>:9000/api/v1/candles/EURUSD?timeframe=H1&limit=500&from=2026-02-15T10:00:00%2B00:00"

# Repeat until has_more is false
```

### Python SDK — automatic pagination

```python
from src.api.client import MT5Client

client = MT5Client("http://<server-ip>:9000")

# Single page
page = await client.get_candles("EURUSD", "H1", limit=1000)
candles = page["data"]
if page["has_more"]:
    page2 = await client.get_candles(
        "EURUSD", "H1", from_dt=page["next_from"], limit=1000,
    )

# All pages at once
all_candles = await client.get_all_candles("EURUSD", "H1", limit=1000)
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


# ---------------------------------------------------------------------------
# llms.txt — AI-agent-friendly API reference (plain text)
# Served at GET /llms.txt with <server-ip> auto-replaced by request host.
# ---------------------------------------------------------------------------

_LLMS_TXT = """\
# MT5 Connector API

> Production-grade REST + WebSocket API for MetaTrader 5 market data.
> Base URL: http://<server-ip>:9000
> OpenAPI spec: http://<server-ip>:9000/openapi.json
> Interactive docs: http://<server-ip>:9000/docs
> This file: http://<server-ip>:9000/llms.txt

## Authentication

None required. The API is designed for local network / private use.

## Pagination

All list endpoints (candles, ticks) return a paginated envelope:

```json
{
  "data": [...],
  "count": 1000,
  "has_more": true,
  "next_from": "2026-03-08T12:00:00+00:00"
}
```

To fetch all pages: repeat the request with `from=<next_from>` until `has_more` is `false`.

## Endpoints

### GET /api/v1/symbols
List all tracked symbols.
Response: `[{"symbol": "EURUSD", "description": "Euro vs US Dollar"}, ...]`

### GET /api/v1/candles/{symbol}
Historical OHLCV candle bars. Missing data is auto-backfilled from MT5.

Parameters:
- symbol (path, required): Instrument name, e.g. EURUSD, XAUUSD
- timeframe (query, default "M1"): M1, M5, M15, H1, H4, D1
- from (query, optional): Start datetime, ISO 8601, inclusive
- to (query, optional): End datetime, ISO 8601, inclusive
- limit (query, default 1000, max 50000): Max candles per page

Response: PaginatedResponse with CandleResponse items
```json
{
  "data": [
    {
      "time": "2026-03-01T00:00:00Z",
      "symbol": "EURUSD",
      "timeframe": "H1",
      "open": 1.0856,
      "high": 1.0872,
      "low": 1.0843,
      "close": 1.0861,
      "tick_volume": 4523,
      "real_volume": 0,
      "spread": 1
    }
  ],
  "count": 1,
  "has_more": false,
  "next_from": null
}
```

### GET /api/v1/candles/custom/{symbol}
Build candles for any timeframe on-the-fly.

Parameters:
- symbol (path, required): Instrument name
- timeframe (query, required): Custom TF string
  - Time-based: M2, M3, M7, M10, M20, M30, H2, H3, H6, H8, H12, D2, W1, ...
  - Tick bars: T100, T250, T500, T1000, ...
  - Standard TFs (M1, M5, M15, H1, H4, D1) auto-redirect to pre-computed data
- from (query, optional): Start datetime, ISO 8601
- to (query, optional): End datetime, ISO 8601
- limit (query, default 1000, max 50000): Max candles per page
- price (query, default "bid"): Price field for tick bars: bid, ask, last, mid
- include_incomplete (query, default false): Include partial last tick bar

Response: PaginatedResponse with CandleResponse items (same schema as standard candles)

### GET /api/v1/ticks/{symbol}
Raw historical tick data. Missing data is auto-backfilled from MT5.

Parameters:
- symbol (path, required): Instrument name
- from (query, optional): Start datetime, ISO 8601, inclusive
- to (query, optional): End datetime, ISO 8601, inclusive
- limit (query, default 5000, max 50000): Max ticks per page

Response: PaginatedResponse with TickResponse items
```json
{
  "data": [
    {
      "time_msc": "2026-03-08T12:34:56.789Z",
      "symbol": "EURUSD",
      "bid": 1.0856,
      "ask": 1.0858,
      "last": 0.0,
      "volume": 0,
      "flags": 6
    }
  ],
  "count": 1,
  "has_more": false,
  "next_from": null
}
```

### GET /api/v1/health
Service health check.

Response:
```json
{
  "status": "ok",
  "mt5_connected": true,
  "db_connected": true,
  "redis_connected": true,
  "uptime_sec": 3600.5,
  "symbols_active": 10,
  "version": "1.0.0"
}
```

### GET /api/v1/uptime
Service uptime summary for the last 24 hours and 30 days.
Data sourced from `service_uptime_log` hypertable (flushed every 5 min by the poller).

Response:
```json
{
  "period_24h": [
    {"service": "api",   "up_sec": 86100.0, "down_sec": 300.0, "uptime_pct": 99.65},
    {"service": "db",    "up_sec": 86400.0, "down_sec": 0.0,   "uptime_pct": 100.0},
    {"service": "mt5",   "up_sec": 85800.0, "down_sec": 600.0, "uptime_pct": 99.31},
    {"service": "redis", "up_sec": 86400.0, "down_sec": 0.0,   "uptime_pct": 100.0}
  ],
  "period_30d": [
    {"service": "api",   "up_sec": 2590000.0, "down_sec": 2000.0, "uptime_pct": 99.92},
    {"service": "db",    "up_sec": 2592000.0, "down_sec": 0.0,    "uptime_pct": 100.0},
    {"service": "mt5",   "up_sec": 2580000.0, "down_sec": 12000.0,"uptime_pct": 99.54},
    {"service": "redis", "up_sec": 2592000.0, "down_sec": 0.0,    "uptime_pct": 100.0}
  ]
}
```

### GET /api/v1/coverage
Data availability statistics per symbol, timeframe, and ticks.

Response:
```json
{
  "total_candle_rows": 394131,
  "total_tick_rows": 28562253,
  "symbols": [
    {
      "symbol": "EURUSD",
      "candles": [
        {
          "timeframe": "M1",
          "first_bar": "2026-02-06T00:00:00Z",
          "last_bar": "2026-03-07T23:59:00Z",
          "total_bars": 28800,
          "last_synced_at": "2026-03-07T23:59:00Z"
        }
      ],
      "ticks": {
        "first_tick": "2026-02-06T00:00:00.123Z",
        "last_tick": "2026-03-07T23:59:59.987Z",
        "total_ticks": 2856225,
        "last_synced_at": "2026-03-07T23:59:59.987Z"
      }
    }
  ]
}
```

### GET /api/v1/stats
API request metrics (live counters, reset on restart).

Response:
```json
{
  "total_requests": 1523,
  "total_errors": 0,
  "uptime_sec": 3600.1,
  "requests_1h": 245,
  "requests_12h": 1200,
  "requests_24h": 1523,
  "errors_1h": 0,
  "avg_latency_ms_1h": 12.34
}
```

### GET /api/v1/stats/daily
Historical daily statistics persisted to DB. Survives restarts.

Parameters:
- from (query, optional): Start date, ISO 8601
- to (query, optional): End date, ISO 8601
- limit (query, default 30, max 365): Max rows

Response:
```json
[
  {
    "date": "2026-03-09",
    "ticks_received": 5234567,
    "ticks_flushed": 5234567,
    "candles_upserted": 12345,
    "redis_published": 5246912,
    "poller_errors": 0,
    "reconnects": 0,
    "gaps_found": 2,
    "poller_uptime_sec": 86400.0,
    "api_requests": 3456,
    "api_errors": 0,
    "api_avg_latency_ms": 8.42,
    "api_uptime_sec": 86400.0
  }
]
```

## WebSocket Endpoints

### ws://<server-ip>:9000/ws/ticks/{symbol}
Real-time tick stream. Every bid/ask change is pushed (~50ms resolution).

Message format:
```json
{"time_msc": "...", "symbol": "EURUSD", "bid": 1.0856, "ask": 1.0858, "last": 0.0, "volume": 0, "flags": 6}
```

### ws://<server-ip>:9000/ws/candles/{symbol}/{timeframe}
Real-time candle updates. Current bar updates with each tick; new bar on candle close.

Timeframes: M1, M5, M15, H1, H4, D1, and custom (M2, H6, ...).

Message format:
```json
{"time": "...", "symbol": "EURUSD", "timeframe": "M1", "open": 1.0856, "high": 1.0872, "low": 1.0843, "close": 1.0861, "tick_volume": 85, "real_volume": 0, "spread": 1}
```

### Heartbeat
Server sends `{"event": "ping"}` every 30s. Client may send `{"action": "ping"}` to receive `{"event": "pong"}`.

## Common Patterns for AI Agents

### Fetch latest 100 H1 candles
```
GET /api/v1/candles/EURUSD?timeframe=H1&limit=100
```

### Fetch all M1 candles for a specific day
```
GET /api/v1/candles/EURUSD?timeframe=M1&from=2026-03-08T00:00:00Z&to=2026-03-08T23:59:59Z&limit=50000
```

### Fetch 500-tick bars with mid price
```
GET /api/v1/candles/custom/EURUSD?timeframe=T500&price=mid&limit=200
```

### Paginate through large dataset
```
# Step 1: GET /api/v1/ticks/EURUSD?from=2026-03-08T00:00:00Z&limit=5000
# Step 2: If has_more=true, GET /api/v1/ticks/EURUSD?from=<next_from>&limit=5000
# Repeat until has_more=false
```

### Check if service is healthy before querying
```
GET /api/v1/health
# Verify: status="ok", mt5_connected=true, db_connected=true
```

### Check 24h/30d uptime history
```
GET /api/v1/uptime
# Review period_24h and period_30d arrays for all services
```

### Discover available data range
```
GET /api/v1/coverage
# Check symbols[].candles[].first_bar / last_bar for available date range
```

## Error Handling

- 400: Invalid parameters (bad timeframe, symbol not found, etc.)
- 404: Not found
- 429: Rate limit exceeded (backfill throttle)
- 500: Internal server error

Error response format:
```json
{"detail": "Invalid timeframe 'X'. Allowed: ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']"}
```

## Rate Limits

Backfill triggers are rate-limited per symbol to prevent MT5 overload.
Normal queries against cached data have no rate limit.
"""


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

    # ---- AI-agent API reference (plain text) ----
    @app.get(
        "/llms.txt",
        response_class=PlainTextResponse,
        include_in_schema=False,
    )
    async def llms_txt(request: Request) -> PlainTextResponse:
        """Serve AI-agent-friendly API reference with auto-resolved host."""
        host = request.headers.get("host", "localhost:9000")
        body = _LLMS_TXT.replace("<server-ip>:9000", host).replace("<server-ip>", host.split(":")[0])
        return PlainTextResponse(body, media_type="text/plain; charset=utf-8")

    return app

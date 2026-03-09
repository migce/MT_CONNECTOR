# MT5 Connector

Production-grade MetaTrader 5 market data service. Collects historical and real-time tick/candle data from MT5, stores in TimescaleDB, and exposes a REST + WebSocket API for consuming applications.

## Architecture

```
┌──────────────────── Windows Host ────────────────────┐
│                                                       │
│  MT5 Terminal  ←──IPC──→  MT5 Poller (Python native)  │
│                           ├── tick_collector (50ms)    │
│                           ├── candle_collector (5s)    │
│                           ├── heartbeat (10s)          │
│                           ├── gap_scan (15min)         │
│                           └── writes → DB + Redis      │
└───────────────────────────┼───────────────────────────┘
                            │ TCP
┌───────────────────────────▼───────────────────────────┐
│              Docker Compose                            │
│  ┌─────────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ TimescaleDB  │  │  Redis   │  │  FastAPI + WS    │ │
│  │  (pg16)      │  │  7-alp   │  │  REST + Stream   │ │
│  │  :5435       │  │  :6379   │  │  :9000           │ │
│  └─────────────┘  └──────────┘  └──────────────────┘ │
└───────────────────────────────────────────────────────┘
```

**Why this split?** MetaTrader 5's Python package uses Windows IPC (named pipes) to communicate with the terminal — it only works on Windows. The DB, Redis, and API server run in Docker for portability.

## Quick Start

### 1. Prerequisites

- Windows 10/11 with MetaTrader 5 terminal installed and logged in
- Docker Desktop for Windows
- Python 3.11+

### 2. Configuration

```bash
cp .env.example .env
# Edit .env with your MT5 credentials, symbols, etc.
```

### 3. Start Infrastructure (Docker)

```bash
docker-compose up -d
```

This starts TimescaleDB (port 5435), Redis (port 6379), and the API server (port 9000).

### 4. Start MT5 Poller (Native Windows)

```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the poller
python -m src.poller_main
```

### 5. Install as Windows Service (Optional)

```powershell
# Requires NSSM: choco install nssm
powershell -ExecutionPolicy Bypass -File scripts\install_poller.ps1

# Control the service
nssm start MT5Poller
nssm stop MT5Poller
nssm status MT5Poller
```

## API Reference

Full OpenAPI documentation available at `http://<server-ip>:9000/docs` when the API server is running.

### Network Access

The API listens on `0.0.0.0:9000` — accessible from any machine on the local network.
Replace `localhost` with the server's IP (e.g. `192.168.1.4`) when connecting from another computer.

```
Server (Windows host):  192.168.1.4
REST API:               http://192.168.1.4:9000/api/v1/...
WebSocket ticks:        ws://192.168.1.4:9000/ws/ticks/{symbol}
WebSocket candles:      ws://192.168.1.4:9000/ws/candles/{symbol}/{timeframe}
OpenAPI docs:           http://192.168.1.4:9000/docs
```

> **Firewall**: ensure TCP port 9000 is open for inbound connections in Windows Firewall.

### REST Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/symbols` | GET | List tracked symbols |
| `/api/v1/candles/{symbol}` | GET | Historical OHLCV candles (standard TFs) |
| `/api/v1/candles/custom/{symbol}` | GET | **Custom timeframe candles** (M2, H6, T100…) |
| `/api/v1/ticks/{symbol}` | GET | Historical raw ticks |
| `/api/v1/health` | GET | Service health check (MT5 + DB + Redis status) |
| `/api/v1/coverage` | GET | Data coverage: first/last bar per symbol × timeframe |
| `/api/v1/stats` | GET | API request statistics (1h/12h/24h windows) |
| `/api/v1/stats/daily` | GET | **Historical daily statistics** (persisted, survives restarts) |

#### GET /api/v1/candles/{symbol}

| Parameter | Type | Default | Description |
|---|---|---|---|
| `timeframe` | string | `M1` | M1, M5, M15, H1, H4, D1 |
| `from` | datetime | — | Start time (ISO 8601) |
| `to` | datetime | — | End time (ISO 8601) |
| `limit` | int | 1000 | Max rows (1–50000) |

**Example:**
```bash
curl "http://localhost:9000/api/v1/candles/EURUSD?timeframe=H1&from=2026-03-01T00:00:00Z&limit=100"
```

**Response:**
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
  "has_more": true,
  "next_from": "2026-03-01T01:00:00+00:00"
}
```

#### GET /api/v1/candles/custom/{symbol}

Builds non-standard timeframe candles **on-the-fly** from stored data.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `timeframe` | string | *required* | Any custom TF (see below) |
| `from` | datetime | — | Start time (ISO 8601) |
| `to` | datetime | — | End time (ISO 8601) |
| `limit` | int | 1000 | Max rows (1–50000) |
| `price` | string | `bid` | Price for tick bars: `bid`, `ask`, `last`, `mid` |
| `include_incomplete` | bool | `false` | Include partial last tick bar |

**Supported custom timeframes:**

| Format | Examples | Source | How it works |
|---|---|---|---|
| `M<n>` | M2, M3, M7, M10, M20, M30 | M1 candles | `time_bucket` aggregation |
| `H<n>` | H2, H3, H6, H8, H12 | H1 candles | `time_bucket` aggregation |
| `D<n>` | D2, D3 | H1 candles | `time_bucket` aggregation |
| `W<n>` | W1 | H1 candles | `time_bucket` aggregation |
| `T<n>` | T100, T250, T500, T1000 | Raw ticks | Every N ticks = 1 bar |

Standard TFs (M1, M5, M15, H1, H4, D1) are **auto-redirected** to the pre-computed table.

**Examples:**
```bash
# 2-minute candles
curl "http://localhost:9000/api/v1/candles/custom/EURUSD?timeframe=M2&limit=100"

# 6-hour candles
curl "http://localhost:9000/api/v1/candles/custom/EURUSD?timeframe=H6&limit=50"

# 500-tick bars (mid price)
curl "http://localhost:9000/api/v1/candles/custom/EURUSD?timeframe=T500&price=mid&limit=200"

# 100-tick bars with time range
curl "http://localhost:9000/api/v1/candles/custom/EURUSD?timeframe=T100&from=2026-03-07T10:00:00Z&to=2026-03-07T18:00:00Z"
```

**Response (same schema as standard candles):**
```json
{
  "data": [
    {
      "time": "2026-03-07T10:00:00Z",
      "symbol": "EURUSD",
      "timeframe": "M2",
      "open": 1.0856,
      "high": 1.0872,
      "low": 1.0843,
      "close": 1.0861,
      "tick_volume": 85,
      "real_volume": 0,
      "spread": 1
    }
  ],
  "count": 1,
  "has_more": false,
  "next_from": null
}
```

#### GET /api/v1/ticks/{symbol}

| Parameter | Type | Default | Description |
|---|---|---|---|
| `from` | datetime | — | Start time (ISO 8601) |
| `to` | datetime | — | End time (ISO 8601) |
| `limit` | int | 5000 | Max rows (1–100000) |

#### GET /api/v1/health

Returns MT5 connection status (relayed from poller via Redis), DB, Redis connectivity, and uptime.

```bash
curl "http://192.168.1.4:9000/api/v1/health"
```

**Response:**
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

| Field | Description |
|---|---|
| `status` | `ok` or `degraded` (DB unreachable) |
| `mt5_connected` | `true` if the Windows poller is running and connected to MT5 terminal |
| `symbols_active` | Number of symbols being tracked |

#### GET /api/v1/coverage

Data availability per symbol: first/last bar, row counts, sync timestamps.

```bash
curl "http://192.168.1.4:9000/api/v1/coverage"
```

**Response (abridged):**
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

#### GET /api/v1/stats

API request metrics for monitoring.

```bash
curl "http://192.168.1.4:9000/api/v1/stats"
```

**Response:**
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

#### GET /api/v1/stats/daily

Historical daily statistics persisted to TimescaleDB. Counters accumulate
throughout the day (flushed every 5 min) and survive poller/API restarts.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `from` | date | — | Start date (ISO 8601) |
| `to` | date | — | End date (ISO 8601) |
| `limit` | int | 30 | Max rows (1–365) |

```bash
curl "http://192.168.1.4:9000/api/v1/stats/daily?limit=7"
```

**Response:**
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

### Pagination

All list endpoints (candles, ticks, custom candles) return a **paginated envelope**:

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
| `has_more` | bool | `true` if more rows exist beyond the limit |
| `next_from` | string / null | ISO-8601 timestamp — pass as `from` for the next page |

**Iterating pages:**
```bash
# Page 1
curl "http://localhost:9000/api/v1/candles/EURUSD?timeframe=H1&limit=500"
# → {"data": [...], "has_more": true, "next_from": "2026-02-15T10:00:00+00:00"}

# Page 2
curl "http://localhost:9000/api/v1/candles/EURUSD?timeframe=H1&limit=500&from=2026-02-15T10:00:00%2B00:00"
# Repeat until has_more is false
```

The Python client SDK provides `get_all_candles()` / `get_all_ticks()` helpers
that iterate all pages automatically.

### WebSocket Endpoints — Real-Time Quotes

Connect from **any machine** on the network to receive live market data. WebSocket connections are long-lived — the server pushes every tick/candle update as soon as it arrives from MT5.

#### /ws/ticks/{symbol} — Live Tick Stream

Every price change (bid/ask update) is delivered in real time (~50 ms polling resolution).

**JavaScript (browser or Node.js):**
```javascript
const ws = new WebSocket("ws://192.168.1.4:9000/ws/ticks/EURUSD");

ws.onopen = () => console.log("Connected to EURUSD tick stream");

ws.onmessage = (event) => {
  const tick = JSON.parse(event.data);
  if (tick.event === "ping") return; // server heartbeat — ignore
  console.log(`EURUSD  bid=${tick.bid}  ask=${tick.ask}  time=${tick.time_msc}`);
};

ws.onclose = () => {
  console.log("Disconnected, reconnecting in 3s...");
  setTimeout(() => { /* reconnect logic */ }, 3000);
};
```

**Python (websockets library):**
```python
import asyncio, json, websockets

async def stream_ticks():
    uri = "ws://192.168.1.4:9000/ws/ticks/EURUSD"
    async for ws in websockets.connect(uri):
        try:
            async for msg in ws:
                tick = json.loads(msg)
                if tick.get("event") == "ping":
                    continue
                print(f"bid={tick['bid']}  ask={tick['ask']}")
        except websockets.ConnectionClosed:
            continue  # auto-reconnect

asyncio.run(stream_ticks())
```

**Tick message format:**
```json
{
  "time_msc": "2026-03-08T12:34:56.789Z",
  "symbol": "EURUSD",
  "bid": 1.0856,
  "ask": 1.0858,
  "last": 0.0,
  "volume": 0,
  "flags": 6
}
```

The server sends `{"event": "ping"}` heartbeats every 30 seconds to keep the connection alive.

#### /ws/candles/{symbol}/{timeframe} — Live Candle Updates

Receive candle OHLCV updates in real time. The current (incomplete) bar updates with each new tick; a new bar object appears when the candle closes.

**JavaScript:**
```javascript
const ws = new WebSocket("ws://192.168.1.4:9000/ws/candles/EURUSD/M1");

ws.onmessage = (event) => {
  const candle = JSON.parse(event.data);
  if (candle.event === "ping") return;
  console.log(
    `${candle.timeframe} ${candle.time}  O=${candle.open} H=${candle.high} L=${candle.low} C=${candle.close}`
  );
};
```

**Python:**
```python
import asyncio, json, websockets

async def stream_candles():
    uri = "ws://192.168.1.4:9000/ws/candles/EURUSD/M1"
    async for ws in websockets.connect(uri):
        try:
            async for msg in ws:
                candle = json.loads(msg)
                if candle.get("event") == "ping":
                    continue
                print(f"{candle['timeframe']} O={candle['open']} H={candle['high']} "
                      f"L={candle['low']} C={candle['close']}")
        except websockets.ConnectionClosed:
            continue

asyncio.run(stream_candles())
```

**Candle message format:**
```json
{
  "time": "2026-03-08T12:34:00Z",
  "symbol": "EURUSD",
  "timeframe": "M1",
  "open": 1.0856,
  "high": 1.0872,
  "low": 1.0843,
  "close": 1.0861,
  "tick_volume": 85,
  "real_volume": 0,
  "spread": 1
}
```

Supported timeframes for candle streaming: `M1`, `M5`, `M15`, `H1`, `H4`, `D1`, and any custom TF (e.g. `M2`, `H6`).

#### Multiple Symbols

Open one WebSocket per symbol/timeframe combination:

```javascript
const symbols = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"];

symbols.forEach(sym => {
  const ws = new WebSocket(`ws://192.168.1.4:9000/ws/ticks/${sym}`);
  ws.onmessage = (e) => {
    const t = JSON.parse(e.data);
    if (t.event !== "ping")
      console.log(`${t.symbol}  bid=${t.bid}  ask=${t.ask}`);
  };
});
```

## Python Client SDK

A built-in client library for consuming the API from Python scripts, notebooks, or other services.

### Async Client

```python
from src.api.client import MT5Client

async def main():
    # Connect from any machine — use the server's IP
    client = MT5Client("http://192.168.1.4:9000")

    # Check connectivity
    health = await client.health()
    print(health)  # {"status": "ok", "mt5_connected": true, ...}

    # List tracked symbols
    symbols = await client.get_symbols()

    # Historical candles (paginated response)
    page = await client.get_candles("EURUSD", "H1", limit=100)
    candles = page["data"]       # list of candle dicts
    print(page["has_more"])      # True if more pages available
    print(page["next_from"])     # pass as from_dt to get next page

    # Auto-paginate all candles
    all_candles = await client.get_all_candles("EURUSD", "H1", limit=1000)

    # Historical ticks (paginated)
    page = await client.get_ticks("EURUSD", limit=500)
    ticks = page["data"]

    # Auto-paginate all ticks
    all_ticks = await client.get_all_ticks("EURUSD", limit=5000)

    # Real-time tick stream (runs forever)
    async for tick in client.stream_ticks("EURUSD"):
        print(f"EURUSD bid={tick['bid']} ask={tick['ask']}")

    # Real-time candle stream
    async for candle in client.stream_candles("EURUSD", "M1"):
        print(f"O={candle['open']} H={candle['high']} L={candle['low']} C={candle['close']}")

    await client.close()
```

### Sync Client (scripts / notebooks)

```python
from src.api.client import MT5ClientSync

client = MT5ClientSync("http://192.168.1.4:9000")

# Get 100 H1 candles (paginated response)
page = client.get_candles("EURUSD", "H1", limit=100)
candles = page["data"]
print(f"Got {len(candles)} candles, has_more={page['has_more']}")

# Auto-paginate all candles
all_candles = client.get_all_candles("EURUSD", "H1", limit=1000)

# Get latest ticks
page = client.get_ticks("EURUSD", limit=500)
ticks = page["data"]

# Check health
print(client.health())

client.close()
```

### Using with `requests` / `httpx` directly

No SDK needed — the API is standard REST:

```python
import requests

BASE = "http://192.168.1.4:9000/api/v1"

# Health check
r = requests.get(f"{BASE}/health")
print(r.json())

# Get M5 candles for XAUUSD (paginated response)
r = requests.get(f"{BASE}/candles/XAUUSD", params={
    "timeframe": "M5",
    "from": "2026-03-01T00:00:00Z",
    "limit": 500,
})
page = r.json()
candles = page["data"]            # list of candle dicts
has_more = page["has_more"]       # True if more pages
next_from = page["next_from"]     # use as 'from' for next page

# Iterate all pages
all_candles = []
cursor = "2026-03-01T00:00:00Z"
while True:
    r = requests.get(f"{BASE}/candles/XAUUSD", params={
        "timeframe": "M5", "from": cursor, "limit": 1000,
    })
    page = r.json()
    all_candles.extend(page["data"])
    if not page["has_more"]:
        break
    cursor = page["next_from"]

# Get tick data (also paginated)
r = requests.get(f"{BASE}/ticks/EURUSD", params={"limit": 1000})
page = r.json()
ticks = page["data"]

# Data coverage
r = requests.get(f"{BASE}/coverage")
coverage = r.json()
print(f"Total candles: {coverage['total_candle_rows']}, ticks: {coverage['total_tick_rows']}")
```

## Database Schema

### TimescaleDB Tables

**ticks** — Raw tick data (hypertable, chunked by day)
- Columns: `time_msc`, `symbol`, `bid`, `ask`, `last`, `volume`, `flags`
- Compression after 7 days (segmentby=symbol)
- Retention: 90 days (configurable)

**candles** — OHLCV bars for M1/M5/M15/H1/H4/D1 (hypertable, chunked by month)
- Columns: `time`, `symbol`, `timeframe`, `open`, `high`, `low`, `close`, `tick_volume`, `real_volume`, `spread`
- Compression after 30 days
- UPSERT on write (current bar gets updated until close)

**sync_state** — Tracks last-synced time per symbol/data_type
- Used for gap detection and backfill resumption

## Data Integrity Features

- **Automatic backfill on startup**: Downloads missing data from `last_synced_at` to now
- **Heartbeat monitoring**: Checks MT5 connection every 10s; reconnects with exponential backoff
- **Gap detection**: Scheduled scan every 15 minutes finds missing candles (market-hours aware)
- **UPSERT writes**: Candles use `ON CONFLICT DO UPDATE` — safe for the current (incomplete) bar
- **Tick deduplication**: `ON CONFLICT DO NOTHING` prevents duplicates
- **Buffered writes**: Ticks are batched (1s flush interval) for throughput
- **Graceful shutdown**: Pending writes complete before exit

## Project Structure

```
MT_Connector/
├── docker-compose.yml          # TimescaleDB + Redis + API
├── Dockerfile.api              # API server image
├── .env / .env.example         # Configuration
├── requirements.txt
├── scripts/
│   ├── init_db.sql             # TimescaleDB DDL
│   └── install_poller.ps1      # Windows service installer
└── src/
    ├── config.py               # Pydantic settings
    ├── logging_config.py       # Structured logging
    ├── poller_main.py          # MT5 poller entry point
    ├── models/                 # SQLAlchemy models
    │   ├── tick.py
    │   ├── candle.py
    │   └── sync_state.py
    ├── db/                     # Database layer
    │   ├── engine.py           # Async engine + pool
    │   ├── init_timescale.py   # Schema init
    │   └── repository.py       # CRUD operations
    ├── mt5/                    # MetaTrader 5 integration
    │   ├── connection.py       # Connect + reconnect + heartbeat
    │   ├── collector.py        # Real-time tick/candle polling
    │   ├── backfill.py         # Historical download + gap fill
    │   └── converters.py       # Shared bar/tick dict converters
    ├── redis_bus/              # Pub/Sub bridge
    │   ├── pool.py             # Shared Redis connection pool
    │   ├── publisher.py        # Poller → Redis
    │   └── subscriber.py       # Redis → WebSocket
    └── api/                    # FastAPI application
        ├── app.py              # App factory
        ├── client.py           # Python client SDK
        ├── schemas.py          # Pydantic models
        ├── middleware/
        │   └── request_metrics.py  # Per-request stats
        ├── services/
        │   ├── validation.py       # Symbol validation + rate limiter
        │   └── backfill_helper.py  # On-demand backfill logic
        ├── routes/
        │   ├── candles.py
        │   ├── custom_candles.py   # Non-standard TFs + tick bars
        │   ├── ticks.py
        │   ├── symbols.py
        │   ├── health.py
        │   ├── coverage.py         # Data coverage stats
        │   └── stats.py            # API request metrics
        └── websocket/
            ├── manager.py      # WS connection manager
            └── streams.py      # WS endpoints (ticks + candles)
```

## Environment Variables

See [.env.example](.env.example) for the full list with descriptions.

Key variables:

| Variable | Default | Description |
|---|---|---|
| `MT5_LOGIN` | — | MT5 account number |
| `MT5_PASSWORD` | — | MT5 password |
| `MT5_SERVER` | — | Broker server name |
| `SYMBOLS` | `EURUSD` | Comma-separated symbol list |
| `TIMEFRAMES` | `M1,M5,M15,H1,H4,D1` | Timeframes to store |
| `BACKFILL_DAYS` | `30` | Days of history to load on startup |
| `TICK_POLL_INTERVAL_MS` | `50` | Tick polling frequency |
| `TICK_RETENTION_DAYS` | `90` | Auto-delete ticks older than N days |

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

Full OpenAPI documentation available at `http://localhost:9000/docs` when the API server is running.

### REST Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/symbols` | GET | List tracked symbols |
| `/api/v1/candles/{symbol}` | GET | Historical OHLCV candles (standard TFs) |
| `/api/v1/candles/custom/{symbol}` | GET | **Custom timeframe candles** (M2, H6, T100…) |
| `/api/v1/ticks/{symbol}` | GET | Historical raw ticks |
| `/api/v1/health` | GET | Service health check |

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
[
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
]
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
[
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
]
```

#### GET /api/v1/ticks/{symbol}

| Parameter | Type | Default | Description |
|---|---|---|---|
| `from` | datetime | — | Start time (ISO 8601) |
| `to` | datetime | — | End time (ISO 8601) |
| `limit` | int | 5000 | Max rows (1–100000) |

### WebSocket Endpoints

#### /ws/ticks/{symbol}

Real-time tick stream. Connect via WebSocket:

```javascript
const ws = new WebSocket("ws://localhost:9000/ws/ticks/EURUSD");
ws.onmessage = (event) => {
  const tick = JSON.parse(event.data);
  console.log(tick.bid, tick.ask);
};
```

**Message format:**
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

#### /ws/candles/{symbol}/{timeframe}

Real-time candle updates (current bar updates + new bar events):

```javascript
const ws = new WebSocket("ws://localhost:9000/ws/candles/EURUSD/M1");
ws.onmessage = (event) => {
  const candle = JSON.parse(event.data);
  console.log(candle.open, candle.high, candle.low, candle.close);
};
```

## Python Client SDK

```python
from src.api.client import MT5Client

# Async usage
async def main():
    client = MT5Client("http://localhost:9000")

    # Historical candles
    candles = await client.get_candles("EURUSD", "H1", limit=100)

    # Historical ticks
    ticks = await client.get_ticks("EURUSD", limit=500)

    # Real-time tick stream
    async for tick in client.stream_ticks("EURUSD"):
        print(f"EURUSD bid={tick['bid']} ask={tick['ask']}")

    await client.close()
```

```python
from src.api.client import MT5ClientSync

# Synchronous usage (scripts, notebooks)
client = MT5ClientSync("http://localhost:9000")
candles = client.get_candles("EURUSD", "H1", limit=100)
print(f"Got {len(candles)} candles")
client.close()
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
    │   └── aggregator.py       # M1 → M5/H1/D1 aggregation
    ├── redis_bus/              # Pub/Sub bridge
    │   ├── publisher.py        # Poller → Redis
    │   └── subscriber.py       # Redis → WebSocket
    └── api/                    # FastAPI application
        ├── app.py              # App factory
        ├── client.py           # Python client SDK
        ├── schemas.py          # Pydantic models
        ├── routes/
        │   ├── candles.py
        │   ├── custom_candles.py  # Non-standard TFs + tick bars
        │   ├── ticks.py
        │   ├── symbols.py
        │   └── health.py
        └── websocket/
            ├── manager.py      # WS connection manager
            └── streams.py      # WS endpoints
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

-- ============================================================
-- MT5 Connector — TimescaleDB Schema
-- ============================================================
-- Execute this script against a fresh PostgreSQL database with
-- the TimescaleDB extension enabled.
--
-- Usage:
--   psql -h localhost -U mt5user -d mt5_data -f init_db.sql
-- ============================================================

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================
-- 1. TICKS — raw tick data (millisecond precision)
-- ============================================================
CREATE TABLE IF NOT EXISTS ticks (
    time_msc     TIMESTAMPTZ        NOT NULL,
    symbol       TEXT               NOT NULL,
    bid          DOUBLE PRECISION,
    ask          DOUBLE PRECISION,
    last         DOUBLE PRECISION,
    volume       BIGINT             DEFAULT 0,
    flags        INTEGER            DEFAULT 0
);

-- Convert to hypertable (chunk interval = 1 day)
SELECT create_hypertable(
    'ticks', 'time_msc',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Unique index to prevent duplicate ticks
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_symbol_time
    ON ticks (symbol, time_msc DESC);

-- Enable compression on tick data (after 7 days by default)
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time_msc DESC'
);

SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- ============================================================
-- 2. CANDLES — OHLCV bars for multiple timeframes
-- ============================================================
CREATE TABLE IF NOT EXISTS candles (
    time         TIMESTAMPTZ        NOT NULL,
    symbol       TEXT               NOT NULL,
    timeframe    TEXT               NOT NULL,   -- M1, M5, M15, H1, H4, D1
    open         DOUBLE PRECISION   NOT NULL,
    high         DOUBLE PRECISION   NOT NULL,
    low          DOUBLE PRECISION   NOT NULL,
    close        DOUBLE PRECISION   NOT NULL,
    tick_volume  BIGINT             NOT NULL DEFAULT 0,
    real_volume  BIGINT             DEFAULT 0,
    spread       INTEGER            DEFAULT 0
);

-- Convert to hypertable (chunk interval = 1 month)
SELECT create_hypertable(
    'candles', 'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists       => TRUE
);

-- Unique constraint for UPSERT support
CREATE UNIQUE INDEX IF NOT EXISTS idx_candles_symbol_tf_time
    ON candles (symbol, timeframe, time DESC);

-- Enable compression (after 30 days)
ALTER TABLE candles SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, timeframe',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('candles', INTERVAL '30 days', if_not_exists => TRUE);

-- ============================================================
-- 3. SYNC_STATE — tracks last-synced time per symbol/timeframe
-- ============================================================
CREATE TABLE IF NOT EXISTS sync_state (
    symbol          TEXT               NOT NULL,
    data_type       TEXT               NOT NULL,   -- 'tick' or timeframe name (M1, H1, …)
    last_synced_at  TIMESTAMPTZ        NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    last_tick_msc   BIGINT             DEFAULT 0,
    updated_at      TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, data_type)
);

-- ============================================================
-- 4. DAILY_STATS — aggregated daily metrics (poller + API)
--    One row per calendar day.  Both components UPSERT-add
--    their deltas independently, so restarts don't lose data.
-- ============================================================
CREATE TABLE IF NOT EXISTS daily_stats (
    date               DATE             NOT NULL PRIMARY KEY,

    -- Poller metrics
    ticks_received     BIGINT           NOT NULL DEFAULT 0,
    ticks_flushed      BIGINT           NOT NULL DEFAULT 0,
    candles_upserted   BIGINT           NOT NULL DEFAULT 0,
    redis_published    BIGINT           NOT NULL DEFAULT 0,
    poller_errors      INTEGER          NOT NULL DEFAULT 0,
    reconnects         INTEGER          NOT NULL DEFAULT 0,
    gaps_found         INTEGER          NOT NULL DEFAULT 0,
    poller_uptime_sec  DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- API metrics
    api_requests       BIGINT           NOT NULL DEFAULT 0,
    api_errors         BIGINT           NOT NULL DEFAULT 0,
    api_latency_sum_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    api_latency_count  BIGINT           NOT NULL DEFAULT 0,
    api_uptime_sec     DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- Meta
    updated_at         TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 5. SERVICE_UPTIME_LOG — periodic uptime / downtime snapshots
--    One row per service per flush interval (~5 min).
--    Hypertable for automatic chunking & retention.
-- ============================================================
CREATE TABLE IF NOT EXISTS service_uptime_log (
    ts        TIMESTAMPTZ        NOT NULL,
    service   TEXT               NOT NULL,   -- 'mt5' | 'db' | 'redis' | 'api'
    up_sec    DOUBLE PRECISION   NOT NULL DEFAULT 0,
    down_sec  DOUBLE PRECISION   NOT NULL DEFAULT 0
);

SELECT create_hypertable(
    'service_uptime_log', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_uptime_service_ts
    ON service_uptime_log (service, ts DESC);

-- Compress after 7 days
ALTER TABLE service_uptime_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'service',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('service_uptime_log', INTERVAL '7 days', if_not_exists => TRUE);

-- Auto-drop after 90 days
SELECT add_retention_policy('service_uptime_log', INTERVAL '90 days', if_not_exists => TRUE);

-- ============================================================
-- 6. RETENTION POLICY — auto-drop old raw ticks (configurable)
--    Default: 90 days.  Candles are kept indefinitely.
-- ============================================================
SELECT add_retention_policy('ticks', INTERVAL '90 days', if_not_exists => TRUE);

-- ============================================================
-- 5. Helper: generate gap-detection series
--    Use with:
--      SELECT gap_start FROM generate_series(from, to, interval)
--      LEFT JOIN candles …  WHERE candles.time IS NULL
-- ============================================================

-- Done.  Run migrations via Alembic for schema evolution.

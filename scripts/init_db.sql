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
-- 4. RETENTION POLICY — auto-drop old raw ticks (configurable)
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

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
-- 7. TRADING_ACCOUNTS — MT5 accounts managed via admin API
--    The poller's "system" account is in .env, not here.
-- ============================================================
CREATE TABLE IF NOT EXISTS trading_accounts (
    id            SERIAL             PRIMARY KEY,
    label         TEXT               NOT NULL UNIQUE,
    description   VARCHAR(255)       NULL,
    mt5_login     INTEGER            NOT NULL UNIQUE,
    mt5_password  TEXT               NOT NULL,
    mt5_server    TEXT               NOT NULL,
    mt5_path      TEXT               NOT NULL DEFAULT 'C:\Program Files\MetaTrader 5\terminal64.exe',
    enabled       BOOLEAN            NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 8. DEALS — closed deal history (from mt5.history_deals_get)
-- ============================================================
CREATE TABLE IF NOT EXISTS deals (
    ticket        BIGINT             PRIMARY KEY,
    account_id    INTEGER            NOT NULL REFERENCES trading_accounts(id),
    "order"       BIGINT             NOT NULL,
    time          TIMESTAMPTZ        NOT NULL,
    time_msc      BIGINT             NOT NULL,
    type          INTEGER            NOT NULL,
    entry         INTEGER            NOT NULL,
    magic         BIGINT             DEFAULT 0,
    position_id   BIGINT             DEFAULT 0,
    reason        INTEGER            DEFAULT 0,
    symbol        TEXT               NOT NULL,
    volume        DOUBLE PRECISION   NOT NULL DEFAULT 0,
    price         DOUBLE PRECISION   NOT NULL DEFAULT 0,
    commission    DOUBLE PRECISION   DEFAULT 0,
    swap          DOUBLE PRECISION   DEFAULT 0,
    profit        DOUBLE PRECISION   NOT NULL DEFAULT 0,
    fee           DOUBLE PRECISION   DEFAULT 0,
    comment       TEXT               DEFAULT '',
    external_id   TEXT               DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_deals_account_time
    ON deals (account_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_deals_account_symbol
    ON deals (account_id, symbol);

CREATE INDEX IF NOT EXISTS idx_deals_position
    ON deals (position_id);

-- ============================================================
-- 9. POSITIONS — open position snapshots (replaced each sync)
-- ============================================================
CREATE TABLE IF NOT EXISTS positions (
    ticket         BIGINT            PRIMARY KEY,
    account_id     INTEGER           NOT NULL REFERENCES trading_accounts(id),
    time           TIMESTAMPTZ       NOT NULL,
    time_update    TIMESTAMPTZ,
    type           INTEGER           NOT NULL,
    magic          BIGINT            DEFAULT 0,
    identifier     BIGINT            DEFAULT 0,
    reason         INTEGER           DEFAULT 0,
    symbol         TEXT              NOT NULL,
    volume         DOUBLE PRECISION  NOT NULL DEFAULT 0,
    price_open     DOUBLE PRECISION  NOT NULL DEFAULT 0,
    price_current  DOUBLE PRECISION  NOT NULL DEFAULT 0,
    sl             DOUBLE PRECISION  DEFAULT 0,
    tp             DOUBLE PRECISION  DEFAULT 0,
    swap           DOUBLE PRECISION  DEFAULT 0,
    profit         DOUBLE PRECISION  NOT NULL DEFAULT 0,
    comment        TEXT              DEFAULT '',
    external_id    TEXT              DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_positions_account
    ON positions (account_id);

-- ============================================================
-- 10. ACCOUNT_INFO — balance / equity / margin snapshots
--     One row per account, upserted every sync cycle.
-- ============================================================
CREATE TABLE IF NOT EXISTS account_info (
    account_id        INTEGER          PRIMARY KEY REFERENCES trading_accounts(id),
    balance           DOUBLE PRECISION NOT NULL DEFAULT 0,
    equity            DOUBLE PRECISION NOT NULL DEFAULT 0,
    margin            DOUBLE PRECISION NOT NULL DEFAULT 0,
    margin_free       DOUBLE PRECISION NOT NULL DEFAULT 0,
    margin_level      DOUBLE PRECISION NOT NULL DEFAULT 0,
    leverage          INTEGER          NOT NULL DEFAULT 0,
    currency          TEXT             NOT NULL DEFAULT 'USD',
    profit            DOUBLE PRECISION NOT NULL DEFAULT 0,
    name              TEXT             NOT NULL DEFAULT '',
    server            TEXT             NOT NULL DEFAULT '',
    trade_mode        INTEGER          NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Helper: generate gap-detection series
--    Use with:
--      SELECT gap_start FROM generate_series(from, to, interval)
--      LEFT JOIN candles …  WHERE candles.time IS NULL
-- ============================================================

-- Done.  Run migrations via Alembic for schema evolution.

-- ============================================================
-- Idempotent migrations (safe to re-run on existing DBs)
-- ============================================================
ALTER TABLE trading_accounts
    ADD COLUMN IF NOT EXISTS description VARCHAR(255) NULL;

-- ============================================================
-- 11. TRADE COMMANDS — durable, idempotent close-only execution
-- ============================================================
CREATE TABLE IF NOT EXISTS trade_commands (
    id                           UUID             PRIMARY KEY,
    account_id                   INTEGER          NOT NULL REFERENCES trading_accounts(id),
    action                       TEXT             NOT NULL CHECK (action = 'close_position'),
    status                       TEXT             NOT NULL DEFAULT 'accepted',
    position_ticket              BIGINT           NOT NULL,
    expected_position_identifier BIGINT,
    expected_symbol              TEXT             NOT NULL,
    expected_type                INTEGER          NOT NULL CHECK (expected_type IN (0, 1)),
    expected_magic               BIGINT,
    max_volume                   DOUBLE PRECISION NOT NULL CHECK (max_volume > 0),
    reason                       TEXT             NOT NULL,
    correlation_id               TEXT,
    requested_by                 TEXT             NOT NULL,
    requested_at                 TIMESTAMPTZ      NOT NULL,
    expires_at                   TIMESTAMPTZ,
    next_attempt_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    attempt_count                INTEGER          NOT NULL DEFAULT 0,
    claimed_at                   TIMESTAMPTZ,
    submitted_at                 TIMESTAMPTZ,
    completed_at                 TIMESTAMPTZ,
    last_error                   TEXT,
    result                       JSONB             NOT NULL DEFAULT '{}'::jsonb,
    created_at                   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trade_commands_dispatch
    ON trade_commands (status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_trade_commands_account_created
    ON trade_commands (account_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_commands_account_id
    ON trade_commands (account_id, id);

CREATE TABLE IF NOT EXISTS trade_attempts (
    id             BIGSERIAL        PRIMARY KEY,
    command_id     UUID             NOT NULL REFERENCES trade_commands(id) ON DELETE CASCADE,
    attempt_no     INTEGER          NOT NULL,
    phase          TEXT             NOT NULL,
    retcode        INTEGER,
    message        TEXT,
    request_payload JSONB           NOT NULL DEFAULT '{}'::jsonb,
    result_payload JSONB            NOT NULL DEFAULT '{}'::jsonb,
    started_at     TIMESTAMPTZ      NOT NULL,
    finished_at    TIMESTAMPTZ      NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trade_attempts_command
    ON trade_attempts (command_id, attempt_no, id);

-- ============================================================
-- 12. BROKER POSITION EVENTS — durable lifecycle cursor for consumers
-- ============================================================
CREATE TABLE IF NOT EXISTS broker_position_events (
    id                  BIGSERIAL        PRIMARY KEY,
    dedupe_key          TEXT             NOT NULL UNIQUE,
    account_id          INTEGER          NOT NULL REFERENCES trading_accounts(id),
    event_type          TEXT             NOT NULL,
    position_ticket     BIGINT           NOT NULL,
    position_identifier BIGINT,
    symbol              TEXT             NOT NULL,
    position_type       INTEGER          NOT NULL,
    magic               BIGINT,
    volume_before       DOUBLE PRECISION NOT NULL,
    volume_after        DOUBLE PRECISION NOT NULL,
    event_time          TIMESTAMPTZ      NOT NULL,
    event_time_msc      BIGINT,
    close_deal_ticket   BIGINT,
    payload             JSONB            NOT NULL DEFAULT '{}'::jsonb,
    observed_at         TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_broker_position_events_cursor
    ON broker_position_events (account_id, id);
CREATE INDEX IF NOT EXISTS idx_broker_position_events_time
    ON broker_position_events (account_id, event_time DESC);

-- ============================================================
-- 13. TRADING ACCOUNT SESSION DEMAND — Monitor-owned desired set
-- ============================================================
ALTER TABLE trading_accounts
    ADD COLUMN IF NOT EXISTS session_required BOOLEAN NULL;

CREATE TABLE IF NOT EXISTS trading_account_session_demand (
    singleton_id        SMALLINT         PRIMARY KEY DEFAULT 1 CHECK (singleton_id = 1),
    source_updated_at   TIMESTAMPTZ      NOT NULL,
    desired_account_ids INTEGER[]        NOT NULL DEFAULT '{}',
    snapshot_id         TEXT             NOT NULL,
    applied_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

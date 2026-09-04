"""Persistent Symbol Management state and coverage queries.

The tables in this module deliberately live in the Connector database: the
poller is the owner of symbol collection and historical backfill execution.
All DDL is idempotent so API and poller startup may race safely.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from src.db.engine import get_engine, get_session_factory

STANDARD_TIMEFRAMES = ("M1", "M5", "M15", "H1", "H4", "D1")
TERMINAL_JOB_STATUSES = ("succeeded", "partial", "failed", "cancelled")
ACTIVE_JOB_STATUSES = ("queued", "running", "cancelling")


async def ensure_schema(configured_symbols: list[str] | None = None) -> None:
    statements = (
        """
        CREATE TABLE IF NOT EXISTS managed_symbols (
            symbol TEXT PRIMARY KEY,
            description TEXT NOT NULL DEFAULT '',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            source TEXT NOT NULL DEFAULT 'connector',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS custom_timeframes (
            code TEXT PRIMARY KEY,
            unit TEXT NOT NULL CHECK (unit IN ('M','H','D','W','T')),
            value INTEGER NOT NULL CHECK (value > 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS symbol_timeframe_bindings (
            symbol TEXT NOT NULL REFERENCES managed_symbols(symbol) ON DELETE CASCADE,
            timeframe TEXT NOT NULL REFERENCES custom_timeframes(code) ON DELETE CASCADE,
            mode TEXT NOT NULL CHECK (mode IN ('virtual','materialized')),
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (symbol, timeframe)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS connector_runtime_settings (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS backfill_jobs (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            target_type TEXT NOT NULL CHECK (target_type IN ('candles','ticks','custom')),
            timeframe TEXT,
            source_type TEXT NOT NULL CHECK (source_type IN ('candles','ticks','custom')),
            source_timeframe TEXT,
            mode TEXT NOT NULL CHECK (mode IN ('fill_missing','refresh')),
            range_from TIMESTAMPTZ NOT NULL,
            range_to TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN (
                    'queued','running','succeeded','partial','failed',
                    'cancelling','cancelled'
                )
            ),
            progress NUMERIC(6,5) NOT NULL DEFAULT 0,
            covered_to TIMESTAMPTZ,
            rows_read BIGINT NOT NULL DEFAULT 0,
            rows_written BIGINT NOT NULL DEFAULT 0,
            error TEXT,
            requested_by TEXT,
            request_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_backfill_jobs_status_created ON backfill_jobs(status, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_backfill_jobs_symbol_created ON backfill_jobs(symbol, created_at DESC)",
    )
    async with get_engine().begin() as conn:
        for statement in statements:
            await conn.execute(text(statement))
        await conn.execute(text("""
            UPDATE backfill_jobs
            SET rows_read=GREATEST(rows_read, 0),
                rows_written=GREATEST(rows_written, 0)
            WHERE rows_read < 0 OR rows_written < 0
        """))
        await conn.execute(
            text("""
                INSERT INTO connector_runtime_settings(key, value)
                VALUES ('tick_retention_days', to_jsonb(365::integer))
                ON CONFLICT (key) DO NOTHING
            """)
        )
        for symbol in sorted(set(configured_symbols or [])):
            await conn.execute(
                text("""
                    INSERT INTO managed_symbols(symbol, active, source)
                    VALUES (:symbol, TRUE, 'config')
                    ON CONFLICT (symbol) DO NOTHING
                """),
                {"symbol": symbol.upper()},
            )


async def list_managed_symbols() -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("SELECT * FROM managed_symbols ORDER BY symbol"))
        return [dict(row._mapping) for row in result]


async def set_managed_symbol(symbol: str, description: str, active: bool) -> dict[str, Any]:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(text("""
            INSERT INTO managed_symbols(symbol, description, active, source)
            VALUES (:symbol, :description, :active, 'management')
            ON CONFLICT (symbol) DO UPDATE SET
                description = EXCLUDED.description,
                active = EXCLUDED.active,
                updated_at = NOW()
            RETURNING *
        """), {"symbol": symbol.upper(), "description": description, "active": active})
        return dict(result.one()._mapping)


async def active_managed_symbol_names() -> list[str]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("SELECT symbol FROM managed_symbols WHERE active ORDER BY symbol"))
        return [str(row[0]) for row in result]


async def list_custom_timeframes() -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("""
            SELECT d.*, COALESCE(json_agg(json_build_object(
                'symbol', b.symbol, 'mode', b.mode, 'enabled', b.enabled
            )) FILTER (WHERE b.symbol IS NOT NULL), '[]'::json) AS bindings
            FROM custom_timeframes d
            LEFT JOIN symbol_timeframe_bindings b ON b.timeframe = d.code
            GROUP BY d.code
            ORDER BY d.unit, d.value
        """))
        return [dict(row._mapping) for row in result]


async def upsert_custom_timeframe(code: str, unit: str, value: int) -> dict[str, Any]:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(text("""
            INSERT INTO custom_timeframes(code, unit, value)
            VALUES (:code, :unit, :value)
            ON CONFLICT (code) DO UPDATE SET unit=EXCLUDED.unit, value=EXCLUDED.value, updated_at=NOW()
            RETURNING *
        """), {"code": code, "unit": unit, "value": value})
        return dict(result.one()._mapping)


async def bind_custom_timeframe(symbol: str, timeframe: str, mode: str, enabled: bool) -> dict[str, Any]:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(text("""
            INSERT INTO symbol_timeframe_bindings(symbol, timeframe, mode, enabled)
            VALUES (:symbol, :timeframe, :mode, :enabled)
            ON CONFLICT (symbol, timeframe) DO UPDATE SET
                mode=EXCLUDED.mode, enabled=EXCLUDED.enabled, updated_at=NOW()
            RETURNING *
        """), {"symbol": symbol, "timeframe": timeframe, "mode": mode, "enabled": enabled})
        return dict(result.one()._mapping)


async def symbol_bindings(symbol: str) -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("""
            SELECT b.*, d.unit, d.value
            FROM symbol_timeframe_bindings b
            JOIN custom_timeframes d ON d.code=b.timeframe
            WHERE b.symbol=:symbol ORDER BY d.unit, d.value
        """), {"symbol": symbol})
        return [dict(row._mapping) for row in result]


async def materialized_bindings() -> list[dict[str, Any]]:
    """Return enabled materialized definitions for incremental refresh."""
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("""
            SELECT b.symbol, b.timeframe, d.unit, d.value
            FROM symbol_timeframe_bindings b
            JOIN custom_timeframes d ON d.code=b.timeframe
            JOIN managed_symbols m ON m.symbol=b.symbol
            WHERE b.enabled AND b.mode='materialized' AND m.active
              AND d.unit <> 'T'
            ORDER BY b.symbol, d.unit, d.value
        """))
        return [dict(row._mapping) for row in result]


async def create_job(values: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        existing = await session.execute(text("""
            SELECT * FROM backfill_jobs
            WHERE symbol=:symbol AND target_type=:target_type
              AND COALESCE(timeframe, '')=COALESCE(:timeframe, '')
              AND mode=:mode AND range_from=:range_from AND range_to=:range_to
              AND status IN ('queued','running','cancelling')
            ORDER BY created_at DESC LIMIT 1
        """), values)
        row = existing.first()
        if row:
            return dict(row._mapping), False
        job_id = uuid4().hex
        params = {**values, "id": job_id}
        result = await session.execute(text("""
            INSERT INTO backfill_jobs(
                id, symbol, target_type, timeframe, source_type, source_timeframe,
                mode, range_from, range_to, status, requested_by
            ) VALUES (
                :id, :symbol, :target_type, :timeframe, :source_type, :source_timeframe,
                :mode, :range_from, :range_to, 'queued', :requested_by
            ) RETURNING *
        """), params)
        return dict(result.one()._mapping), True


async def get_job(job_id: str) -> dict[str, Any] | None:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            text("SELECT * FROM backfill_jobs WHERE id=:id"),
            {"id": job_id},
        )
        row = result.first()
        return dict(row._mapping) if row else None


async def list_jobs(limit: int = 50) -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            text("SELECT * FROM backfill_jobs ORDER BY created_at DESC LIMIT :limit"),
            {"limit": limit},
        )
        return [dict(row._mapping) for row in result]


async def queued_jobs() -> list[dict[str, Any]]:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            text("SELECT * FROM backfill_jobs WHERE status='queued' ORDER BY created_at")
        )
        return [dict(row._mapping) for row in result]


async def update_job(job_id: str, **changes: Any) -> dict[str, Any] | None:
    allowed = {
        "status", "progress", "covered_to", "rows_read", "rows_written", "error",
        "request_id", "started_at", "finished_at",
    }
    selected = {key: value for key, value in changes.items() if key in allowed}
    if not selected:
        return await get_job(job_id)
    assignments = ", ".join(f"{key}=:{key}" for key in selected)
    params = {"id": job_id, **selected}
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(text(
            f"UPDATE backfill_jobs SET {assignments}, updated_at=NOW() WHERE id=:id RETURNING *"
        ), params)
        row = result.first()
        return dict(row._mapping) if row else None


async def request_cancel(job_id: str) -> dict[str, Any] | None:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(text("""
            UPDATE backfill_jobs
            SET status=CASE WHEN status='queued' THEN 'cancelled' ELSE 'cancelling' END,
                finished_at=CASE WHEN status='queued' THEN NOW() ELSE finished_at END,
                updated_at=NOW()
            WHERE id=:id AND status IN ('queued','running') RETURNING *
        """), {"id": job_id})
        row = result.first()
        if row:
            return dict(row._mapping)
    return await get_job(job_id)


async def recover_interrupted_jobs() -> int:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        result = await session.execute(text("""
            UPDATE backfill_jobs
            SET status=CASE WHEN status='cancelling' THEN 'cancelled' ELSE 'queued' END,
                started_at=CASE WHEN status='cancelling' THEN started_at ELSE NULL END,
                finished_at=CASE WHEN status='cancelling' THEN NOW() ELSE NULL END,
                error=CASE
                    WHEN status='cancelling' THEN 'Cancelled during poller restart'
                    ELSE 'Recovered after poller restart'
                END,
                updated_at=NOW()
            WHERE status IN ('running','cancelling')
        """))
        return int(result.rowcount or 0)


async def coverage_tree() -> list[dict[str, Any]]:
    """Return navigation watermarks without scanning the full history.

    Exact counts, first timestamps, and storage size remain in
    ``coverage_detail`` and are calculated only for the selected branch.
    """
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("""
            WITH symbols AS (
                SELECT symbol FROM managed_symbols
                UNION SELECT DISTINCT symbol FROM sync_state
            ), watermarks AS (
                SELECT symbol, data_type, last_synced_at
                FROM sync_state
            )
            SELECT s.symbol, COALESCE(m.active, FALSE) active, COALESCE(m.description, '') description,
                COALESCE((SELECT json_agg(json_build_object(
                    'timeframe', w.data_type, 'first_at', NULL, 'last_at', w.last_synced_at
                ) ORDER BY w.data_type) FROM watermarks w
                    WHERE w.symbol=s.symbol AND w.data_type <> 'tick'), '[]'::json) timeframes,
                (SELECT json_build_object('first_at', NULL, 'last_at', w.last_synced_at)
                    FROM watermarks w WHERE w.symbol=s.symbol AND w.data_type='tick') ticks
            FROM symbols s LEFT JOIN managed_symbols m USING(symbol)
            ORDER BY s.symbol
        """))
        return [dict(row._mapping) for row in result]


async def coverage_detail(symbol: str, target_type: str, timeframe: str | None) -> dict[str, Any]:
    factory = get_session_factory()
    async with factory() as session:
        if target_type == "ticks":
            sql = text("""
                SELECT COUNT(*)::bigint total, MIN(time_msc) first_at, MAX(time_msc) last_at,
                    COALESCE(SUM(pg_column_size(t)),0)::bigint storage_bytes
                FROM ticks t WHERE symbol=:symbol
            """)
            params = {"symbol": symbol}
        else:
            sql = text("""
                SELECT COUNT(*)::bigint total, MIN(time) first_at, MAX(time) last_at,
                    COALESCE(SUM(pg_column_size(c)),0)::bigint storage_bytes
                FROM candles c WHERE symbol=:symbol AND timeframe=:timeframe
            """)
            params = {"symbol": symbol, "timeframe": timeframe}
        result = await session.execute(sql, params)
        row = dict(result.one()._mapping)
        sync_key = "tick" if target_type == "ticks" else timeframe
        sync_result = await session.execute(text("""
            SELECT last_synced_at, updated_at FROM sync_state
            WHERE symbol=:symbol AND data_type=:data_type
        """), {"symbol": symbol, "data_type": sync_key})
        sync = sync_result.first()
        row.update(dict(sync._mapping) if sync else {"last_synced_at": None, "updated_at": None})
        return row


async def retention_preview(days: int) -> dict[str, Any]:
    cutoff = datetime.now(UTC).timestamp() - days * 86400
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text("""
            SELECT COUNT(*)::bigint affected_rows, MIN(time_msc) earliest_tick,
                   MAX(time_msc) latest_removed_tick
            FROM ticks WHERE time_msc < to_timestamp(:cutoff)
        """), {"cutoff": cutoff})
        row = dict(result.one()._mapping)
        row.update({"days": days, "cutoff": datetime.fromtimestamp(cutoff, tz=UTC)})
        return row


async def get_retention_days() -> int:
    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(text(
            "SELECT value FROM connector_runtime_settings "
            "WHERE key='tick_retention_days'"
        ))
        value = result.scalar_one_or_none()
        return int(value if value is not None else 365)


async def apply_retention_days(days: int) -> int:
    async with get_engine().begin() as conn:
        await conn.execute(text("SELECT remove_retention_policy('ticks', if_exists => TRUE)"))
        await conn.execute(
            text(
                "SELECT add_retention_policy("
                "'ticks', make_interval(days => :days), if_not_exists => TRUE)"
            ),
            {"days": days},
        )
        await conn.execute(text("""
            INSERT INTO connector_runtime_settings(key, value, updated_at)
            VALUES ('tick_retention_days', to_jsonb(CAST(:days AS integer)), NOW())
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
        """), {"days": days})
    return days


async def materialize_timeframe(
    symbol: str,
    timeframe: str,
    bucket_seconds: int,
    source_timeframe: str,
    dt_from: datetime,
    dt_to: datetime,
    refresh: bool,
) -> int:
    factory = get_session_factory()
    async with factory() as session, session.begin():
        conflict_clause = """
            DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high,
                low=EXCLUDED.low, close=EXCLUDED.close,
                tick_volume=EXCLUDED.tick_volume,
                real_volume=EXCLUDED.real_volume, spread=EXCLUDED.spread
        """ if refresh else "DO NOTHING"
        result = await session.execute(text(f"""
            INSERT INTO candles(time, symbol, timeframe, open, high, low, close,
                                tick_volume, real_volume, spread)
            SELECT time_bucket(make_interval(secs => :seconds), time), :symbol, :timeframe,
                   (ARRAY_AGG(open ORDER BY time ASC))[1], MAX(high), MIN(low),
                   (ARRAY_AGG(close ORDER BY time DESC))[1], SUM(tick_volume),
                   SUM(real_volume), MAX(spread)
            FROM candles
            WHERE symbol=:symbol AND timeframe=:source_timeframe
              AND time>=:dt_from AND time<=:dt_to
            GROUP BY time_bucket(make_interval(secs => :seconds), time)
            ON CONFLICT (symbol, timeframe, time) {conflict_clause}
            RETURNING time
        """), {
            "symbol": symbol, "timeframe": timeframe, "source_timeframe": source_timeframe,
            "seconds": bucket_seconds, "dt_from": dt_from, "dt_to": dt_to,
        })
        return len(result.all())

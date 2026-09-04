"""Small, isolated budget for tick history reads (shared across API workers).

The transaction advisory lock is fail-fast and belongs to the SQL transaction,
so a cancelled/orphaned backend cannot let another heavy query overlap it.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config import get_settings

HEAVY_LOCK_KEY = 788601100001
_engine = None
_factory = None


class HeavyReadUnavailable(Exception):
    code = "history_busy"
    status_code = 503


class HeavyReadTimeout(HeavyReadUnavailable):
    code = "history_timeout"
    status_code = 504


class HistoryBudgetExceeded(HeavyReadUnavailable):
    code = "history_budget_exceeded"
    status_code = 422


def validate_source_budget(rows: int) -> None:
    maximum = get_settings().history_max_source_rows
    if rows < 1 or rows > maximum:
        raise HistoryBudgetExceeded(
            f"Requested history exceeds the interactive budget ({maximum} source ticks). "
            "Request fewer bars or a smaller date range."
        )


def get_heavy_engine():
    global _engine, _factory
    if _engine is None:
        s = get_settings()
        _engine = create_async_engine(
            s.dsn, pool_size=1, max_overflow=0, pool_timeout=0.25,
            pool_pre_ping=True, pool_recycle=300,
            connect_args={
                "timeout": 3,
                "command_timeout": s.history_statement_timeout_sec + 3,
                "server_settings": {
                    "application_name": "mt_connector_history",
                    "statement_timeout": f"{s.history_statement_timeout_sec}s",
                    "lock_timeout": "1s",
                    "idle_in_transaction_session_timeout": "20s",
                    "work_mem": "32MB",
                    "temp_file_limit": "262144",  # 256 MiB, per backend
                },
            },
        )
        _factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


@asynccontextmanager
async def heavy_read_session():
    get_heavy_engine()
    try:
        async with _factory() as session:
            async with session.begin():
                acquired = await session.scalar(
                    text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": HEAVY_LOCK_KEY}
                )
                if not acquired:
                    raise HeavyReadUnavailable("Another history query is still running. Retry later.")
                yield session
    except PoolTimeout as exc:
        raise HeavyReadUnavailable("History query capacity is busy. Retry later.") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        raise HeavyReadTimeout("History query exceeded its time budget.") from exc
    except DBAPIError as exc:
        sqlstate = getattr(exc.orig, "sqlstate", None)
        if sqlstate in ("57014", "53400"):
            raise HeavyReadTimeout("History query exceeded its execution budget.") from exc
        if sqlstate == "55P03":
            raise HeavyReadUnavailable("History is temporarily locked. Retry later.") from exc
        raise


async def dispose_heavy_engine():
    global _engine, _factory
    if _engine is not None:
        await _engine.dispose()
        _engine = _factory = None

"""Strictly bounded latest-one reads, independent of the heavy history gate.

This lane cannot be selected for a range or a bulk read. It uses the indexed
database truth, not an unlabelled stale cache or a trading/control connection.
"""
import asyncio
from contextlib import asynccontextmanager

from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config import get_settings
from src.db.heavy_reads import HeavyReadUnavailable

_engine = None
_factory = None


class QuoteReadUnavailable(HeavyReadUnavailable):
    code = "quote_busy"


def get_quote_engine():
    global _engine, _factory
    if _engine is None:
        _engine = create_async_engine(
            get_settings().dsn, pool_size=2, max_overflow=0, pool_timeout=0.25,
            pool_pre_ping=True, pool_recycle=300,
            connect_args={"timeout": 2, "command_timeout": 3, "server_settings": {
                "application_name": "mt_connector_quote", "statement_timeout": "2s",
                "lock_timeout": "250ms", "idle_in_transaction_session_timeout": "5s",
                "default_transaction_read_only": "on", "work_mem": "4MB",
                "temp_file_limit": "16384",
            }},
        )
        _factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


@asynccontextmanager
async def quote_read_session():
    get_quote_engine()
    try:
        async with asyncio.timeout(3), _factory() as session:
            async with session.begin():
                yield session
    except (PoolTimeout, TimeoutError) as exc:
        raise QuoteReadUnavailable("Latest quote capacity is temporarily busy.") from exc
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) in {"57014", "53400", "55P03"}:
            raise QuoteReadUnavailable("Latest quote read exceeded its bounded budget.") from exc
        raise


async def dispose_quote_engine():
    global _engine, _factory
    if _engine is not None:
        await _engine.dispose()
        _engine = _factory = None

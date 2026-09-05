"""
Database engine and session management.

Creates an async SQLAlchemy engine backed by asyncpg, with connection
pooling configured from application settings.
"""

from __future__ import annotations

import structlog
import ssl
from urllib.parse import urlsplit
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.config import Settings, get_settings

logger = structlog.get_logger(__name__)

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_command_engine: AsyncEngine | None = None
_command_factory: async_sessionmaker[AsyncSession] | None = None
_trading_engine: AsyncEngine | None = None
_trading_factory: async_sessionmaker[AsyncSession] | None = None


def control_connect_args(settings: Settings) -> dict:
    args = {"timeout": 3, "command_timeout": 6, "server_settings": {
        "application_name": "mt_connector_control", "statement_timeout": "5000ms",
        "lock_timeout": "500ms", "idle_in_transaction_session_timeout": "10000ms",
    }}
    if settings.control_db_url:
        endpoint = urlsplit(settings.control_db_url)
        if endpoint.scheme != "postgresql+asyncpg":
            raise ValueError("Control database requires the asyncpg driver")
        if endpoint.query:
            raise ValueError("Control database TLS options must use the verified CA configuration")
        if endpoint.hostname not in {"localhost", "127.0.0.1", "::1"}:
            args["ssl"] = ssl.create_default_context(cafile=settings.control_db_ca_file)
    if settings.control_db_ca_file:
        args["ssl"] = ssl.create_default_context(cafile=settings.control_db_ca_file)
    return args


def get_trading_engine(settings: Settings | None = None) -> AsyncEngine:
    global _trading_engine
    settings = settings or get_settings()
    if not settings.control_db_url:
        return get_engine(settings)
    if _trading_engine is None:
        _trading_engine = create_async_engine(
            settings.control_db_url, pool_size=3, max_overflow=2,
            pool_timeout=1, pool_pre_ping=True, pool_recycle=300,
            connect_args=control_connect_args(settings),
        )
    return _trading_engine


def get_trading_session_factory() -> async_sessionmaker[AsyncSession]:
    global _trading_factory
    if not get_settings().control_db_url:
        return get_session_factory()
    if _trading_factory is None:
        _trading_factory = async_sessionmaker(get_trading_engine(), expire_on_commit=False)
    return _trading_factory


async def verify_control_store() -> None:
    if not get_settings().control_db_url:
        return
    async with get_trading_engine().connect() as conn:
        version = (await conn.execute(text(
            "SELECT schema_version FROM control_store_metadata WHERE singleton_id=1"
        ))).scalar_one()
        if version != 1:
            raise RuntimeError("Unsupported operational store schema")


def get_command_session_factory() -> async_sessionmaker[AsyncSession]:
    """Reserve a small, short-deadline pool for command journals, not history."""
    global _command_engine, _command_factory
    if _command_factory is None:
        _command_engine = create_async_engine(
            get_settings().control_db_url or get_settings().dsn, pool_size=2, max_overflow=0,
            pool_timeout=0.5, pool_pre_ping=True, pool_recycle=300,
            connect_args=control_connect_args(get_settings()),
        )
        _command_factory = async_sessionmaker(_command_engine, expire_on_commit=False)
    return _command_factory


def get_engine(settings: Settings | None = None) -> AsyncEngine:
    """Return (and cache) the async engine singleton."""
    global _engine
    if _engine is not None:
        return _engine

    settings = settings or get_settings()
    _engine = create_async_engine(
        settings.dsn,
        pool_size=settings.db_pool_min,
        max_overflow=settings.db_pool_max - settings.db_pool_min,
        pool_timeout=3,
        pool_pre_ping=True,
        pool_recycle=600,
        connect_args={
            "timeout": 3,
            "command_timeout": settings.db_command_timeout_sec,
            "server_settings": {
                "application_name": "mt_connector",
                "statement_timeout": f"{settings.db_statement_timeout_ms}ms",
                "lock_timeout": "2000ms",
                "idle_in_transaction_session_timeout": (
                    f"{settings.db_idle_in_transaction_timeout_ms}ms"
                ),
            },
        },
        echo=False,
    )
    logger.info(
        "db_engine_created",
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    )
    return _engine


def get_session_factory(settings: Settings | None = None) -> async_sessionmaker[AsyncSession]:
    """Return (and cache) the async session factory."""
    global _session_factory
    if _session_factory is not None:
        return _session_factory

    engine = get_engine(settings)
    _session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return _session_factory


async def dispose_engine() -> None:
    """Gracefully close the connection pool."""
    global _engine, _session_factory
    global _command_engine, _command_factory
    global _trading_engine, _trading_factory
    if _trading_engine is not None:
        await _trading_engine.dispose()
        _trading_engine = None
        _trading_factory = None
    if _command_engine is not None:
        await _command_engine.dispose()
        _command_engine = None
        _command_factory = None
    if _engine is not None:
        await _engine.dispose()
        logger.info("db_engine_disposed")
        _engine = None
        _session_factory = None

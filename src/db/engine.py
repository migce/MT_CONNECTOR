"""
Database engine and session management.

Creates an async SQLAlchemy engine backed by asyncpg, with connection
pooling configured from application settings.
"""

from __future__ import annotations

import structlog
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


def get_command_session_factory() -> async_sessionmaker[AsyncSession]:
    """Reserve a small, short-deadline pool for command journals, not history."""
    global _command_engine, _command_factory
    if _command_factory is None:
        _command_engine = create_async_engine(
            get_settings().dsn, pool_size=2, max_overflow=0,
            pool_timeout=0.5, pool_pre_ping=True, pool_recycle=300,
            connect_args={"timeout": 3, "command_timeout": 5, "server_settings": {
                "application_name": "mt_connector_execution",
                "statement_timeout": "5000ms", "lock_timeout": "500ms",
                "idle_in_transaction_session_timeout": "10000ms",
            }},
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
    if _command_engine is not None:
        await _command_engine.dispose()
        _command_engine = None
        _command_factory = None
    if _engine is not None:
        await _engine.dispose()
        logger.info("db_engine_disposed")
        _engine = None
        _session_factory = None

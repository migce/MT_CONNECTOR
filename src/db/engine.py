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
        pool_pre_ping=True,
        pool_recycle=600,
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
    if _engine is not None:
        await _engine.dispose()
        logger.info("db_engine_disposed")
        _engine = None
        _session_factory = None

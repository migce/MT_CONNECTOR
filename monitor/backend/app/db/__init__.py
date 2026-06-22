"""
Async SQLAlchemy engine for monitor DB.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

_engine: AsyncEngine | None = None
_session_factory: sessionmaker | None = None


async def get_engine(url: str | None = None) -> AsyncEngine:
    global _engine, _session_factory
    if _engine is None:
        if url is None:
            from app.config import get_settings
            url = get_settings().database_url
        _engine = create_async_engine(url, pool_size=5, max_overflow=10)
        _session_factory = sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    return _engine


async def get_session() -> AsyncSession:  # type: ignore[misc]
    if _session_factory is None:
        await get_engine()
    async with _session_factory() as session:  # type: ignore[misc]
        yield session


async def dispose_engine() -> None:
    global _engine, _session_factory
    if _engine:
        await _engine.dispose()
        _engine = None
        _session_factory = None

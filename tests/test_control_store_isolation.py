"""Routing and failure-domain qualification; no live broker interaction."""
import asyncio
import os
import ssl
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from src.config import Settings
from src.db import engine, trading_repository
from src.redis_bus import pool, publisher, subscriber


def settings(**values):
    return Settings(_env_file=None, mt5_login=0, mt5_password="fixture",
                    mt5_server="fixture", db_password="fixture", **values)


def test_remote_database_uses_verified_tls():
    args = engine.control_connect_args(settings(control_db_url="postgresql+asyncpg://u:p@192.0.2.1/control"))
    assert args["ssl"].verify_mode == ssl.CERT_REQUIRED
    assert args["ssl"].check_hostname
    with pytest.raises(ValueError):
        engine.control_connect_args(settings(control_db_url="postgresql+asyncpg://u:p@192.0.2.1/control?ssl=disable"))


def test_explicit_control_redis_is_distinct_and_tls_verified(monkeypatch):
    monkeypatch.setattr(pool, "_control_pool", None)
    monkeypatch.setattr(pool, "_pool", None)
    cfg = settings(control_redis_url="rediss://u:p@192.0.2.1:16380/0")
    control = pool.get_redis_pool(cfg)
    cache = pool.get_cache_redis_pool(cfg)
    assert control is not cache
    assert control.connection_pool.connection_kwargs["ssl_cert_reqs"] == "required"
    assert control.connection_pool.connection_kwargs["ssl_check_hostname"] is True
    assert publisher.get_redis_pool is pool.get_cache_redis_pool
    assert subscriber.get_redis_pool is pool.get_cache_redis_pool
    assert trading_repository.get_session_factory is engine.get_trading_session_factory
    with pytest.raises(ValueError):
        pool.new_control_redis(settings(control_redis_url="redis://u:p@192.0.2.1:16380/0"))
    with pytest.raises(ValueError):
        pool.new_control_redis(settings(control_redis_url="rediss://u:p@192.0.2.1:16380/0?ssl_cert_reqs=none"))


async def test_real_control_sql_and_redis_with_unavailable_history_and_cache(monkeypatch):
    url = os.getenv("RELIABILITY_TEST_DSN")
    redis_url = os.getenv("CONTROL_TEST_REDIS_URL")
    if not url or not redis_url:
        pytest.skip("Disposable control database and Redis required")
    target = make_url(url)
    assert target.host == "127.0.0.1" and target.database == "reliability_test"
    assert target.port not in (None, 5432)
    assert redis_url.startswith("redis://127.0.0.1:")
    cfg = settings(db_host="127.0.0.1", db_port=1, redis_host="127.0.0.1", redis_port=1,
                   control_db_url=url, control_redis_url=redis_url)
    monkeypatch.setattr(engine, "get_settings", lambda: cfg)
    monkeypatch.setattr(pool, "get_settings", lambda: cfg)
    await engine.dispose_engine()
    await pool.close_redis_pool()
    try:
        with pytest.raises(Exception):
            async with asyncio.timeout(4):
                async with engine.get_engine().connect():
                    pass
        # Journal pool and trading pool both remain usable independently.
        async with engine.get_command_session_factory()() as session:
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
        async with engine.get_trading_session_factory()() as session:
            assert (await session.execute(text("SELECT 2"))).scalar_one() == 2
        with pytest.raises(Exception):
            await pool.get_cache_redis_pool().ping()
        control = pool.get_redis_pool()
        assert await control.ping()
        assert (await control.config_get("maxmemory-policy"))["maxmemory-policy"] == "noeviction"
        await control.set("fixture:control:lease", "owner", nx=True, ex=10)
        assert not await control.set("fixture:control:lease", "competitor", nx=True, ex=10)
        await control.delete("fixture:control:lease")
    finally:
        await engine.dispose_engine()
        await pool.close_redis_pool()


async def test_explicit_control_failure_does_not_use_healthy_cache(monkeypatch):
    monkeypatch.setattr(pool, "_control_pool", None)
    cache = MagicMock()
    monkeypatch.setattr(pool, "get_cache_redis_pool", cache)
    control = pool.get_redis_pool(settings(control_redis_url="redis://127.0.0.1:1/0"))
    try:
        with pytest.raises(Exception):
            await control.ping()
        cache.assert_not_called()
    finally:
        await control.aclose()
        monkeypatch.setattr(pool, "_control_pool", None)

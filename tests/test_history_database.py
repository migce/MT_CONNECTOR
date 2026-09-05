"""Real SQL proof, opt-in disposable database only."""
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.config import Settings
from src.db import repository, symbol_management
from src.mt5 import history_queue

URL = os.environ.get("RELIABILITY_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not URL, reason="disposable history SQL fixture required")


@pytest_asyncio.fixture
async def db(monkeypatch):
    url = make_url(URL)
    assert url.host == "127.0.0.1" and url.database == "reliability_test" and url.port not in (None, 5432)
    schema = "history_" + uuid4().hex
    engine = create_async_engine(URL)
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    await engine.dispose()
    engine = create_async_engine(URL, connect_args={"server_settings": {"search_path": schema}})
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(history_queue, "get_engine", lambda: engine)
    monkeypatch.setattr(symbol_management, "get_engine", lambda: engine)
    monkeypatch.setattr(symbol_management, "get_session_factory", lambda: factory)
    monkeypatch.setattr(repository, "get_session_factory", lambda: factory)
    await symbol_management.ensure_schema([])
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE sync_state(symbol text,data_type text,last_synced_at timestamptz,last_tick_msc bigint,updated_at timestamptz,PRIMARY KEY(symbol,data_type))"))
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def test_frozen_job_ranges_survive_new_live_cursor(db):
    old = datetime.now(UTC)-timedelta(days=2)
    await repository.update_sync_state("TEST", "M1", old)
    settings = Settings(_env_file=None, mt5_login=0, mt5_password="fixture", mt5_server="fixture",
                        db_password="fixture", timeframes_csv="M1")
    await history_queue.freeze_ranges(["TEST"], settings)
    await repository.update_sync_state("TEST", "M1", datetime.now(UTC))
    jobs = await symbol_management.queued_jobs()
    candles = next(j for j in jobs if j["target_type"] == "candles")
    assert candles["range_from"] == old
    assert candles["range_to"] < datetime.now(UTC)
    assert len(jobs) == 2


async def test_old_history_cannot_rewind_live_watermark(db):
    now = datetime.now(UTC)
    await repository.update_sync_state("TEST", "tick", now, 10000)
    await repository.update_sync_state("TEST", "tick", now-timedelta(days=1), 100)
    state = await repository.get_sync_state("TEST", "tick")
    assert state["last_synced_at"] == now and state["last_tick_msc"] == 10000


async def test_freeze_failure_is_atomic_and_does_not_advance_cursor(db, monkeypatch):
    now = datetime.now(UTC)
    monkeypatch.setattr(history_queue, "plan_ranges", lambda *a: [
        ("OK", "ticks", None, "fill_missing", now-timedelta(days=1), now),
        ("BAD", "not_a_valid_type", None, "fill_missing", now-timedelta(days=1), now)])
    with pytest.raises(Exception):
        await history_queue.freeze_ranges([], None)
    assert await symbol_management.queued_jobs() == []

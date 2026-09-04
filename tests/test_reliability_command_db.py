"""Opt-in tests: refuses production-like targets and contains only fake commands."""
from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.db import trading_repository as repo

URL = os.environ.get("RELIABILITY_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not URL, reason="isolated reliability database only")


@pytest_asyncio.fixture
async def database(monkeypatch):
    target = make_url(URL)
    assert target.host == "127.0.0.1" and target.database == "reliability_test"
    assert target.port not in (None, 5432)
    engine = create_async_engine(URL, pool_size=4, max_overflow=0)
    schema = "reliability_" + uuid4().hex
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        # Capture contains DDL only and uses a per-test namespace.
        ddl = Path(__file__).with_name("reliability_trade_schema.sql").read_text()
        ddl = ddl.replace("public.", f"{schema}.").replace("TO public", f'TO "{schema}"')
        raw = await conn.get_raw_connection()
        await raw.driver_connection.execute(ddl)
    await engine.dispose()
    engine = create_async_engine(URL, pool_size=4, max_overflow=4,
                                 connect_args={"server_settings": {"search_path": schema}})
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(repo, "get_command_session_factory", lambda: factory)
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO trading_accounts(id) VALUES (68), (84)"))
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def command(account_id=68):
    now = datetime.now(UTC)
    row, _ = await repo.create_trade_command(
        command_id=uuid4(), account_id=account_id, position_ticket=1,
        expected_position_identifier=1, expected_symbol="TEST", expected_type=0,
        expected_magic=1, max_volume=1, reason="TEST_ONLY", correlation_id=None,
        requested_by="isolated_test", requested_at=now, expires_at=now+timedelta(seconds=120),
    )
    return row


async def test_concurrent_claims_allow_only_one_owner_per_account(database):
    assert await repo.get_execution_account(68) == {"id": 68, "enabled": True}
    assert await repo.get_execution_account(999) is None
    await asyncio.gather(*(command() for _ in range(8)))
    results = await asyncio.gather(*(repo.claim_next_trade_command([68]) for _ in range(16)))
    assert sum(row is not None for row in results) == 1
    assert await repo.claim_next_trade_command([68]) is None
    await command(84)
    assert (await repo.claim_next_trade_command([84]))["account_id"] == 84


async def test_stale_claim_is_unknown_never_automatically_replayed(database):
    row = await command()
    claimed = await repo.claim_next_trade_command([68])
    assert claimed["id"] == row["id"]
    async with database.begin() as conn:
        await conn.execute(text("UPDATE trade_commands SET claimed_at=NOW()-interval '10 minutes'"))
    assert await repo.quarantine_stale_claimed_commands(120) == 1
    state = await repo.get_trade_command(row["id"])
    assert state["status"] == "unknown"
    assert await repo.execution_blocked_accounts() == [68]
    await command()
    assert await repo.claim_next_trade_command([68]) is None
    # An old worker cannot overwrite the recovery fence, even with a late result.
    await repo.finish_trade_command(row["id"], status="confirmed", result={}, expected_attempt=1)
    assert (await repo.get_trade_command(row["id"]))["status"] == "unknown"


async def test_attempt_fence_and_definitive_retry(database):
    row = await command()
    await repo.claim_next_trade_command([68])
    await repo.finish_trade_command(row["id"], status="confirmed", result={}, expected_attempt=0)
    assert (await repo.get_trade_command(row["id"]))["status"] == "claimed"
    await repo.retry_trade_command(row["id"], result={}, error="definitive_reject", delay_sec=0,
                                  expected_attempt=1)
    claimed = await repo.claim_next_trade_command([68])
    assert claimed["attempt_count"] == 2
    await repo.finish_trade_command(row["id"], status="confirmed", result={}, expected_attempt=1)
    assert (await repo.get_trade_command(row["id"]))["status"] == "claimed"
    await repo.finish_trade_command(row["id"], status="confirmed", result={}, expected_attempt=2)
    assert (await repo.get_trade_command(row["id"]))["status"] == "confirmed"


async def test_missing_or_elapsed_expiry_never_claims(database):
    await command()
    async with database.begin() as conn:
        await conn.execute(text("UPDATE trade_commands SET expires_at=NULL"))
    assert await repo.claim_next_trade_command([68]) is None
    assert await repo.expire_trade_commands() == 1


async def test_tick_row_counts_are_real_under_executemany_duplicates(database, monkeypatch):
    from src.db import repository
    factory = async_sessionmaker(database, expire_on_commit=False)
    monkeypatch.setattr(repository, "get_session_factory", lambda: factory)
    async with database.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE ticks (
                time_msc timestamptz NOT NULL, symbol text NOT NULL, bid double precision,
                ask double precision, last double precision, volume double precision,
                flags bigint, PRIMARY KEY(symbol,time_msc)
            )
        """))
    now = datetime.now(UTC)
    rows = [{"time_msc": now + timedelta(milliseconds=i), "symbol": "TEST",
             "bid": 1., "ask": 2., "last": 1., "volume": 1., "flags": 1} for i in range(2100)]
    assert await repository.insert_ticks(rows) == 2100
    assert await repository.insert_ticks(rows) == 0
    assert await repository.insert_ticks([*rows[-2:], {**rows[0], "symbol": "TEST2"}]) == 1
    assert await repository.upsert_ticks(rows) == 2100

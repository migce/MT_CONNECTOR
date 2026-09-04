from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.api.middleware.history_disconnect import HistoryDisconnectMiddleware
from src.db import heavy_reads as history


def test_source_budget_is_explicit():
    with patch.object(history, 'get_settings', return_value=SimpleNamespace(history_max_source_rows=300_000)):
        history.validate_source_budget(300_000)
        for rows in (0, -1, 300_001):
            with pytest.raises(history.HistoryBudgetExceeded):
                history.validate_source_budget(rows)


@pytest.mark.asyncio
async def test_disconnect_cancels_history_but_not_trading():
    cancelled = asyncio.Event()
    started = asyncio.Event()
    incoming = asyncio.Queue()

    async def handler(scope, receive, send):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    task = asyncio.create_task(HistoryDisconnectMiddleware(handler)(
        {'type': 'http', 'method': 'GET', 'path': '/api/v1/candles/custom/EURUSD'},
        incoming.get, None,
    ))
    await started.wait()
    await incoming.put({'type': 'http.disconnect'})
    await asyncio.wait_for(task, 1)
    assert cancelled.is_set()

    calls = []
    async def trade_handler(scope, receive, send):
        calls.append(receive)
    await HistoryDisconnectMiddleware(trade_handler)(
        {'type': 'http', 'method': 'POST', 'path': '/api/v1/orders'}, incoming.get, None,
    )
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('HISTORY_TEST_DSN'), reason='requires isolated PostgreSQL')
async def test_real_pool_gate_deadline_lock_timeout_and_cancel():
    settings = SimpleNamespace(dsn=os.environ['HISTORY_TEST_DSN'], history_statement_timeout_sec=2)
    observer = create_async_engine(settings.dsn)
    with patch.object(history, 'get_settings', return_value=settings):
        try:
            async with observer.connect() as conn:
                # A separate connection simulates another API worker or compression.
                await conn.execute(text('SELECT pg_advisory_lock(:key)'), {'key': history.HEAVY_LOCK_KEY})
                with pytest.raises(history.HeavyReadUnavailable):
                    async with history.heavy_read_session():
                        pytest.fail('shared gate was bypassed')
                await conn.execute(text('SELECT pg_advisory_unlock(:key)'), {'key': history.HEAVY_LOCK_KEY})
                await conn.rollback()

            with pytest.raises(history.HeavyReadTimeout):
                async with history.heavy_read_session() as session:
                    await session.execute(text('SELECT pg_sleep(10)'))

            async with observer.begin() as conn:
                await conn.execute(text('CREATE TABLE IF NOT EXISTS history_test_lock (n int)'))
            async with observer.begin() as conn:
                await conn.execute(text('LOCK history_test_lock IN ACCESS EXCLUSIVE MODE'))
                with pytest.raises(history.HeavyReadUnavailable) as locked:
                    async with history.heavy_read_session() as session:
                        await session.execute(text('SELECT * FROM history_test_lock'))
                assert locked.value.code == 'history_busy'

            started = asyncio.Event()
            async def slow_read():
                async with history.heavy_read_session() as session:
                    started.set()
                    await session.execute(text('SELECT pg_sleep(10)'))
            task = asyncio.create_task(slow_read())
            await started.wait()
            await asyncio.sleep(.05)
            # The light engine is responsive while the history pool is occupied.
            async with observer.connect() as conn:
                assert await conn.scalar(text('SELECT 1')) == 1
            with pytest.raises(history.HeavyReadUnavailable):
                async with history.heavy_read_session():
                    pytest.fail('pool was not bounded')
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            async with history.heavy_read_session() as session:
                assert await session.scalar(text('SELECT 42')) == 42
                assert await session.scalar(text('SHOW lock_timeout')) == '1s'
            async with observer.connect() as conn:
                remaining = await conn.scalar(text("SELECT count(*) FROM pg_stat_activity WHERE application_name='mt_connector_history' AND state='active'"))
                assert remaining == 0
        finally:
            await history.dispose_heavy_engine()
            await observer.dispose()

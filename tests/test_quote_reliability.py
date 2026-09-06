import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import redis.asyncio as aioredis
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.db import heavy_reads, quote_reads, repository
from src.redis_bus import subscriber


@pytest.mark.asyncio
async def test_only_exact_latest_one_uses_quote_lane():
    lanes = []
    def lane(name):
        @asynccontextmanager
        async def session():
            lanes.append(name)
            yield SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [])))
        return session
    with patch.object(repository, 'quote_read_session', lane('quote')), patch.object(repository, 'heavy_read_session', lane('history')):
        await repository.query_ticks('EURUSD', limit=1)
        await repository.query_ticks('EURUSD', limit=2)
        await repository.query_ticks('EURUSD', dt_to=datetime.now(UTC), limit=1)
        await repository.query_ticks('EURUSD', dt_from=datetime.now(UTC), limit=1)
    assert lanes == ['quote', 'history', 'history', 'history']


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('HISTORY_TEST_DSN'), reason='requires isolated PostgreSQL')
async def test_quote_lane_independent_bounded_readonly_and_cancel_safe():
    settings = SimpleNamespace(dsn=os.environ['HISTORY_TEST_DSN'])
    observer = create_async_engine(settings.dsn)
    with patch.object(quote_reads, 'get_settings', return_value=settings):
        try:
            async with observer.begin() as conn:
                await conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': heavy_reads.HEAVY_LOCK_KEY})
                async with quote_reads.quote_read_session() as session:
                    assert await session.scalar(text('SELECT 42')) == 42
                    assert await session.scalar(text('SHOW transaction_read_only')) == 'on'
                    assert await session.scalar(text('SHOW statement_timeout')) == '2s'
            async with quote_reads.quote_read_session() as first, quote_reads.quote_read_session() as second:
                await first.scalar(text('SELECT 1'))
                await second.scalar(text('SELECT 1'))
                with pytest.raises(quote_reads.QuoteReadUnavailable):
                    async with quote_reads.quote_read_session() as excess:
                        await excess.scalar(text('SELECT 1'))
            async with observer.begin() as conn:
                await conn.execute(text('CREATE TABLE ticks (symbol text, time_msc timestamptz, bid float8, ask float8, last float8, volume bigint, flags int, PRIMARY KEY(symbol,time_msc))'))
                await conn.execute(text("INSERT INTO ticks SELECT 'EURUSD', '2026-09-06'::timestamptz + n * interval '1 second', 1, 2, 1, 1, 0 FROM generate_series(1,1000) n"))
            async with observer.begin() as conn:
                await conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': heavy_reads.HEAVY_LOCK_KEY})
                # Real repository SQL, not merely SELECT1 on an unrelated pool.
                rows = await asyncio.gather(*(repository.query_ticks('EURUSD', limit=1) for _ in range(20)))
                assert all(len(row) == 1 and row[0]['bid'] == 1 for row in rows)
            with pytest.raises(quote_reads.QuoteReadUnavailable):
                async with quote_reads.quote_read_session() as session:
                    await session.execute(text('SELECT pg_sleep(8)'))
            async with quote_reads.quote_read_session() as session:
                assert await session.scalar(text('SELECT 1')) == 1
        finally:
            async with observer.begin() as conn:
                await conn.execute(text('DROP TABLE IF EXISTS ticks'))
            await quote_reads.dispose_quote_engine()
            await observer.dispose()


@pytest.mark.asyncio
async def test_idle_pubsub_pings_and_delivers_without_reconnect():
    inbox = asyncio.Queue()
    calls = []
    async def get_message(timeout):
        calls.append(timeout)
        try:
            return await asyncio.wait_for(inbox.get(), .002)
        except TimeoutError:
            return None
    async def ping():
        await inbox.put({'type': 'pong'})
        await inbox.put({'type': 'message', 'channel': b'tick:EURUSD', 'data': b'{"bid":1}'})
    sub = subscriber.RedisSubscriber()
    sub._pubsub = SimpleNamespace(get_message=get_message, ping=AsyncMock(side_effect=ping))
    with patch.object(subscriber, 'IDLE_PING_SECONDS', .005):
        channel, payload = await asyncio.wait_for(anext(sub.listen()), .1)
    assert (channel, payload) == ('tick:EURUSD', {'bid': 1})
    assert calls and set(calls) == {1.0}
    sub._pubsub.ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_blackholed_subscription_fails_and_cancellation_propagates():
    async def idle(**kwargs):
        await asyncio.sleep(.002)
    sub = subscriber.RedisSubscriber()
    sub._pubsub = SimpleNamespace(get_message=idle, ping=AsyncMock())
    with patch.object(subscriber, 'IDLE_PING_SECONDS', .002), patch.object(subscriber, 'PONG_TIMEOUT_SECONDS', .005):
        with pytest.raises(RedisTimeoutError, match='pubsub_pong_timeout'):
            await asyncio.wait_for(anext(sub.listen_raw()), .1)
    task = asyncio.create_task(anext(sub.listen_raw()))
    await asyncio.sleep(.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('QUOTE_REDIS_TEST_PORT'), reason='requires isolated Redis')
async def test_real_redis_idle_beyond_socket_timeout_and_ping_interval():
    settings = SimpleNamespace(redis_host='127.0.0.1', redis_port=int(os.environ['QUOTE_REDIS_TEST_PORT']),
                               redis_password=None, redis_db=0)
    client = aioredis.Redis(host=settings.redis_host, port=settings.redis_port, socket_timeout=10)
    sub = subscriber.RedisSubscriber(settings)
    with patch.object(subscriber, 'get_redis_pool', return_value=client):
        try:
            await sub.connect()
            await sub.subscribe('quote-qualification:idle')
            pending = asyncio.create_task(anext(sub.listen()))
            await asyncio.sleep(32)  # two successful idle PING cycles; old code failed at10s
            assert not pending.done()
            await client.publish('quote-qualification:idle', '{"bid":1}')
            assert await asyncio.wait_for(pending, 2) == ('quote-qualification:idle', {'bid': 1})
        finally:
            await sub.close()
            await client.aclose()

"""Real PostgreSQL-only fixture: never points at production by default."""
import asyncio
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.db import symbol_management as sm

URL = os.environ.get('CHART_JOB_TEST_DSN', '')
pytestmark = [pytest.mark.asyncio, pytest.mark.skipif(not URL, reason='requires owned disposable PostgreSQL')]


async def test_atomic_duplicate_admission_cooldown_and_cancellation():
    engine = create_async_engine(URL, pool_size=10)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    values = {'symbol':'FIXTURE', 'target_type':'custom', 'timeframe':'T16000', 'source_type':'ticks',
              'source_timeframe':None, 'mode':'fill_missing', 'range_from':now-timedelta(days=7),
              'range_to':now-timedelta(days=1), 'requested_by':'fixture', '_chart_recovery':True}
    try:
        with patch('src.db.symbol_management.get_engine', return_value=engine), \
             patch('src.db.symbol_management.get_session_factory', return_value=factory):
            await sm.ensure_schema()
            results = await asyncio.gather(*(sm.create_job(dict(values)) for _ in range(20)), return_exceptions=True)
            accepted = [result for result in results if not isinstance(result, Exception)]
            assert sum(created for _job, created in accepted) == 1
            assert len({job['id'] for job, _ in accepted}) == 1
            assert all(isinstance(result, sm.HistoryJobCapacityError) for result in results if isinstance(result, Exception))
            job = accepted[0][0]
            # Another tick chart shares the existing source job, no duplicate work.
            same, created = await sm.create_job(dict(values, timeframe='T4000'))
            assert same['id'] == job['id'] and not created
            await sm.request_cancel(job['id'])
            assert (await sm.get_job(job['id']))['status'] == 'cancelled'
            with pytest.raises(sm.HistoryJobCapacityError):
                await sm.create_job(values)
            async with factory() as session, session.begin():
                await session.execute(text("UPDATE backfill_jobs SET created_at=NOW()-INTERVAL '3 hours'"))
            # Independent keys consume at most eight global slots.
            for i in range(8):
                await sm.create_job(dict(values, symbol=f'FIXTURE{i}'))
            with pytest.raises(sm.HistoryJobCapacityError):
                await sm.create_job(dict(values, symbol='EXCESS'))
    finally:
        await engine.dispose()

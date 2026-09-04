"""Integration against a disposable TimescaleDB 2.25.2 database only."""
import asyncio
import os
import subprocess
import time
from pathlib import Path

import asyncpg
import pytest


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('GUARD_TEST_CONTAINER'), reason='requires disposable TimescaleDB')
async def test_guard_is_idempotent_bounded_and_recovers():
    container = os.environ['GUARD_TEST_CONTAINER']
    assert container.startswith('mt5-stall-test-'), 'never run this fixture against production'
    def sql(query):
        return subprocess.run(['docker','exec','-i',container,'psql','-U','postgres','-X','-qAt','-v','ON_ERROR_STOP=1'], input=query, text=True, capture_output=True, check=True).stdout
    sql("""
      CREATE TABLE ticks(time_msc timestamptz NOT NULL,symbol text,bid double precision);
      SELECT create_hypertable('ticks','time_msc',chunk_time_interval=>interval '1 day');
      ALTER TABLE ticks SET(timescaledb.compress,timescaledb.compress_segmentby='symbol',timescaledb.compress_orderby='time_msc');
      INSERT INTO ticks SELECT now()-interval '10 days'+n*interval '1 second','TEST',1 FROM generate_series(1,100) n;
      SELECT add_compression_policy('ticks',interval '7 days',initial_start=>now()+interval '1 day');
    """)
    installer = Path('scripts/install_history_compression_guard.sql').read_text()
    sql(installer)
    sql(installer)
    assert sql("SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name='connector_guarded_compression'").strip() == '1'
    sql("SELECT alter_job(job_id,scheduled=>false) FROM timescaledb_information.jobs WHERE proc_name='connector_guarded_compression'")
    original = int(sql("SELECT job_id FROM timescaledb_information.jobs WHERE proc_name='policy_compression' AND hypertable_name='ticks'").strip())
    query = f"CALL public.connector_guarded_compression(0,'{{\"original_job_id\":{original}}}'::jsonb)"
    conn = await asyncpg.connect('postgresql://postgres:isolated-test-only@127.0.0.1:55439/postgres')
    try:
        await conn.execute('BEGIN')
        await conn.fetch('SELECT * FROM ticks')
        started=time.monotonic()
        result=await asyncio.to_thread(subprocess.run,['docker','exec',container,'psql','-U','postgres','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True)
        assert result.returncode != 0
        assert 'lock timeout' in result.stderr
        assert time.monotonic()-started < 4
        await conn.execute('ROLLBACK')
        # The failed dedicated connection releases its session advisory lock.
        assert await conn.fetchval('SELECT pg_try_advisory_lock(788601100001::bigint)')
        await conn.execute('SELECT pg_advisory_unlock(788601100001::bigint)')
        sql(query)
        assert sql("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='ticks' AND is_compressed").strip() == '1'
        assert sql('SELECT count(*) FROM ticks').strip() == '100'
        sql(Path('scripts/rollback_history_compression_guard.sql').read_text())
        assert sql("SELECT scheduled FROM timescaledb_information.jobs WHERE job_id="+str(original)).strip() == 't'
    finally:
        await conn.close()

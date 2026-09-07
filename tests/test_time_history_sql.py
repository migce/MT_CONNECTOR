"""Opt-in real Timescale test, ONLY against an owned disposable fixture."""
import json
import os
import subprocess
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.db.time_history import latest_time_bars

NAME = 'mt5-history-edges-sql-fixture'
pytestmark = pytest.mark.skipif(os.getenv('HISTORY_EDGES_SQL_FIXTURE') != '1', reason='owned Timescale fixture required')


def sql(statement):
    result = subprocess.run(['docker', 'exec', '-i', NAME, 'psql', '-U', 'postgres', '-qAt', '-v', 'ON_ERROR_STOP=1'],
                            input=statement, text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, 'fixture SQL failed: '+result.stderr[:400]
    return result.stdout.strip()


@pytest.fixture(scope='module')
def source():
    subprocess.run(['docker', 'run', '--rm', '-d', '--name', NAME, '-e', 'POSTGRES_PASSWORD=fixture',
                    'timescale/timescaledb:latest-pg16'], check=True, capture_output=True, timeout=30)
    try:
        import time
        for _ in range(40):
            ready = subprocess.run(['docker','exec',NAME,'pg_isready','-h','127.0.0.1','-U','postgres'], capture_output=True, timeout=3)
            if ready.returncode == 0: break
            time.sleep(.25)
        else: pytest.fail('fixture unavailable')
        sql('CREATE EXTENSION IF NOT EXISTS timescaledb; CREATE TABLE candles(time timestamptz, symbol text, timeframe text, open float8, high float8, low float8, close float8, tick_volume bigint, real_volume bigint, spread int, UNIQUE(symbol,timeframe,time));')
        sql("SELECT create_hypertable('candles','time');")
        # A weekend, a full holiday, intraday breaks, and a very short contract.
        start = datetime(2026, 8, 20, tzinfo=UTC)
        times = [start+timedelta(minutes=i) for i in range(19*1440)]
        times = [t for t in times if t.weekday()<5 and t.day != 1 and t.hour not in (2,3)]
        values = []
        for symbol, data in [('EURUSD', times), ('SHORT', times[-47:])]:
            for i,t in enumerate(data):
                values.append(f"('{t.isoformat()}','{symbol}','M1',{i},{i+2},{i-1},{i+1},1,0,2)")
        sql('INSERT INTO candles VALUES '+','.join(values)+';')
        yield times
    finally:
        # Exact owned fixture only; no host/prod volumes or other containers.
        subprocess.run(['docker', 'rm', '-f', '-v', NAME], check=True, capture_output=True, timeout=30)


@asynccontextmanager
async def fixture_session():
    class Session:
        async def execute(self, statement, params):
            from sqlalchemy.dialects import postgresql
            bound = statement.bindparams(**{k:v for k,v in params.items() if k in statement._bindparams})
            query = str(bound.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds':True}))
            output = json.loads(sql("BEGIN READ ONLY; SET LOCAL statement_timeout='10s'; SELECT COALESCE(json_agg(q),'[]') FROM ("+query+") q; ROLLBACK;"))
            for row in output: row['time'] = datetime.fromisoformat(row['time'])
            return SimpleNamespace(all=lambda:[SimpleNamespace(_mapping=r) for r in output])
    yield Session()


@pytest.mark.asyncio
@pytest.mark.parametrize('period,limit,symbol,anchor', [
    (180,1500,'EURUSD',None), (420,1500,'EURUSD',None),
    (180,1500,'EURUSD',datetime(2026,9,6,8,tzinfo=UTC)),
    (180,268,'SHORT',None), (180,1,'EURUSD',None),
    (180,1500,'EMPTY',None), (180,1500,'EURUSD',datetime(2023,1,1,tzinfo=UTC)),
    (180,1500,'EURUSD',datetime(2026,9,4,15,1,tzinfo=UTC)),
])
async def test_real_sql_matches_full_oracle_without_clipping_oldest_bucket(source, period, limit, symbol, anchor):
    with patch('src.db.time_history.heavy_read_session', fixture_session):
        actual = await latest_time_bars(symbol,'M1',period,'custom',limit,anchor)
    raw = source if symbol=='EURUSD' else source[-47:] if symbol=='SHORT' else []
    buckets = {}
    origin = datetime(2000,1,3,tzinfo=UTC)
    for i,t in enumerate(raw):
        if anchor and t>anchor: continue
        bucket = origin+timedelta(seconds=((t-origin).total_seconds()//period)*period)
        buckets.setdefault(bucket,[]).append(i)
    expected = sorted(buckets.items())[-limit:]
    assert len(actual) == len(expected)
    for row,(bucket,values) in zip(actual,expected):
        assert row['time'] == bucket
        assert (row['open'],row['close'],row['high'],row['low'],row['tick_volume']) == (values[0],values[-1]+1,max(values)+2,min(values)-1,len(values))

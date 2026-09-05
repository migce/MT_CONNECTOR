from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.api.routes import health


@pytest.mark.parametrize('age,expected',[(-3,False),(-0.35,True),(0,True),(29.9,True),(30,False),(float('nan'),False)])
def test_history_clock_and_staleness_bound(monkeypatch,age,expected):
    monkeypatch.setattr(health.time,'time',lambda:100)
    assert health._fresh_history({'observed_at':100-age}) is expected


@pytest.mark.asyncio
async def test_http_history_cache_never_extends_proof_or_hides_new_failure(monkeypatch):
    monkeypatch.setattr(health,'_history_cache',{})
    monkeypatch.setattr(health.time,'time',lambda:100)
    monkeypatch.setattr(health,'get_settings',lambda:SimpleNamespace(history_worker_enabled=True))
    healthy={'observed_at':99,'connected':True,'generation':'one'}
    get=AsyncMock(return_value=orjson.dumps(healthy))
    monkeypatch.setattr(health,'get_redis_pool',lambda:SimpleNamespace(get=get))
    app=FastAPI();app.include_router(health.router)
    async with AsyncClient(transport=ASGITransport(app=app),base_url='http://fixture') as client:
        first=await client.get('/api/v1/history/status')
        assert first.status_code==200 and first.json()==healthy
        get.return_value=None
        assert (await client.get('/api/v1/history/status')).json()==healthy
        failed={**healthy,'observed_at':100,'connected':False,'phase':'failed'}
        get.return_value=orjson.dumps(failed)
        assert (await client.get('/api/v1/history/status')).json()['connected'] is False
        get.return_value=orjson.dumps(healthy)  # older delivery cannot undo failure
        assert (await client.get('/api/v1/history/status')).json()['connected'] is False
        monkeypatch.setattr(health.time,'time',lambda:130)
        get.return_value=None
        assert (await client.get('/api/v1/history/status')).status_code==503

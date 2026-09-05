import sys
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest
from httpx import ASGITransport, AsyncClient

from src.config import Settings
from src.history_main import HistoryService
from src.history_status import HistoryWorkerUnavailable
from src.mt5.history_connection import HistoryConnection
from src.redis_bus.backfill_manager import BackfillRequester


def settings():
    return Settings(_env_file=None, mt5_login=123, mt5_password="fixture", mt5_server="fixture",
                    db_password="fixture", history_worker_enabled=True)


def terminal(**kwargs):
    return SimpleNamespace(**{**dict(data_path=r"C:\MT5_History", connected=True,
                                   trade_allowed=False, tradeapi_disabled=True), **kwargs})


@pytest.mark.asyncio
async def test_broker_outage_recovers_same_worker_with_bounded_backoff(monkeypatch):
    service = HistoryService(settings(), exit_process=lambda _: pytest.fail("no process exit"))
    samples = [terminal(connected=False)] * 8 + [terminal()]
    monkeypatch.setitem(sys.modules, "MetaTrader5", SimpleNamespace(
        terminal_info=lambda: samples.pop(0), account_info=lambda: SimpleNamespace(login=123)))
    async def native(fn): return fn()
    monkeypatch.setattr("src.history_main.run_in_mt5", native)
    pauses = []
    async def pause(delay):
        assert service.phase == "reconnecting" and not service.connected
        pauses.append(delay)
    monkeypatch.setattr("src.history_main.asyncio.sleep", pause)
    generation = service.generation
    await service.verify_connection()
    assert service.connected and service.generation == generation
    assert pauses == [2, 4, 8, 16, 30, 30, 30, 30]
    assert service.last_success and service.reconnect_attempts == 0


@pytest.mark.parametrize("bad", ["path", "account", "trading", "python"])
def test_safety_drift_is_fatal_even_while_disconnected(monkeypatch, bad):
    info = terminal(connected=False)
    account = SimpleNamespace(login=123)
    if bad == "path": info.data_path = r"C:\Other"
    if bad == "account": account.login = 999
    if bad == "trading": info.trade_allowed = True
    if bad == "python": info.tradeapi_disabled = False
    monkeypatch.setitem(sys.modules, "MetaTrader5", SimpleNamespace(
        terminal_info=lambda: info, account_info=lambda: account))
    with pytest.raises(RuntimeError, match="proof failed"):
        HistoryConnection(settings()).connection_proof()


@pytest.mark.parametrize("info,account", [(None, None), (terminal(connected=False), None)])
def test_missing_ipc_proof_waits_without_dequeuing_or_initializing(monkeypatch, info, account):
    monkeypatch.setitem(sys.modules, "MetaTrader5", SimpleNamespace(
        terminal_info=lambda: info, account_info=lambda: account))
    assert HistoryConnection(settings()).connection_proof() is False


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [None, {"observed_at": 1, "connected": True},
                                     {"phase": "reconnecting", "connected": False}])
async def test_unavailable_worker_fails_before_queue_write(payload):
    requester = BackfillRequester(settings())
    if payload and "observed_at" not in payload: payload = {**payload, "observed_at": time.time()}
    async def get(key): return orjson.dumps(payload) if key == "history:status" and payload else None
    redis = SimpleNamespace(get=AsyncMock(side_effect=get), set=AsyncMock(), rpush=AsyncMock())
    requester._redis = redis
    with pytest.raises(HistoryWorkerUnavailable):
        await requester.request_and_wait("USTEC", "candles", datetime(2026, 7, 1, tzinfo=UTC),
                                         datetime(2026, 9, 1, tzinfo=UTC), timeframe="H1")
    redis.set.assert_not_awaited()
    redis.rpush.assert_not_awaited()


@pytest.mark.asyncio
async def test_cached_success_does_not_require_worker():
    requester = BackfillRequester(settings())
    requester._redis = SimpleNamespace(get=AsyncMock(return_value=b'{"status":"ok","rows":2}'))
    now = datetime.now(UTC)
    assert (await requester.request_and_wait("USTEC", "candles", now, now))["rows"] == 2


@pytest.mark.asyncio
async def test_worker_loss_during_wait_propagates_and_closes_subscription():
    requester = BackfillRequester(settings())
    ps = SimpleNamespace(subscribe=AsyncMock(), unsubscribe=AsyncMock(), close=AsyncMock())
    requester._redis = SimpleNamespace(get=AsyncMock(return_value=None), pubsub=lambda: ps)
    requester._require_history_worker = AsyncMock(side_effect=HistoryWorkerUnavailable("reconnecting"))
    with pytest.raises(HistoryWorkerUnavailable):
        await requester._wait_for_response("fixture", 60)
    ps.unsubscribe.assert_awaited_once()
    ps.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_http_history_error_has_distinct_code_and_retry(monkeypatch):
    from src.api.app import create_app
    app = create_app()
    @app.get("/fixture-history")
    async def fixture(): raise HistoryWorkerUnavailable("reconnecting")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://fixture") as client:
        response = await client.get("/fixture-history")
    assert response.status_code == 503
    assert response.json()["code"] == "history_worker_unavailable"
    assert response.json()["history_phase"] == "reconnecting"
    assert response.headers["retry-after"] == "15"

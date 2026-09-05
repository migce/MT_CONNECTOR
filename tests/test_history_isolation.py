from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio
import sys
import os
import multiprocessing
import time

import pytest

from src.config import Settings, Timeframe
from src.mt5.history_connection import HistoryConnection, validate_history_path
from src.mt5.history_queue import RemoteHistory, plan_ranges


def settings(**kw):
    return Settings(_env_file=None, mt5_login=123, mt5_password="fixture",
                    mt5_server="fixture", db_password="fixture", **kw)


@pytest.mark.parametrize("path", [r"C:\MT5_Portable\123\terminal64.exe",
                                  r"C:\Program Files\MetaTrader 5\terminal64.exe",
                                  "terminal64.exe", r"C:\MT5_History\other.exe"])
def test_reject_live_or_ambiguous_terminal(path):
    with pytest.raises(ValueError):
        validate_history_path(settings(history_mt5_path=path))


def test_default_is_opt_in_and_path_separate():
    s = settings()
    assert not s.history_worker_enabled
    assert validate_history_path(s) == r"c:\mt5_history\terminal64.exe"


@pytest.mark.parametrize("bad", ["account", "path", "trading", "python", "offline"])
def test_native_identity_fail_closed(monkeypatch, bad):
    terminal = SimpleNamespace(data_path=r"C:\MT5_History", connected=True,
                               trade_allowed=False, tradeapi_disabled=True)
    account = SimpleNamespace(login=123)
    if bad == "account": account.login = 999
    if bad == "path": terminal.data_path = r"C:\MT5_Portable\123"
    if bad == "trading": terminal.trade_allowed = True
    if bad == "python": terminal.tradeapi_disabled = False
    if bad == "offline": terminal.connected = False
    calls = []
    monkeypatch.setitem(sys.modules, "MetaTrader5", SimpleNamespace(
        initialize=lambda **kw: calls.append(kw["portable"]) or True,
        terminal_info=lambda: terminal, account_info=lambda: account,
        shutdown=lambda: calls.append("shutdown")))
    monkeypatch.setattr("src.mt5.portable.start_terminal_protected", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="proof failed"):
        HistoryConnection(settings())._try_connect()
    assert calls == [True, "shutdown"]


def test_freeze_original_gap_and_settlement_before_live_advances():
    now = datetime(2026, 9, 5, 12, tzinfo=UTC)
    states = [dict(symbol="EURUSD", data_type="M1", last_synced_at=now-timedelta(days=3)),
              dict(symbol="EURUSD", data_type="tick", last_synced_at=now-timedelta(days=3))]
    jobs = plan_ranges(["EURUSD"], settings(timeframes_csv="M1"), states, now)
    states[0]["last_synced_at"] = now
    assert jobs[0][4] == now-timedelta(days=3)
    assert jobs[0][5] == now-timedelta(minutes=1)
    assert jobs[1][4] == now-timedelta(days=3)


def test_reconnect_uses_pre_disconnect_clock_not_advanced_watermark():
    now = datetime(2026, 9, 5, 12, tzinfo=UTC)
    start = now-timedelta(hours=6)
    jobs = plan_ranges(["EURUSD"], settings(), [], now, start)
    assert jobs[-1][4] == start


@pytest.mark.asyncio
async def test_live_side_only_submits_durable_jobs(monkeypatch):
    freeze = AsyncMock(return_value=datetime.now(UTC))
    monkeypatch.setattr("src.mt5.history_queue.freeze_ranges", freeze)
    remote = RemoteHistory(settings())
    remote.update_symbols(["EURUSD"])
    await remote.run_initial_backfill()
    remote.mark_live(1000)
    await remote.run_reconnect_backfill()
    assert freeze.await_count == 2
    assert freeze.call_args.args[2] == datetime.fromtimestamp(1000, UTC)
    task = asyncio.create_task(remote.start_scheduled_gap_scan())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task


@pytest.mark.asyncio
async def test_overdue_native_exits_history_only(monkeypatch):
    from src.history_main import HistoryService
    exits = []
    monkeypatch.setattr("src.history_main._native_budget.status", lambda: {"overdue": 1})
    redis = SimpleNamespace(set=AsyncMock())
    monkeypatch.setattr("src.history_main.get_redis_pool", lambda: redis)
    service = HistoryService(settings(), exit_process=exits.append)
    await service.publish_health()
    assert exits == [70]
    assert redis.set.call_args.args[0] == "history:status"


def _hung_history_process():
    """Synthetic native DLL stall in a disposable OS process, never MT5."""
    from concurrent.futures import ThreadPoolExecutor
    from src.mt5.native_budget import NativeCallBudget
    from src.history_main import HistoryService
    import src.history_main as module
    budget = NativeCallBudget(ThreadPoolExecutor(max_workers=1), timeout=0.03)
    module._native_budget = budget
    module.get_redis_pool = lambda: SimpleNamespace(set=AsyncMock())
    async def run():
        try:
            await budget.run(time.sleep, 3600)
        except TimeoutError:
            await HistoryService(settings()).publish_health()
    asyncio.run(run())


def test_actual_history_process_exit_leaves_live_native_lane_usable():
    from concurrent.futures import ThreadPoolExecutor
    from src.mt5.native_budget import NativeCallBudget
    child = multiprocessing.get_context("spawn").Process(target=_hung_history_process)
    child.start()
    child.join(timeout=10)
    try:
        assert not child.is_alive() and child.exitcode == 70
        with ThreadPoolExecutor(max_workers=1) as live_executor:
            assert asyncio.run(NativeCallBudget(live_executor).run(lambda: "live")) == "live"
    finally:
        if child.is_alive(): child.kill(); child.join(timeout=3)


@pytest.mark.asyncio
async def test_failed_reconnect_persistence_retains_original_gap(monkeypatch):
    freeze = AsyncMock(side_effect=TimeoutError)
    monkeypatch.setattr("src.mt5.history_queue.freeze_ranges", freeze)
    remote = RemoteHistory(settings())
    remote.mark_live(1000)
    with pytest.raises(TimeoutError): await remote.run_reconnect_backfill()
    remote.mark_live(2000)
    assert remote.pending_reconnect and remote.last_seen == datetime.fromtimestamp(1000, UTC)


@pytest.mark.asyncio
async def test_tick_empty_ranges_are_bounded_without_skipping_time(monkeypatch):
    from src.mt5.backfill import Backfiller
    calls = []
    async def native(func, symbol, start, end):
        calls.append((start,end))
        return []
    monkeypatch.setattr("src.mt5.backfill.run_in_mt5", native)
    bf = Backfiller(None, settings(history_worker_enabled=True))
    start = datetime(2026,9,5,12,tzinfo=UTC)
    await bf.on_demand_ticks("TEST",start,start+timedelta(minutes=3))
    assert calls == [(start+timedelta(minutes=i),start+timedelta(minutes=i+1)) for i in range(3)]


@pytest.mark.asyncio
async def test_oversized_ticks_fail_before_python_expansion(monkeypatch):
    from src.mt5.backfill import Backfiller
    class Huge:
        nbytes = 17*1024*1024
        def __len__(self): return 100
    monkeypatch.setattr("src.mt5.backfill.run_in_mt5", AsyncMock(return_value=Huge()))
    monkeypatch.setattr("src.mt5.backfill.ticks_to_dicts", lambda *a: pytest.fail("must not expand"))
    start=datetime(2026,9,5,12,tzinfo=UTC)
    with pytest.raises(RuntimeError,match="safety budget"):
        await Backfiller(None,settings(history_worker_enabled=True)).on_demand_ticks("TEST",start,start+timedelta(minutes=1))

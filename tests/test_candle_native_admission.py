import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.mt5 import collector as module
from src.mt5.collector import Collector
from src.mt5.native_budget import NativeCallBudget


@pytest.mark.asyncio
async def test_full_symbol_timeframe_cycle_respects_native_admission(monkeypatch):
    executor = ThreadPoolExecutor(max_workers=1)
    budget = NativeCallBudget(executor, capacity=8, timeout=2)
    collector = Collector.__new__(Collector)
    collector._active_symbols = [f"TEST{i}" for i in range(35)]
    collector._metrics = SimpleNamespace(record_error=Mock())
    collector._persisted_candle_signatures = {}
    calls = []

    def rates(symbol, timeframe, _offset, _count):
        time.sleep(0.001)
        calls.append((symbol, timeframe))
        return None

    collector._get_rates = rates
    monkeypatch.setattr(module, "run_in_mt5", budget.run)
    frames = [SimpleNamespace(value=f"TF{i}", mt5_constant=i) for i in range(3)]
    try:
        cycle = asyncio.create_task(collector._run_candle_cycle(frames))
        await asyncio.sleep(0.01)
        # Ticks/heartbeat must still have room while the candle matrix runs.
        assert await budget.run(lambda: "heartbeat") == "heartbeat"
        await cycle
        assert len(calls) == 105
        assert len(set(calls)) == 105
        collector._metrics.record_error.assert_not_called()
        assert budget.status()["pending"] == 0
    finally:
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_candle_cycle_cancellation_does_not_submit_remaining_matrix(monkeypatch):
    collector = Collector.__new__(Collector)
    collector._active_symbols = ["A", "B", "C"]
    called = []
    started = asyncio.Event()

    async def native(_fn, symbol, *_args):
        called.append(symbol)
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(module, "run_in_mt5", native)
    task = asyncio.create_task(collector._run_candle_cycle([SimpleNamespace(value="M1", mt5_constant=1)]))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert called == ["A"]

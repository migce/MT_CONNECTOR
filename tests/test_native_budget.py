import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.mt5.native_budget import NativeCallBudget, NativeCallUnavailable


@pytest.mark.asyncio
async def test_native_timeout_retains_capacity_until_real_completion():
    release = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    budget = NativeCallBudget(executor, timeout=0.03)
    try:
        with pytest.raises(TimeoutError):
            await budget.run(release.wait, 2)
        assert budget.status()["overdue"] == 1
        with pytest.raises(NativeCallUnavailable):
            await budget.run(lambda: 1)
        release.set()
        for _ in range(50):
            if budget.status()["pending"] == 0:
                break
            await asyncio.sleep(0.01)
        assert await budget.run(lambda: 3) == 3
    finally:
        release.set()
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_cancelled_queued_native_call_never_executes():
    started, release, called = threading.Event(), threading.Event(), threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    budget = NativeCallBudget(executor, capacity=2, timeout=2)

    def blocked():
        started.set()
        release.wait(3)

    first = asyncio.create_task(budget.run(blocked))
    second = None
    try:
        await asyncio.to_thread(started.wait, 1)
        second = asyncio.create_task(budget.run(called.set))
        await asyncio.sleep(0.02)
        with pytest.raises(NativeCallUnavailable):
            await budget.run(lambda: None)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        release.set()
        await first
        assert not called.is_set()
    finally:
        release.set()
        await asyncio.gather(first, *([second] if second else []), return_exceptions=True)
        executor.shutdown(wait=True)

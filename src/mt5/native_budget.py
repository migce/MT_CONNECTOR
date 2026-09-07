"""Bound polling native calls without pretending cancellation stops a native thread."""
from __future__ import annotations

import asyncio
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from concurrent.futures import Executor, Future


class NativeCallUnavailable(TimeoutError):  # noqa: N818 - availability boundary, not native failure
    """The polling native lane cannot safely accept more work."""


class NativeCallBudget:
    def __init__(self, executor: Executor, *, capacity: int = 8, timeout: float = 90):
        self.executor = executor
        self.capacity = capacity
        self.timeout = timeout
        self._lock = threading.Lock()
        self._pending: set[Future] = set()
        self._overdue: set[Future] = set()
        self._last_completed = 0.0
        # Set only by the isolated history process. Live and Trader keep the
        # default None and never acquire a history admission dependency.
        self.before_run = None

    def status(self) -> dict:
        with self._lock:
            return {"pending": len(self._pending), "overdue": len(self._overdue),
                    "last_completed_monotonic": self._last_completed}

    async def run(self, function, *args, **kwargs):
        if self.before_run is not None:
            await self.before_run()
        deadline = time.monotonic() + self.timeout

        def invoke():
            if time.monotonic() >= deadline:
                raise NativeCallUnavailable("Native request expired in queue")
            return function(*args, **kwargs)

        def finished(future):
            with self._lock:
                self._pending.discard(future)
                self._overdue.discard(future)
                if not future.cancelled():
                    self._last_completed = time.monotonic()

        with self._lock:
            if self._overdue or len(self._pending) >= self.capacity:
                raise NativeCallUnavailable("Native polling lane saturated or overdue")
            future = self.executor.submit(invoke)
            self._pending.add(future)
        future.add_done_callback(finished)
        wrapped = asyncio.wrap_future(future)
        wrapped.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        try:
            return await asyncio.wait_for(asyncio.shield(wrapped), self.timeout)
        except (TimeoutError, asyncio.CancelledError):
            # Cancel only queued work. A running native call retains capacity
            # and fences new work until it really finishes; never spawn another
            # thread against the same terminal to hide a hang.
            if not future.cancel():
                with self._lock:
                    if not future.done():
                        self._overdue.add(future)
            raise

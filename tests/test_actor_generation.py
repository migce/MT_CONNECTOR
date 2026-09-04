import asyncio
import queue
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.mt5.trading import IPC_PROTOCOL_VERSION, AccountSession, _account_worker


def test_child_rejects_old_generation_and_echoes_exact_identity(monkeypatch):
    terminal_calls = MagicMock()
    fake_mt5 = SimpleNamespace(initialize=terminal_calls, shutdown=terminal_calls)
    fake_portable = SimpleNamespace(
        minimize_terminal_window=terminal_calls, prepare_terminal=terminal_calls,
        start_terminal_protected=terminal_calls, stop_terminal_process=MagicMock(),
    )
    monkeypatch.setitem(sys.modules, "MetaTrader5", fake_mt5)
    monkeypatch.setitem(sys.modules, "src.mt5.portable", fake_portable)
    commands, responses = queue.Queue(), queue.Queue()
    base = {"protocol": IPC_PROTOCOL_VERSION, "deadline_mono": time.monotonic()+5}
    commands.put({**base, "cmd": "connect", "request_id": "old", "generation": "old"})
    commands.put({**base, "cmd": "health", "request_id": "health-1", "generation": "new"})
    commands.put({**base, "cmd": "shutdown", "request_id": "stop-1", "generation": "new"})
    _account_worker(1, "test", "test", "test", commands, responses, "new")
    terminal_calls.assert_not_called()
    assert responses.qsize() == 2
    assert responses.get() == {
        "cmd": "health", "alive": False, "protocol": IPC_PROTOCOL_VERSION,
        "generation": "new", "request_id": "health-1",
    }


@pytest.mark.asyncio
async def test_dead_child_dispatcher_cannot_survive_into_new_generation():
    session = AccountSession(1, 1, "test", "test", "test")
    process = MagicMock()
    process.is_alive.return_value = False
    session._process = process
    dispatcher = asyncio.create_task(session._request_dispatcher())
    session._dispatcher_task = dispatcher
    await asyncio.sleep(0)
    await session.disconnect()
    assert dispatcher.done()
    assert session._dispatcher_task is None
    assert session._process is None
    assert session._ipc_faulted


@pytest.mark.asyncio
async def test_health_probe_does_not_recycle_a_busy_close_within_its_deadline():
    session = AccountSession(1, 1, "test", "test", "test")
    session._connected = True
    session._process = MagicMock()
    session._process.is_alive.return_value = True
    session._active_request = {"cmd": "close_position", "deadline_mono": time.monotonic()+2}
    session._request = AsyncMock(return_value=None)
    assert await session.check_health()
    session._request.assert_not_awaited()
    session._active_request["deadline_mono"] = time.monotonic()-1
    assert not await session.check_health()

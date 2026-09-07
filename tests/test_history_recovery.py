import asyncio
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.history_authority import HistoryAuthority, HistoryAuthorityConflict
from src.history_supervisor import HistorySupervisor, spawn_owned_python
from src.mt5.native_budget import NativeCallBudget


def proof(owner="poller-one", isolated=True, ttl=10_000):
    return [json.dumps({"history_isolated": isolated, "started_at": owner}), ttl]


@pytest.mark.asyncio
async def test_missing_status_pauses_native_and_recovers_same_authority():
    redis = SimpleNamespace(eval=AsyncMock(side_effect=[proof(), [b"", -2], [b"", -2], proof()]))
    pauses = []
    calls = []
    async def pause(delay):
        assert calls == [1] and not authority.ready
        pauses.append(delay)
    authority = HistoryAuthority(lambda: redis, sleep=pause)
    with ThreadPoolExecutor(max_workers=1) as executor:
        native = NativeCallBudget(executor)
        native.before_run = authority.wait
        await native.run(lambda: calls.append(1))
        await native.run(lambda: calls.append(2))
    assert pauses == [1, 2] and calls == [1, 2]
    assert authority.owner == "poller-one" and authority.ready


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [proof(isolated=False), proof(owner="poller-two")])
async def test_positive_conflict_is_sticky_even_if_next_status_looks_good(payload):
    redis = SimpleNamespace(eval=AsyncMock(side_effect=[proof(), payload, proof()]))
    authority = HistoryAuthority(lambda: redis)
    assert await authority.check()
    for _ in range(2):
        with pytest.raises(HistoryAuthorityConflict):
            await authority.check()
        assert not authority.ready
    assert redis.eval.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [[b"", -2], [b"{}", 1000], [b"[]", 1000],
                                     [b"invalid", 1000], proof(ttl=-1), proof(ttl=0),
                                     proof(ttl=10_001), proof(owner=None), proof(isolated="true")])
async def test_missing_or_invalid_proof_never_authorizes_native(payload):
    authority = HistoryAuthority(lambda: SimpleNamespace(eval=AsyncMock(return_value=payload)))
    assert not await authority.check()
    assert not authority.ready and authority.conflict is None


@pytest.mark.asyncio
async def test_transport_failure_revokes_proof_but_is_recoverable():
    from redis.exceptions import ConnectionError
    redis = SimpleNamespace(eval=AsyncMock(side_effect=[proof(), ConnectionError("private endpoint"), proof()]))
    authority = HistoryAuthority(lambda: redis)
    assert await authority.check()
    assert not await authority.check() and not authority.ready
    assert authority.reason == "poller_status_read_failed"
    assert await authority.check()


@pytest.mark.asyncio
async def test_original_ttl_and_read_duration_bound_proof():
    now = [100.0]
    async def read(*_):
        now[0] += .2
        return proof(ttl=100)
    authority = HistoryAuthority(lambda: SimpleNamespace(eval=read), clock=lambda: now[0])
    assert not await authority.check()
    authority.redis = lambda: SimpleNamespace(eval=AsyncMock(return_value=proof(ttl=100)))
    assert await authority.check()
    now[0] += .101
    assert not authority.ready


@pytest.mark.asyncio
async def test_waiting_cancel_has_no_native_submission():
    entered = asyncio.Event()
    async def pause(_):
        entered.set()
        await asyncio.Event().wait()
    authority = HistoryAuthority(lambda: SimpleNamespace(eval=AsyncMock(return_value=[b"", -2])), sleep=pause)
    with ThreadPoolExecutor(max_workers=1) as executor:
        native = NativeCallBudget(executor)
        native.before_run = authority.wait
        task = asyncio.create_task(native.run(lambda: pytest.fail("no native work")))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert native.status()["pending"] == native.status()["overdue"] == 0


@pytest.mark.asyncio
async def test_live_budget_has_no_history_dependency():
    with ThreadPoolExecutor(max_workers=1) as executor:
        native = NativeCallBudget(executor)
        assert native.before_run is None
        assert await native.run(lambda: "live") == "live"


def test_real_owned_interpreter_pid_venv_and_dependencies(tmp_path):
    code = "import os,sys,json,redis;print(json.dumps(dict(pid=os.getpid(),prefix=sys.prefix)))"
    child = spawn_owned_python(["-c", code], cwd=tmp_path, stdout=subprocess.PIPE)
    try:
        output, _ = child.communicate(timeout=8)
        assert child.returncode == 0
        actual = json.loads(output)
        assert actual["pid"] == child.pid
        assert actual["prefix"] == sys.prefix
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)


def test_real_owned_interpreter_termination_leaves_no_redirected_child(tmp_path):
    code = "import os,time;print(os.getpid(),flush=True);time.sleep(60)"
    child = spawn_owned_python(["-c", code], cwd=tmp_path, stdout=subprocess.PIPE)
    supervisor = HistorySupervisor(tmp_path)
    supervisor.child = child
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            child.communicate(timeout=1)
    finally:
        supervisor.stop_owned_child()
    output, _ = child.communicate(timeout=5)
    assert int(output) == child.pid
    assert child.poll() is not None and child.returncode != 0


class Child:
    pid = 123
    def __init__(self, code):
        self.code = code
        self.killed = False
    def poll(self): return self.code
    def wait(self, timeout): return self.code
    def kill(self): self.killed = True; self.code = -9


def test_supervisor_bounds_retries_and_persists_circuit(tmp_path):
    children, pauses = [], []
    def spawn():
        child = Child(70); children.append(child); return child
    supervisor = HistorySupervisor(tmp_path, spawn=spawn, sleep=pauses.append)
    assert supervisor.run() == 78
    assert len(children) == 4 and pauses == [10, 30, 60]
    assert not any(x.killed for x in children)
    assert json.loads(supervisor.path.read_text())["state"] == "circuit_open"
    assert HistorySupervisor(tmp_path, spawn=lambda: pytest.fail("circuit persists")).run() == 78


def test_supervisor_does_not_retry_safety_conflict(tmp_path):
    supervisor = HistorySupervisor(tmp_path, spawn=lambda: Child(78), sleep=lambda _: pytest.fail("no retry"))
    assert supervisor.run() == 78
    assert json.loads(supervisor.path.read_text())["reason"] == "safety_conflict"


def test_supervisor_kills_only_its_stalled_owned_child(tmp_path):
    now = [100.0]
    first, second = Child(None), Child(78)
    children = [first, second]
    def pause(delay): now[0] += delay
    supervisor = HistorySupervisor(tmp_path, spawn=lambda: children.pop(0), clock=lambda: now[0],
                                   sleep=pause, startup_grace=3, heartbeat_deadline=2)
    assert supervisor.run() == 78
    assert first.killed and not second.killed


def test_waiting_authority_progress_is_not_treated_as_hang(tmp_path):
    now = [100.0]
    child = Child(None)
    def pause(delay):
        now[0] += delay
        if now[0] >= 110: child.code = 78
    supervisor = HistorySupervisor(tmp_path, spawn=lambda: child, clock=lambda: now[0],
                                   sleep=pause, startup_grace=3, heartbeat_deadline=2)
    def health():
        return {"pid": child.pid, "observed_at": now[0], "connected": False, "phase": "reconnecting"}
    supervisor.health = health
    assert supervisor.run() == 78 and not child.killed


def test_old_child_heartbeat_cannot_keep_new_child_healthy(tmp_path):
    (tmp_path / ".runtime").mkdir()
    supervisor = HistorySupervisor(tmp_path, clock=lambda: 100)
    supervisor.child = Child(None)
    for data in ({"pid": 1, "observed_at": 100}, {"pid": 123, "observed_at": 1},
                 {"pid": 123, "observed_at": 101}):
        supervisor.child_health.write_text(json.dumps(data))
        assert supervisor.health() is None


def test_existing_terminal_query_is_bounded_and_exact(monkeypatch):
    from src.mt5.history_connection import existing_history_terminal
    def query(*args, **kwargs):
        assert kwargs["timeout"] == 5
        return json.dumps([{"ExecutablePath": r"C:\MT5_History\terminal64.exe", "ProcessId": 456},
                           {"ExecutablePath": r"C:\Other\terminal64.exe", "ProcessId": 789}])
    monkeypatch.setattr(subprocess, "check_output", query)
    assert existing_history_terminal(r"c:\mt5_history\terminal64.exe") == 456


@pytest.mark.parametrize("output", ["[]", json.dumps([
    {"ExecutablePath": r"C:\MT5_History\terminal64.exe", "ProcessId": 1},
    {"ExecutablePath": r"C:\MT5_History\terminal64.exe", "ProcessId": 2}])])
def test_missing_or_duplicate_terminal_never_launches(monkeypatch, output):
    from src.mt5.history_connection import existing_history_terminal
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: output)
    with pytest.raises(HistoryAuthorityConflict):
        existing_history_terminal(r"c:\mt5_history\terminal64.exe")


def test_attach_only_reuses_without_protected_replacement(monkeypatch):
    import sys
    from src.mt5.history_connection import HistoryConnection
    from src.config import Settings
    settings = Settings(_env_file=None, mt5_login=123, mt5_password="fixture", mt5_server="fixture", db_password="fixture")
    monkeypatch.setattr("src.mt5.history_connection.existing_history_terminal", lambda _: 456)
    monkeypatch.setattr("src.mt5.portable.start_terminal_protected", lambda *a, **k: pytest.fail("no launch or replacement"))
    def initialize(**kw):
        assert not {"login", "password", "server"}.intersection(kw)
        return True
    monkeypatch.setitem(sys.modules, "MetaTrader5", SimpleNamespace(
        initialize=initialize,
        terminal_info=lambda: SimpleNamespace(data_path=r"C:\MT5_History", connected=True, trade_allowed=False, tradeapi_disabled=True),
        account_info=lambda: SimpleNamespace(login=123)))
    assert HistoryConnection(settings, attach_only=True)._try_connect()


@pytest.mark.asyncio
async def test_service_supervision_survives_missing_proof_then_fails_on_conflict(monkeypatch):
    from contextlib import asynccontextmanager
    from src.config import Settings
    import src.history_main as module
    settings = Settings(_env_file=None, mt5_login=123, mt5_password="fixture", mt5_server="fixture", db_password="fixture")
    payload = [proof()]
    redis = SimpleNamespace(eval=AsyncMock(side_effect=lambda *a: payload[0]), set=AsyncMock())
    lease = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar=lambda: 42)), commit=AsyncMock())
    @asynccontextmanager
    async def connection(): yield lease
    monkeypatch.setattr(module, "get_engine", lambda *a: SimpleNamespace(connect=connection))
    monkeypatch.setattr(module, "get_redis_pool", lambda: redis)
    budget = SimpleNamespace(before_run=None, status=lambda: {"overdue": 0})
    monkeypatch.setattr(module, "_native_budget", budget)
    exits = []
    service = module.HistoryService(settings, exit_process=exits.append)
    submissions = []
    async def work():
        while True:
            await budget.before_run()
            submissions.append(1)
            await asyncio.sleep(.01)
    service.work = work
    task = asyncio.create_task(service.run())
    async def until(predicate):
        async with asyncio.timeout(3):
            while not predicate(): await asyncio.sleep(.005)
    try:
        await until(lambda: bool(submissions))
        generation = service.generation
        payload[0] = [b"", -2]
        await until(lambda: service.authority.reason == "poller_status_missing")
        count = len(submissions)
        await asyncio.sleep(.1)
        assert len(submissions) == count and not task.done() and not exits
        payload[0] = proof()
        await until(lambda: len(submissions) > count)
        assert service.generation == generation
        payload[0] = proof(isolated=False)
        await asyncio.wait_for(task, 3)
        assert exits == [78] and budget.before_run is None
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_startup_without_proof_has_no_database_reconciliation_or_mt5_connect(monkeypatch):
    from src.config import Settings
    import src.history_main as module
    service = module.HistoryService(Settings(_env_file=None, mt5_login=123, mt5_password="fixture", mt5_server="fixture", db_password="fixture"))
    service.authority.wait = AsyncMock(side_effect=asyncio.CancelledError)
    service.connection.connect = AsyncMock()
    monkeypatch.setattr(module, "get_engine", lambda *a: pytest.fail("no reconciliation"))
    with pytest.raises(asyncio.CancelledError): await service.work()
    service.connection.connect.assert_not_awaited()

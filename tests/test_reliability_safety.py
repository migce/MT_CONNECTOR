from __future__ import annotations

import asyncio
import queue
import time
from collections import deque
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.api.services.position_snapshot import validate_position_snapshot
from src.mt5 import collector as collector_module
from src.mt5.collector import Collector
from src.mt5.trading import IPC_QUEUE_CAPACITY, AccountSession, execute_close_position


class FakeMT5:
    TRADE_ACTION_DEAL = 1
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    POSITION_TYPE_BUY = 0
    ORDER_TIME_GTC = 0
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    SYMBOL_TRADE_EXECUTION_MARKET = 2
    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_DONE_PARTIAL = 10010
    TRADE_RETCODE_PLACED = 10008
    TRADE_RETCODE_INVALID_FILL = 10030
    TRADE_RETCODE_TIMEOUT = 10012
    TRADE_RETCODE_REQUOTE = 10004
    TRADE_RETCODE_MARKET_CLOSED = 10018
    TRADE_RETCODE_PRICE_CHANGED = 10020
    TRADE_RETCODE_PRICE_OFF = 10021
    TRADE_RETCODE_TOO_MANY_REQUESTS = 10024
    TRADE_RETCODE_LOCKED = 10028
    TRADE_RETCODE_CONNECTION = 10031

    def __init__(self, *, send_mode: str = "done") -> None:
        self.send_mode = send_mode
        self.position = SimpleNamespace(
            ticket=77,
            identifier=7007,
            symbol="EURUSD",
            type=0,
            magic=7111109,
            volume=1.2,
        )
        self.sent: list[dict] = []

    def positions_get(self, *, ticket: int):
        if self.position is None or ticket != self.position.ticket:
            return ()
        return (self.position,)

    def last_error(self):
        return (0, "ok")

    def account_info(self):
        return SimpleNamespace(login=123, trade_allowed=True, trade_expert=True)

    def terminal_info(self):
        return SimpleNamespace(trade_allowed=True)

    def symbol_info(self, _symbol: str):
        return SimpleNamespace(
            visible=True,
            filling_mode=3,
            trade_exemode=self.SYMBOL_TRADE_EXECUTION_MARKET,
            volume_step=0.01,
        )

    def symbol_select(self, _symbol: str, _selected: bool):
        return True

    def symbol_info_tick(self, _symbol: str):
        return SimpleNamespace(bid=1.1, ask=1.2)

    def order_check(self, request: dict):
        return {"retcode": 0, "comment": "Done", "request": request}

    def order_send(self, request: dict):
        self.sent.append(dict(request))
        if self.send_mode == "timeout":
            return None
        if self.send_mode == "partial" and len(self.sent) == 1:
            self.position.volume = 0.4
            return {"retcode": self.TRADE_RETCODE_DONE_PARTIAL, "comment": "Partial"}
        self.position = None
        return {"retcode": self.TRADE_RETCODE_DONE, "comment": "Done"}


def close_message(**overrides):
    payload = {
        "command_id": "0c86611c-6814-4bcc-8cd6-4f7d2b548b97",
        "position_ticket": 77,
        "expected_login": 123,
        "expected_position_identifier": 7007,
        "expected_symbol": "EURUSD",
        "expected_type": 0,
        "expected_magic": 7111109,
        "max_volume": 1.2,
        "deviation_points": 100,
        "send_attempts": 3,
        "reconcile_timeout_sec": 0.01,
        "deadline_mono": time.monotonic() + 60,
        "expires_at": (datetime.now(UTC) + timedelta(seconds=60)).isoformat(),
    }
    payload.update(overrides)
    return payload


def test_exact_position_is_closed_and_broker_state_is_confirmed():
    mt5 = FakeMT5()
    result = execute_close_position(mt5, close_message(), sleep_fn=lambda _seconds: None)

    assert result["status"] == "confirmed"
    assert result["remaining_volume"] == 0
    assert len(mt5.sent) == 1
    assert mt5.sent[0]["position"] == 77
    assert mt5.sent[0]["type"] == mt5.ORDER_TYPE_SELL
    assert "price" not in mt5.sent[0]  # market execution


def test_changed_position_identity_is_rejected_without_order_send():
    mt5 = FakeMT5()
    result = execute_close_position(
        mt5,
        close_message(expected_position_identifier=9999),
        sleep_fn=lambda _seconds: None,
    )

    assert result["status"] == "rejected"
    assert result["error"] == "position_identifier_mismatch"
    assert mt5.sent == []


def test_ambiguous_timeout_is_not_blindly_retried():
    mt5 = FakeMT5(send_mode="timeout")
    result = execute_close_position(mt5, close_message(), sleep_fn=lambda _seconds: None)

    assert result["status"] == "unknown"
    assert result["retryable"] is False
    assert len(mt5.sent) == 1


def test_empty_account_on_wrong_login_is_not_already_satisfied():
    mt5 = FakeMT5()
    mt5.position = None
    mt5.account_info = lambda: SimpleNamespace(login=999)
    result = execute_close_position(mt5, close_message())
    assert result["status"] == "rejected"
    assert result["error"] == "account_login_mismatch"
    assert not mt5.sent


def test_login_change_after_send_cannot_confirm_close():
    mt5 = FakeMT5()
    mt5.account_info = lambda: SimpleNamespace(
        login=999 if mt5.sent else 123, trade_allowed=True, trade_expert=True,
    )
    result = execute_close_position(mt5, close_message(), sleep_fn=lambda _: None)
    assert result["status"] == "unknown"
    assert result["retryable"] is False
    assert len(mt5.sent) == 1


def test_missing_expected_login_is_not_authorized():
    mt5 = FakeMT5()
    result = execute_close_position(mt5, close_message(expected_login=None))
    assert result["status"] == "rejected"
    assert not mt5.sent


def test_partial_close_reconciles_then_closes_only_remainder():
    mt5 = FakeMT5(send_mode="partial")
    original_info = mt5.symbol_info
    def ioc_info(symbol):
        info = original_info(symbol)
        info.filling_mode = 2
        return info
    mt5.symbol_info = ioc_info
    result = execute_close_position(mt5, close_message(), sleep_fn=lambda _seconds: None)

    assert result["status"] == "confirmed"
    assert [request["volume"] for request in mt5.sent] == [1.2, 0.4]




def actor():
    obj = AccountSession(account_id=68, login=123, password="test", server="test", mt5_path="fake")
    obj._cmd_q = queue.Queue(maxsize=IPC_QUEUE_CAPACITY)
    obj._resp_q = queue.Queue(maxsize=IPC_QUEUE_CAPACITY)
    return obj


def reply(message, **payload):
    return {**{k: message[k] for k in ("request_id", "generation", "protocol", "cmd")}, **payload}


@pytest.mark.asyncio
async def test_ipc_discards_other_request_generation_and_operation():
    obj = actor()
    task = asyncio.create_task(obj._request({"cmd": "account_info"}, timeout=0.2))
    await asyncio.sleep(0)
    message = obj._cmd_q.get_nowait()
    obj._resp_q.put(reply(message, request_id="old", data="wrong-request"))
    obj._resp_q.put(reply(message, generation="old", data="wrong-generation"))
    obj._resp_q.put(reply(message, cmd="positions", data="wrong-operation"))
    obj._resp_q.put(reply(message, data={"valid": True}))
    result = await task
    assert result["data"] == {"valid": True}


@pytest.mark.asyncio
async def test_timed_out_actor_does_not_enqueue_work_behind_hung_native_call():
    obj = actor()
    assert await obj._request({"cmd": "positions"}, timeout=0.01) is None
    old = obj._cmd_q.get_nowait()
    obj._resp_q.put(reply(old, data=[]))
    assert await obj._request({"cmd": "account_info"}, timeout=0.01) is None
    assert obj._cmd_q.empty()
    assert obj._ipc_faulted


@pytest.mark.asyncio
async def test_cancelled_and_expired_queued_work_is_never_sent():
    obj = actor()
    obj._dispatcher_task = asyncio.create_task(obj._request_dispatcher())
    first = asyncio.create_task(obj._request({"cmd": "deals"}, timeout=0.2))
    await asyncio.sleep(0.01)
    message = obj._cmd_q.get_nowait()
    second = asyncio.create_task(obj._request({"cmd": "close_position"}, timeout=0.01))
    assert await second is None
    obj._resp_q.put(reply(message, data=[]))
    await first
    await asyncio.sleep(0.02)
    assert obj._cmd_q.empty()
    obj._dispatcher_task.cancel()
    with suppress(asyncio.CancelledError):
        await obj._dispatcher_task


@pytest.mark.asyncio
async def test_queue_capacity_rejects_admission_without_an_unbounded_wait():
    obj = actor()
    obj._dispatcher_task = SimpleNamespace(done=lambda: False)
    for number in range(IPC_QUEUE_CAPACITY):
        obj._request_queue.put_nowait((20, number, {}, 1, asyncio.get_running_loop().create_future()))
    assert await obj._request({"cmd": "positions"}, timeout=1) is None
    assert obj._request_queue.qsize() == IPC_QUEUE_CAPACITY


@pytest.mark.parametrize("field,value", [("expires_at", None), ("expires_at", "invalid"),
                                        ("deadline_mono", 0), ("deadline_mono", None)])
def test_missing_or_expired_authority_never_sends(field, value):
    mt5 = FakeMT5()
    result = execute_close_position(mt5, close_message(**{field: value}))
    assert result["status"] == "expired"
    assert not result["retryable"]
    assert mt5.sent == []


def test_expiry_is_checked_after_blocking_order_check():
    mt5 = FakeMT5()
    message = close_message()
    def check(_request):
        message["deadline_mono"] = 0
        return {"retcode": 0}
    mt5.order_check = check
    result = execute_close_position(mt5, message)
    assert result["status"] == "expired" and not mt5.sent


def test_wrong_terminal_login_is_rejected():
    mt5 = FakeMT5()
    mt5.account_info = lambda: SimpleNamespace(login=999, trade_allowed=True, trade_expert=True)
    result = execute_close_position(mt5, close_message(expected_login=123))
    assert result["status"] == "rejected" and not mt5.sent


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ConnectionError("db"), asyncio.CancelledError()])
async def test_tick_batch_is_restored_on_failure_or_cancellation(monkeypatch, error):
    obj = Collector.__new__(Collector)
    obj._tick_buffer = deque([{"test_tick": 1}, {"test_tick": 2}], maxlen=4)
    obj._metrics = SimpleNamespace(set_tick_buffer_depth=lambda _n: None)
    async def insert(batch):
        obj._tick_buffer.append({"test_tick": 3})
        raise error
    monkeypatch.setattr(collector_module.repo, "insert_ticks", insert)
    with pytest.raises(type(error)):
        await obj._flush_tick_buffer()
    assert [r["test_tick"] for r in obj._tick_buffer] == [1, 2, 3]


def snapshot(rows=None):
    rows = [] if rows is None else rows
    now = datetime.now(UTC).isoformat()
    return {"account_id": 68, "login": 123, "status": "ok", "complete": True,
            "generation": "one", "started_at": now, "last_success_at": now,
            "positions": rows, "position_count": len(rows),
            "tickets": sorted(r["ticket"] for r in rows)}


@pytest.mark.parametrize("mutation", [
    {"complete": False}, {"generation": ""}, {"account_id": 84}, {"position_count": 1},
    {"positions": [None]}, {"tickets": [1]}, {"started_at": "2000-01-01T00:00:00+00:00"},
    {"started_at": "3000-01-01T00:00:00+00:00"}, {"started_at": "2026-09-01"},
])
def test_snapshot_proof_is_fail_closed(mutation):
    with pytest.raises(ValueError):
        validate_position_snapshot({**snapshot(), **mutation}, 68)


def test_complete_empty_snapshot_is_valid_data_not_trade_confirmation():
    assert validate_position_snapshot(snapshot(), 68)["positions"] == []


@pytest.mark.parametrize("retcode", [None, 10008, 10011, 10012, 10023, 10028, 10031, 10039])
def test_partial_effect_after_ambiguous_reply_never_authorizes_second_send(retcode):
    mt5 = FakeMT5()
    def uncertain_send(request):
        mt5.sent.append(request)
        mt5.position.volume = 0.4
        return None if retcode is None else {"retcode": retcode}
    mt5.order_send = uncertain_send
    result = execute_close_position(mt5, close_message(), sleep_fn=lambda _: None)
    assert result["status"] == "unknown"
    assert not result["retryable"]
    assert len(mt5.sent) == 1


def test_return_partial_can_still_be_pending_so_no_second_close(monkeypatch):
    mt5 = FakeMT5(send_mode="partial")
    monkeypatch.setattr("src.mt5.trading._filling_candidates", lambda *_: [mt5.ORDER_FILLING_RETURN])
    result = execute_close_position(mt5, close_message(), sleep_fn=lambda _: None)
    assert result["status"] == "unknown"
    assert len(mt5.sent) == 1

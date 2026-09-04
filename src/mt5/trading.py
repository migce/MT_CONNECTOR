"""
MT5 Trading operations — history, positions, orders.

The ``MetaTrader5`` Python library is a **singleton per process** — only
one terminal connection is allowed at a time.  To support multiple
trading accounts *simultaneously* (without killing the poller's
connection), each account runs inside its own **child process**.

Architecture
~~~~~~~~~~~~

    trader_main (parent)
        ├── AccountWorker(login=5052841)   # child process 1
        │       └── mt5.initialize(portable=True)
        ├── AccountWorker(login=5066472)   # child process 2
        │       └── mt5.initialize(portable=True)
        └── status publisher, signal handling …

Communication is via ``multiprocessing.Queue``:

    - parent sends *commands*  → child  (``CMD_DEALS``, ``CMD_POSITIONS``, …)
    - child  sends *responses* → parent (pickled dicts / lists)

``portable=True`` tells the terminal to use its own installation folder
as the data directory, so multiple terminals can coexist.
"""

from __future__ import annotations

import asyncio
import math
import multiprocessing as mp
import queue
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import structlog

logger = structlog.get_logger(__name__)

# Command constants
CMD_CONNECT = "connect"
CMD_DISCONNECT = "disconnect"
CMD_DEALS = "deals"
CMD_POSITION_DEALS = "position_deals"
CMD_POSITIONS = "positions"
CMD_ORDERS = "orders"
CMD_ACCOUNT_INFO = "account_info"
CMD_HEALTH = "health"
CMD_CLOSE_POSITION = "close_position"
CMD_SHUTDOWN = "shutdown"

PRIORITY_CLOSE = 0
PRIORITY_POSITION_EVENT = 10
PRIORITY_LIVE_SNAPSHOT = 20
PRIORITY_HEALTH = 40
PRIORITY_BULK_HISTORY = 100
IPC_QUEUE_CAPACITY = 64
IPC_PROTOCOL_VERSION = 1


def _command_expired(msg: dict[str, Any]) -> bool:
    """Check both the monotonic IPC budget and the absolute trade expiry."""
    try:
        deadline = float(msg["deadline_mono"])
        if not math.isfinite(deadline):
            return True
        expires = datetime.fromisoformat(str(msg["expires_at"]).replace("Z", "+00:00"))
        if expires.tzinfo is None:
            return True
        return time.monotonic() >= deadline or datetime.now(timezone.utc) >= expires
    except (KeyError, TypeError, ValueError, OverflowError):
        return True  # Missing or malformed authority must never permit a send.


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "_asdict"):
        data = dict(value._asdict())
        request = data.get("request")
        if hasattr(request, "_asdict"):
            data["request"] = dict(request._asdict())
        return data
    return dict(value) if isinstance(value, dict) else {"value": str(value)}


def _filling_candidates(mt5: Any, symbol_info: Any) -> list[int]:
    """Return broker-compatible filling modes in a conservative order."""
    candidates: list[int] = []
    mode = int(getattr(symbol_info, "filling_mode", 0) or 0)
    execution = int(getattr(symbol_info, "trade_exemode", -1) or -1)
    fok = int(getattr(mt5, "ORDER_FILLING_FOK", 0))
    ioc = int(getattr(mt5, "ORDER_FILLING_IOC", 1))
    returning = int(getattr(mt5, "ORDER_FILLING_RETURN", 2))
    market_execution = int(getattr(mt5, "SYMBOL_TRADE_EXECUTION_MARKET", 2))

    if mode & 1:
        candidates.append(fok)
    if mode & 2:
        candidates.append(ioc)
    if execution != market_execution:
        candidates.append(returning)

    # Some brokers report incomplete symbol flags. Trying another filling mode
    # is safe only while the server explicitly reports INVALID_FILL.
    for candidate in (ioc, fok, returning):
        if candidate not in candidates and not (
            candidate == returning and execution == market_execution
        ):
            candidates.append(candidate)
    return candidates


def execute_close_position(
    mt5: Any,
    msg: dict[str, Any],
    *,
    sleep_fn=time.sleep,
) -> dict[str, Any]:
    """Close one exact position and reconcile the broker state.

    This function is deliberately adapter-like and receives the MT5 module as
    an argument so its safety behavior can be tested without a live terminal.
    It never opens a position by symbol alone: every request contains the exact
    current position ticket and is preceded by live precondition checks.
    """
    ticket = int(msg["position_ticket"])
    expected_identifier = msg.get("expected_position_identifier")
    expected_symbol = str(msg["expected_symbol"])
    expected_type = int(msg["expected_type"])
    expected_magic = msg.get("expected_magic")
    max_volume = float(msg["max_volume"])
    command_id = str(msg.get("command_id") or "")
    deviation = int(msg.get("deviation_points", 100))
    max_sends = int(msg.get("send_attempts", 3))
    reconcile_timeout = float(msg.get("reconcile_timeout_sec", 5.0))
    attempts: list[dict[str, Any]] = []
    sends = 0
    ever_submitted = False

    done = int(getattr(mt5, "TRADE_RETCODE_DONE", 10009))
    done_partial = int(getattr(mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010))
    placed = int(getattr(mt5, "TRADE_RETCODE_PLACED", 10008))
    invalid_fill = int(getattr(mt5, "TRADE_RETCODE_INVALID_FILL", 10030))
    timeout_retcode = int(getattr(mt5, "TRADE_RETCODE_TIMEOUT", 10012))
    retryable_retcodes = {
        int(getattr(mt5, "TRADE_RETCODE_REQUOTE", 10004)),
        int(getattr(mt5, "TRADE_RETCODE_MARKET_CLOSED", 10018)),
        int(getattr(mt5, "TRADE_RETCODE_PRICE_CHANGED", 10020)),
        int(getattr(mt5, "TRADE_RETCODE_PRICE_OFF", 10021)),
        int(getattr(mt5, "TRADE_RETCODE_TOO_MANY_REQUESTS", 10024)),
    }
    # Transport/processing ambiguity is not permission to repeat an order.
    ambiguous_retcodes = {timeout_retcode, placed, done, done_partial, 10011, 10023, 10028, 10031, 10036, 10039}
    rejected_retcodes = {
        10006, 10007, 10013, 10014, 10015, 10016, 10017, 10019, 10022, 10025,
        10026, 10027, 10029, 10032, 10033, 10034, 10035, 10038, 10040, 10041,
        10042, 10043, 10044, 10045, 10046,
    }

    def failure(status: str, error: str, *, retryable: bool) -> dict[str, Any]:
        return {
            "status": status,
            "retryable": retryable,
            "error": error,
            "position_ticket": ticket,
            "attempts": attempts,
        }

    def get_position() -> tuple[Any | None, str | None]:
        # An empty result on the wrong login is not proof of a closed position.
        # Check identity during reconciliation too, not only before order_send.
        info = mt5.account_info()
        if info is None:
            return None, "account_identity_unavailable"
        if int(getattr(info, "login", -1)) != int(msg["expected_login"]):
            return None, "account_login_mismatch"
        raw = mt5.positions_get(ticket=ticket)
        if raw is None:
            return None, f"positions_get failed: {mt5.last_error()}"
        return (raw[0] if raw else None), None

    if msg.get("expected_login") is None:
        return failure("rejected", "expected_account_identity_missing", retryable=False)

    while sends < max_sends:
        if _command_expired(msg):
            return failure("unknown" if ever_submitted else "expired", "command_deadline_exceeded", retryable=False)
        position, position_error = get_position()
        if position_error:
            if ever_submitted:
                return failure("unknown", position_error, retryable=False)
            if position_error == "account_login_mismatch":
                return failure("rejected", position_error, retryable=False)
            return failure("retryable_error", position_error, retryable=True)
        if position is None:
            return {
                "status": "confirmed" if ever_submitted else "already_satisfied",
                "retryable": False,
                "error": None,
                "position_ticket": ticket,
                "remaining_volume": 0.0,
                "attempts": attempts,
            }

        actual_identifier = int(getattr(position, "identifier", ticket) or ticket)
        actual_symbol = str(getattr(position, "symbol", ""))
        actual_type = int(getattr(position, "type", -1))
        actual_magic = int(getattr(position, "magic", 0) or 0)
        volume = float(getattr(position, "volume", 0) or 0)

        if expected_identifier is not None and actual_identifier != int(expected_identifier):
            return failure("rejected", "position_identifier_mismatch", retryable=False)
        if actual_symbol != expected_symbol:
            return failure("rejected", "position_symbol_mismatch", retryable=False)
        if actual_type != expected_type:
            return failure("rejected", "position_direction_mismatch", retryable=False)
        if expected_magic is not None and actual_magic != int(expected_magic):
            return failure("rejected", "position_magic_mismatch", retryable=False)
        if not math.isfinite(volume) or not math.isfinite(max_volume) or volume <= 0 or max_volume <= 0:
            return failure("rejected", "position_has_no_positive_volume", retryable=False)

        account_info = mt5.account_info()
        if account_info is None:
            return failure("retryable_error", f"account_info failed: {mt5.last_error()}", retryable=True)
        if msg.get("expected_login") is not None and int(getattr(account_info, "login", -1)) != int(msg["expected_login"]):
            return failure("rejected", "account_login_mismatch", retryable=False)
        if not bool(getattr(account_info, "trade_allowed", False)):
            return failure("rejected", "account_trading_not_allowed", retryable=False)
        if not bool(getattr(account_info, "trade_expert", False)):
            return failure("rejected", "expert_trading_not_allowed", retryable=False)

        terminal_info = mt5.terminal_info()
        if terminal_info is None:
            return failure("retryable_error", f"terminal_info failed: {mt5.last_error()}", retryable=True)
        if hasattr(terminal_info, "trade_allowed") and not bool(terminal_info.trade_allowed):
            return failure("rejected", "terminal_trading_not_allowed", retryable=False)

        symbol_info = mt5.symbol_info(actual_symbol)
        if symbol_info is None:
            return failure("retryable_error", f"symbol_info failed: {mt5.last_error()}", retryable=True)
        if not bool(getattr(symbol_info, "visible", True)):
            if not mt5.symbol_select(actual_symbol, True):
                return failure("retryable_error", f"symbol_select failed: {mt5.last_error()}", retryable=True)
            symbol_info = mt5.symbol_info(actual_symbol)
            if symbol_info is None:
                return failure("retryable_error", f"symbol_info failed: {mt5.last_error()}", retryable=True)

        volume_step = float(getattr(symbol_info, "volume_step", 0.01) or 0.01)
        tolerance = max(volume_step / 2.0, 1e-9)
        if volume > max_volume + tolerance:
            return failure("rejected", "position_volume_exceeds_authorized_maximum", retryable=False)

        fill_invalid_seen = False
        for filling in _filling_candidates(mt5, symbol_info):
            if sends >= max_sends:
                break
            tick = mt5.symbol_info_tick(actual_symbol)
            if tick is None:
                return failure("retryable_error", f"symbol_info_tick failed: {mt5.last_error()}", retryable=True)

            close_type = (
                int(getattr(mt5, "ORDER_TYPE_SELL", 1))
                if actual_type == int(getattr(mt5, "POSITION_TYPE_BUY", 0))
                else int(getattr(mt5, "ORDER_TYPE_BUY", 0))
            )
            request: dict[str, Any] = {
                "action": int(getattr(mt5, "TRADE_ACTION_DEAL", 1)),
                "symbol": actual_symbol,
                "volume": volume,
                "type": close_type,
                "position": ticket,
                "deviation": deviation,
                "magic": actual_magic,
                "comment": f"forced-close:{command_id[:12]}",
                "type_time": int(getattr(mt5, "ORDER_TIME_GTC", 0)),
                "type_filling": filling,
            }
            market_execution = int(getattr(mt5, "SYMBOL_TRADE_EXECUTION_MARKET", 2))
            if int(getattr(symbol_info, "trade_exemode", -1)) != market_execution:
                request["price"] = float(tick.bid if close_type == int(getattr(mt5, "ORDER_TYPE_SELL", 1)) else tick.ask)

            check_started = datetime.now(timezone.utc)
            check = mt5.order_check(request)
            check_finished = datetime.now(timezone.utc)
            check_data = _as_dict(check)
            check_retcode = int(check_data.get("retcode", -1)) if check is not None else None
            attempts.append({
                "phase": "order_check",
                "retcode": check_retcode,
                "message": check_data.get("comment") or (str(mt5.last_error()) if check is None else None),
                "request": request,
                "result": check_data,
                "started_at": check_started.isoformat(),
                "finished_at": check_finished.isoformat(),
            })
            if check is None:
                return failure("retryable_error", "order_check_failed", retryable=True)
            if check_retcode != 0:
                if check_retcode == invalid_fill:
                    fill_invalid_seen = True
                    continue
                if check_retcode in retryable_retcodes:
                    return failure("retryable_error", f"order_check_retcode_{check_retcode}", retryable=True)
                return failure("rejected", f"order_check_retcode_{check_retcode}", retryable=False)

            # order_check and quote/account reads can themselves block. Recheck
            # immediately before every external side effect, including retries.
            if _command_expired(msg):
                return failure("unknown" if ever_submitted else "expired", "command_deadline_exceeded", retryable=False)
            send_started = datetime.now(timezone.utc)
            result = mt5.order_send(request)
            send_finished = datetime.now(timezone.utc)
            sends += 1
            ever_submitted = True
            result_data = _as_dict(result)
            retcode = int(result_data.get("retcode", -1)) if result is not None else None
            attempts.append({
                "phase": "order_send",
                "retcode": retcode,
                "message": result_data.get("comment") or (str(mt5.last_error()) if result is None else None),
                "request": request,
                "result": result_data,
                "started_at": send_started.isoformat(),
                "finished_at": send_finished.isoformat(),
            })

            if result is None or retcode in ambiguous_retcodes:
                # Only a definitive IOC partial result cancels the unfilled
                # remainder. RETURN can still be processing it at the broker.
                can_finish_partial = (
                    retcode == done_partial
                    and filling == int(getattr(mt5, "ORDER_FILLING_IOC", 1))
                )
                deadline = time.monotonic() + reconcile_timeout
                remaining_position = None
                reconcile_error = None
                while time.monotonic() < deadline:
                    remaining_position, reconcile_error = get_position()
                    if reconcile_error is None and remaining_position is None:
                        return {
                            "status": "confirmed",
                            "retryable": False,
                            "error": None,
                            "position_ticket": ticket,
                            "remaining_volume": 0.0,
                            "broker_result": result_data,
                            "attempts": attempts,
                        }
                    if reconcile_error is None:
                        remaining_volume = float(getattr(remaining_position, "volume", 0) or 0)
                        if can_finish_partial and remaining_volume < volume - tolerance:
                            # A definitive partial effect occurred. Re-read the
                            # position at the top and close only the remainder.
                            break
                    sleep_fn(0.1)

                if reconcile_error:
                    return failure("unknown", reconcile_error, retryable=False)
                if remaining_position is not None:
                    remaining_volume = float(getattr(remaining_position, "volume", 0) or 0)
                    if can_finish_partial and remaining_volume < volume - tolerance and sends < max_sends:
                        break
                return failure("unknown", "submission_outcome_not_confirmed", retryable=False)

            if retcode == invalid_fill:
                fill_invalid_seen = True
                continue
            if retcode in retryable_retcodes:
                break  # refresh position/quote and retry within this call
            return failure(
                "rejected" if retcode in rejected_retcodes else "unknown",
                f"order_send_retcode_{retcode}", retryable=False,
            )

        else:
            if fill_invalid_seen:
                return failure("rejected", "no_supported_filling_mode", retryable=False)

    # Only definitive retryable retcodes reach this point. Reconciliation on
    # the next dispatcher attempt still happens before another order is sent.
    return failure("retryable_error", "close_send_attempts_exhausted", retryable=True)


# ---------------------------------------------------------------
# Child-process worker (runs in its own process with its own MT5)
# ---------------------------------------------------------------

def _account_worker(
    login: int,
    password: str,
    server: str,
    mt5_path: str,
    cmd_q: mp.Queue,
    resp_q: mp.Queue,
    generation: str,
) -> None:
    """Entry point for per-account child process.

    Blocks on *cmd_q*, executes MT5 calls, puts results into *resp_q*.
    Exits when it receives ``CMD_SHUTDOWN``.
    """
    import MetaTrader5 as mt5  # singleton — safe, we own the process

    from src.mt5.portable import (
        minimize_terminal_window,
        prepare_terminal,
        start_terminal_protected,
        stop_terminal_process,
    )

    connected = False

    while True:
        try:
            msg = cmd_q.get(timeout=5)
        except queue.Empty:
            continue

        cmd = msg.get("cmd")

        def respond(payload: dict[str, Any]) -> None:
            resp_q.put({**payload, "request_id": msg.get("request_id"),
                        "generation": generation, "protocol": IPC_PROTOCOL_VERSION})

        # Parent and child must agree on the request identity and budget. This
        # prevents a queued close from executing after its caller timed out.
        if (msg.get("protocol") != IPC_PROTOCOL_VERSION
                or not isinstance(msg.get("request_id"), str)
                or not msg["request_id"] or msg.get("generation") != generation):
            continue
        if time.monotonic() >= float(msg.get("deadline_mono", 0)):
            respond({"cmd": cmd, "ok": False, "error": "request_expired",
                     "data": {"status": "expired", "retryable": False,
                              "error": "command_deadline_exceeded", "attempts": []}})
            continue

        if cmd == CMD_CONNECT:
            # Write chart-less terminal.ini before mt5.initialize launches it
            prepare_terminal(mt5_path)
            start_terminal_protected(mt5_path, portable=True)
            ok = mt5.initialize(
                path=mt5_path,
                login=login,
                password=password,
                server=server,
                timeout=30_000,
                portable=True,
            )
            if ok:
                ok = mt5.login(
                    login=login,
                    password=password,
                    server=server,
                    timeout=30_000,
                )
            if ok:
                connected = True
                # Terminal is connected — minimize its window
                minimize_terminal_window(mt5_path)
                respond({"cmd": CMD_CONNECT, "ok": True})
            else:
                err_code, err_msg = mt5.last_error()
                respond({
                    "cmd": CMD_CONNECT,
                    "ok": False,
                    "error": [err_code, err_msg],
                })

        elif cmd == CMD_DEALS:
            if not connected:
                respond({"cmd": CMD_DEALS, "ok": False, "error": "not_connected"})
                continue
            date_from = msg["date_from"]
            date_to = msg["date_to"]
            raw = mt5.history_deals_get(date_from, date_to)
            if raw is None:
                respond({"cmd": CMD_DEALS, "ok": False, "error": list(mt5.last_error())})
                continue
            deals = [dict(d._asdict()) for d in raw] if raw else []
            respond({"cmd": CMD_DEALS, "ok": True, "data": deals})

        elif cmd == CMD_POSITION_DEALS:
            if not connected:
                respond({"cmd": CMD_POSITION_DEALS, "ok": False, "error": "not_connected"})
                continue
            raw = mt5.history_deals_get(position=int(msg["position_identifier"]))
            if raw is None:
                respond({"cmd": CMD_POSITION_DEALS, "ok": False, "error": list(mt5.last_error())})
                continue
            deals = [dict(d._asdict()) for d in raw] if raw else []
            respond({"cmd": CMD_POSITION_DEALS, "ok": True, "data": deals})

        elif cmd == CMD_POSITIONS:
            if not connected:
                respond({"cmd": CMD_POSITIONS, "ok": False, "error": "not_connected"})
                continue
            raw = mt5.positions_get()
            if raw is None:
                respond({"cmd": CMD_POSITIONS, "ok": False, "error": list(mt5.last_error())})
                continue
            positions = [dict(p._asdict()) for p in raw] if raw else []
            respond({"cmd": CMD_POSITIONS, "ok": True, "data": positions})

        elif cmd == CMD_ORDERS:
            if not connected:
                respond({"cmd": CMD_ORDERS, "ok": False, "error": "not_connected"})
                continue
            raw = mt5.orders_get()
            if raw is None:
                respond({"cmd": CMD_ORDERS, "ok": False, "error": list(mt5.last_error())})
                continue
            orders = [dict(o._asdict()) for o in raw] if raw else []
            respond({"cmd": CMD_ORDERS, "ok": True, "data": orders})

        elif cmd == CMD_CLOSE_POSITION:
            if not connected:
                respond({
                    "cmd": CMD_CLOSE_POSITION,
                    "data": {
                        "status": "retryable_error",
                        "retryable": True,
                        "error": "not_connected",
                        "attempts": [],
                    },
                })
                continue
            try:
                result = execute_close_position(mt5, {**msg, "expected_login": login})
            except Exception as exc:
                result = {
                    "status": "unknown",
                    "retryable": False,
                    "error": f"close_worker_exception:{type(exc).__name__}",
                    "attempts": [],
                }
            respond({"cmd": CMD_CLOSE_POSITION, "data": result})

        elif cmd == CMD_ACCOUNT_INFO:
            if not connected:
                respond({"cmd": CMD_ACCOUNT_INFO, "data": None})
                continue
            info = mt5.account_info()
            if info is None:
                # Terminal might be dead — mark disconnected
                connected = False
                respond({"cmd": CMD_ACCOUNT_INFO, "data": None, "dead": True})
            else:
                respond({
                    "cmd": CMD_ACCOUNT_INFO,
                    "data": dict(info._asdict()),
                })

        elif cmd == CMD_HEALTH:
            # Lightweight liveness check — terminal_info returns None
            # when the terminal process has been killed.
            if not connected:
                respond({"cmd": CMD_HEALTH, "alive": False})
                continue
            info = mt5.terminal_info()
            alive = info is not None
            if not alive:
                connected = False
            respond({"cmd": CMD_HEALTH, "alive": alive})

        elif cmd in (CMD_DISCONNECT, CMD_SHUTDOWN):
            if connected:
                mt5.shutdown()
                connected = False
            if cmd == CMD_SHUTDOWN:
                stop_terminal_process(mt5_path)
            respond({"cmd": cmd, "ok": True})
            if cmd == CMD_SHUTDOWN:
                break


# ---------------------------------------------------------------
# One-shot credential verification (child process)
# ---------------------------------------------------------------

def _verify_worker(
    login: int,
    password: str,
    server: str,
    mt5_path: str,
    resp_q: mp.Queue,
) -> None:
    """Spawn, connect, grab account_info, shutdown. One-shot."""
    import MetaTrader5 as mt5

    from src.mt5.portable import prepare_terminal, start_terminal_protected

    # Write chart-less terminal.ini before mt5.initialize launches it
    prepare_terminal(mt5_path)
    start_terminal_protected(mt5_path, portable=True)
    ok = mt5.initialize(
        path=mt5_path,
        login=login,
        password=password,
        server=server,
        timeout=30_000,
        portable=True,
    )
    if not ok:
        err_code, err_msg = mt5.last_error()
        resp_q.put({"ok": False, "error_code": err_code, "error_msg": err_msg})
        return

    ok = mt5.login(
        login=login,
        password=password,
        server=server,
        timeout=30_000,
    )
    if not ok:
        err_code, err_msg = mt5.last_error()
        mt5.shutdown()
        resp_q.put({"ok": False, "error_code": err_code, "error_msg": err_msg})
        return

    info = mt5.account_info()
    data = dict(info._asdict()) if info else {}
    mt5.shutdown()
    resp_q.put({
        "ok": True,
        "account_name": data.get("name", ""),
        "server": data.get("server", server),
        "balance": data.get("balance", 0),
        "leverage": data.get("leverage", 0),
        "currency": data.get("currency", "USD"),
    })


async def verify_credentials(
    login: int,
    password: str,
    server: str,
    mt5_path: str,
    timeout: float = 90,
) -> dict:
    """Verify MT5 credentials in a temporary child process.

    Returns a dict with ``ok: True/False`` and either account info or
    error details.  No DB records are created.
    """
    resp_q: mp.Queue = mp.Queue()
    proc = mp.Process(
        target=_verify_worker,
        args=(login, password, server, mt5_path, resp_q),
        name=f"mt5-verify-{login}",
        daemon=True,
    )
    proc.start()

    end = time.monotonic() + timeout
    result = None
    while time.monotonic() < end:
        try:
            result = resp_q.get_nowait()
            break
        except queue.Empty:
            await asyncio.sleep(0.1)

    proc.join(timeout=5)
    if proc.is_alive():
        proc.terminate()

    if result is None:
        return {"ok": False, "error_code": -1, "error_msg": "Verification timed out"}
    return result


# ---------------------------------------------------------------
# Parent-side async wrapper
# ---------------------------------------------------------------

class AccountSession:
    """Async wrapper around a per-account child process.

    All MT5 operations are delegated to the child via queues.
    """

    def __init__(
        self,
        account_id: int,
        login: int,
        password: str,
        server: str,
        mt5_path: str,
        mt5_server_time_offset_hours: int = 0,
    ) -> None:
        self.account_id = account_id
        self.login = login
        self.password = password
        self.server = server
        self.mt5_path = mt5_path
        self._mt5_server_time_offset_hours = mt5_server_time_offset_hours
        self._connected = False
        self._process: mp.Process | None = None
        self._cmd_q: mp.Queue | None = None
        self._resp_q: mp.Queue | None = None
        # Serialize all commands — only one in-flight at a time.
        self._lock = asyncio.Lock()
        self._request_queue: asyncio.PriorityQueue[tuple[int, int, dict, float, asyncio.Future]] = asyncio.PriorityQueue(maxsize=IPC_QUEUE_CAPACITY)
        self._request_sequence = 0
        self._generation = uuid4().hex
        self._ipc_faulted = False
        self._last_response_mono: float | None = None
        self._active_request: dict | None = None
        self._dispatcher_task: asyncio.Task | None = None

    def _to_mt5_query_time(self, dt_utc: datetime) -> datetime:
        """Convert UTC window boundary to MT5 server-time boundary."""
        return dt_utc + timedelta(hours=self._mt5_server_time_offset_hours)

    def _from_mt5_seconds(self, ts: int | float) -> datetime:
        """Convert MT5 deal/position epoch seconds to normalized UTC datetime."""
        dt_server = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return dt_server - timedelta(hours=self._mt5_server_time_offset_hours)

    def _from_mt5_milliseconds(self, ts_msc: int) -> datetime:
        """Convert MT5 epoch milliseconds to normalized UTC datetime."""
        dt_server = datetime.fromtimestamp(ts_msc / 1000.0, tz=timezone.utc)
        return dt_server - timedelta(hours=self._mt5_server_time_offset_hours)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _send(self, msg: dict) -> None:
        if self._cmd_q is None:
            raise RuntimeError("MT5 actor is unavailable")
        self._cmd_q.put_nowait(msg)

    async def _recv(self, timeout: float = 60, *, request: dict) -> dict | None:
        """Receive only the matching response; never consume a foreign result."""
        end = min(time.monotonic() + timeout, request["deadline_mono"])
        while time.monotonic() < end:
            try:
                response = self._resp_q.get_nowait()
            except queue.Empty:
                await asyncio.sleep(min(0.01, max(0, end - time.monotonic())))
                continue
            if not isinstance(response, dict) or any(
                response.get(key) != request.get(key)
                for key in ("protocol", "generation", "request_id", "cmd")
            ):
                logger.warning("mt5_ipc_foreign_response", account_id=self.account_id)
                continue
            self._last_response_mono = time.monotonic()
            return response
        return None

    async def _request_dispatcher(self) -> None:
        """Serialize account commands while honoring close-operation priority."""
        try:
            while True:
                _priority, _sequence, msg, timeout, future = await self._request_queue.get()
                try:
                    if future.cancelled():
                        continue
                    if self._ipc_faulted or time.monotonic() >= msg["deadline_mono"]:
                        if not future.done():
                            future.set_result(None)
                        continue
                    self._active_request = msg
                    self._send(msg)
                    response = await self._recv(timeout, request=msg)
                    if response is None:
                        # Do not build up another physical queue behind a hung
                        # native call. Recovery must fence unknown trade outcomes.
                        self._ipc_faulted = True
                    if not future.done():
                        future.set_result(response)
                except asyncio.CancelledError:
                    if not future.done():
                        future.cancel()
                    raise
                except Exception as exc:
                    if not future.done():
                        future.set_exception(exc)
                finally:
                    self._active_request = None
                    self._request_queue.task_done()
        finally:
            while True:
                try:
                    _priority, _sequence, _msg, _timeout, future = self._request_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if not future.done():
                    future.cancel()
                self._request_queue.task_done()

    async def _request(
        self,
        msg: dict,
        timeout: float = 60,
        *,
        priority: int = PRIORITY_LIVE_SNAPSHOT,
    ) -> dict | None:
        """Bound the complete wait, including admission and queue time."""
        if self._ipc_faulted:
            return None
        deadline = time.monotonic() + timeout
        envelope = {**msg, "protocol": IPC_PROTOCOL_VERSION,
                    "request_id": uuid4().hex, "generation": self._generation,
                    "deadline_mono": deadline}
        if self._dispatcher_task is None or self._dispatcher_task.done():
            try:
                async with asyncio.timeout(timeout):
                    async with self._lock:
                        if self._ipc_faulted or time.monotonic() >= deadline:
                            return None
                        self._send(envelope)
                        response = await self._recv(timeout, request=envelope)
                        if response is None:
                            self._ipc_faulted = True
                        return response
            except TimeoutError:
                self._ipc_faulted = True
                return None

        future = asyncio.get_running_loop().create_future()
        self._request_sequence += 1
        try:
            self._request_queue.put_nowait(
                (priority, self._request_sequence, envelope, timeout, future)
            )
        except asyncio.QueueFull:
            logger.warning("mt5_ipc_queue_full", account_id=self.account_id)
            return None
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            return None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        self._generation = uuid4().hex
        self._ipc_faulted = False
        self._cmd_q = mp.Queue(maxsize=IPC_QUEUE_CAPACITY)
        self._resp_q = mp.Queue(maxsize=IPC_QUEUE_CAPACITY)
        self._process = mp.Process(
            target=_account_worker,
            args=(
                self.login,
                self.password,
                self.server,
                self.mt5_path,
                self._cmd_q,
                self._resp_q,
                self._generation,
            ),
            name=f"mt5-acct-{self.login}",
            daemon=True,
        )
        self._process.start()

        resp = await self._request({"cmd": CMD_CONNECT}, timeout=90)

        if resp and resp.get("ok"):
            self._connected = True
            self._dispatcher_task = asyncio.create_task(
                self._request_dispatcher(),
                name=f"mt5-account-dispatcher-{self.account_id}",
            )
            logger.info(
                "trader_account_connected",
                account_id=self.account_id,
                login=self.login,
                server=self.server,
            )
            return True

        err = resp.get("error") if resp else "timeout"
        logger.warning(
            "trader_account_connect_failed",
            account_id=self.account_id,
            login=self.login,
            error=err,
        )
        # Kill the child process on failure
        await self.disconnect()
        return False

    async def disconnect(self) -> None:
        self._connected = False
        if self._process is not None and self._process.is_alive():
            if self._dispatcher_task is not None and not self._dispatcher_task.done():
                await self._request(
                    {"cmd": CMD_SHUTDOWN},
                    timeout=10,
                    priority=PRIORITY_CLOSE - 1,
                )
            else:
                await self._request({"cmd": CMD_SHUTDOWN}, timeout=10)
        # Also cancel the dispatcher when the child has already crashed.
        # Otherwise it can survive into the replacement actor generation.
        self._ipc_faulted = True
        if self._dispatcher_task is not None:
            self._dispatcher_task.cancel()
            await asyncio.gather(self._dispatcher_task, return_exceptions=True)
        if self._process is not None:
            await asyncio.to_thread(self._process.join, timeout=5)
            if self._process.is_alive():
                self._process.terminate()
                await asyncio.to_thread(self._process.join, timeout=5)
                if self._process.is_alive():
                    raise RuntimeError("Previous MT5 worker did not exit; replacement is fenced")
            self._process.close()
        for channel in (self._cmd_q, self._resp_q):
            if channel is not None:
                channel.cancel_join_thread()
                channel.close()
        self._cmd_q = self._resp_q = None
        self._connected = False
        self._process = None
        self._dispatcher_task = None
        logger.info(
            "trader_account_disconnected",
            account_id=self.account_id,
            login=self.login,
        )

    @property
    def generation(self) -> str:
        return self._generation

    @property
    def connected(self) -> bool:
        return self._connected

    async def check_health(self) -> bool:
        """Return True if the MT5 terminal process is still alive."""
        if not self._connected or self._process is None or not self._process.is_alive():
            self._connected = False
            return False
        if (not self._ipc_faulted and self._active_request is not None
                and time.monotonic() < self._active_request["deadline_mono"]):
            # This is process liveness, not fresh account-data proof. Do not
            # restart a worker merely because a higher-priority close/history
            # request currently owns the lane within its bounded budget.
            return True
        resp = await self._request(
            {"cmd": CMD_HEALTH}, timeout=15, priority=PRIORITY_HEALTH
        )
        if (resp is None and not self._ipc_faulted and self._active_request is not None
                and time.monotonic() < self._active_request["deadline_mono"]):
            return True
        if resp is None or not resp.get("alive"):
            self._connected = False
            return False
        return True

    async def reconnect(self) -> bool:
        """Tear down the child process and start a fresh one."""
        logger.info(
            "trader_account_reconnecting",
            account_id=self.account_id,
            login=self.login,
        )
        await self.disconnect()
        return await self.connect()

    # ------------------------------------------------------------------
    # Trading data queries
    # ------------------------------------------------------------------

    async def get_deals(
        self,
        date_from: datetime,
        date_to: datetime,
    ) -> list[dict[str, Any]]:
        date_from_mt5 = self._to_mt5_query_time(date_from)
        date_to_mt5 = self._to_mt5_query_time(date_to)
        resp = await self._request(
            {
                "cmd": CMD_DEALS,
                "date_from": date_from_mt5,
                "date_to": date_to_mt5,
            },
            priority=PRIORITY_BULK_HISTORY,
        )
        if not resp:
            raise RuntimeError("MT5 deals request timed out")
        if resp.get("ok") is False:
            raise RuntimeError(f"MT5 deals request failed: {resp.get('error')}")
        deals = resp.get("data", [])
        for d in deals:
            d["account_id"] = self.account_id
            if d.get("time_msc"):
                d["time"] = self._from_mt5_milliseconds(int(d["time_msc"]))
            else:
                d["time"] = self._from_mt5_seconds(d["time"])
        return deals

    async def get_position_deals(self, position_identifier: int) -> list[dict[str, Any]]:
        """Return broker deals for one stable MT5 position identifier."""
        resp = await self._request(
            {
                "cmd": CMD_POSITION_DEALS,
                "position_identifier": int(position_identifier),
            },
            priority=PRIORITY_POSITION_EVENT,
        )
        if not resp:
            raise RuntimeError("MT5 position-deals request timed out")
        if resp.get("ok") is False:
            raise RuntimeError(f"MT5 position-deals request failed: {resp.get('error')}")
        deals = resp.get("data", [])
        for deal in deals:
            deal["account_id"] = self.account_id
            if deal.get("time_msc"):
                deal["time"] = self._from_mt5_milliseconds(int(deal["time_msc"]))
            elif deal.get("time") is not None:
                deal["time"] = self._from_mt5_seconds(deal["time"])
        return deals

    async def get_positions(self) -> list[dict[str, Any]]:
        resp = await self._request(
            {"cmd": CMD_POSITIONS}, priority=PRIORITY_LIVE_SNAPSHOT
        )
        if not resp:
            raise RuntimeError("MT5 positions request timed out")
        if resp.get("ok") is False:
            raise RuntimeError(f"MT5 positions request failed: {resp.get('error')}")
        positions = resp.get("data", [])
        for p in positions:
            p["account_id"] = self.account_id
            p["time"] = self._from_mt5_seconds(p["time"])
            if p.get("time_update"):
                p["time_update"] = self._from_mt5_seconds(p["time_update"])
        return positions

    async def get_orders(self) -> list[dict[str, Any]]:
        resp = await self._request(
            {"cmd": CMD_ORDERS}, priority=PRIORITY_LIVE_SNAPSHOT
        )
        if not resp:
            raise RuntimeError("MT5 orders request timed out")
        if resp.get("ok") is False:
            raise RuntimeError(f"MT5 orders request failed: {resp.get('error')}")
        return resp.get("data", [])

    async def get_account_info(self) -> dict[str, Any] | None:
        resp = await self._request(
            {"cmd": CMD_ACCOUNT_INFO}, priority=PRIORITY_HEALTH
        )
        if not resp:
            return None
        if resp.get("ok") is False:
            return None
        return resp.get("data")

    async def close_position(
        self,
        *,
        command_id: str,
        position_ticket: int,
        expected_position_identifier: int | None,
        expected_symbol: str,
        expected_type: int,
        expected_magic: int | None,
        max_volume: float,
        deviation_points: int,
        send_attempts: int,
        reconcile_timeout_sec: float,
        expires_at: datetime,
    ) -> dict[str, Any]:
        """Execute and reconcile one exact-ticket close in the account child."""
        resp = await self._request(
            {
                "cmd": CMD_CLOSE_POSITION,
                "command_id": command_id,
                "position_ticket": position_ticket,
                "expected_position_identifier": expected_position_identifier,
                "expected_symbol": expected_symbol,
                "expected_type": expected_type,
                "expected_magic": expected_magic,
                "max_volume": max_volume,
                "deviation_points": deviation_points,
                "send_attempts": send_attempts,
                "reconcile_timeout_sec": reconcile_timeout_sec,
                "expires_at": expires_at.isoformat(),
                "expected_login": self.login,
            },
            timeout=max(60.0, reconcile_timeout_sec * send_attempts + 30.0),
            priority=PRIORITY_CLOSE,
        )
        if not resp:
            return {
                "status": "unknown",
                "retryable": False,
                "error": "close_command_timed_out",
                "attempts": [],
            }
        return resp.get("data") or {
            "status": "unknown",
            "retryable": False,
            "error": "empty_close_response",
            "attempts": [],
        }

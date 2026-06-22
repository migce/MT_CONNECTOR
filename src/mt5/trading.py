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
import multiprocessing as mp
import queue
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# Command constants
CMD_CONNECT = "connect"
CMD_DISCONNECT = "disconnect"
CMD_DEALS = "deals"
CMD_POSITIONS = "positions"
CMD_ORDERS = "orders"
CMD_ACCOUNT_INFO = "account_info"
CMD_HEALTH = "health"
CMD_SHUTDOWN = "shutdown"


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
) -> None:
    """Entry point for per-account child process.

    Blocks on *cmd_q*, executes MT5 calls, puts results into *resp_q*.
    Exits when it receives ``CMD_SHUTDOWN``.
    """
    import MetaTrader5 as mt5  # singleton — safe, we own the process
    from src.mt5.portable import minimize_terminal_window, prepare_terminal

    connected = False

    while True:
        try:
            msg = cmd_q.get(timeout=5)
        except queue.Empty:
            continue

        cmd = msg.get("cmd")

        if cmd == CMD_CONNECT:
            # Write chart-less terminal.ini before mt5.initialize launches it
            prepare_terminal(mt5_path)
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
                resp_q.put({"cmd": CMD_CONNECT, "ok": True})
            else:
                err_code, err_msg = mt5.last_error()
                resp_q.put({
                    "cmd": CMD_CONNECT,
                    "ok": False,
                    "error": [err_code, err_msg],
                })

        elif cmd == CMD_DEALS:
            if not connected:
                resp_q.put({"cmd": CMD_DEALS, "data": []})
                continue
            date_from = msg["date_from"]
            date_to = msg["date_to"]
            raw = mt5.history_deals_get(date_from, date_to)
            deals = [dict(d._asdict()) for d in raw] if raw else []
            resp_q.put({"cmd": CMD_DEALS, "data": deals})

        elif cmd == CMD_POSITIONS:
            if not connected:
                resp_q.put({"cmd": CMD_POSITIONS, "data": []})
                continue
            raw = mt5.positions_get()
            positions = [dict(p._asdict()) for p in raw] if raw else []
            resp_q.put({"cmd": CMD_POSITIONS, "data": positions})

        elif cmd == CMD_ORDERS:
            if not connected:
                resp_q.put({"cmd": CMD_ORDERS, "data": []})
                continue
            raw = mt5.orders_get()
            orders = [dict(o._asdict()) for o in raw] if raw else []
            resp_q.put({"cmd": CMD_ORDERS, "data": orders})

        elif cmd == CMD_ACCOUNT_INFO:
            if not connected:
                resp_q.put({"cmd": CMD_ACCOUNT_INFO, "data": None})
                continue
            info = mt5.account_info()
            if info is None:
                # Terminal might be dead — mark disconnected
                connected = False
                resp_q.put({"cmd": CMD_ACCOUNT_INFO, "data": None, "dead": True})
            else:
                resp_q.put({
                    "cmd": CMD_ACCOUNT_INFO,
                    "data": dict(info._asdict()),
                })

        elif cmd == CMD_HEALTH:
            # Lightweight liveness check — terminal_info returns None
            # when the terminal process has been killed.
            if not connected:
                resp_q.put({"cmd": CMD_HEALTH, "alive": False})
                continue
            info = mt5.terminal_info()
            alive = info is not None
            if not alive:
                connected = False
            resp_q.put({"cmd": CMD_HEALTH, "alive": alive})

        elif cmd in (CMD_DISCONNECT, CMD_SHUTDOWN):
            if connected:
                mt5.shutdown()
                connected = False
            resp_q.put({"cmd": cmd, "ok": True})
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
    from src.mt5.portable import minimize_terminal_window, prepare_terminal

    # Write chart-less terminal.ini before mt5.initialize launches it
    prepare_terminal(mt5_path)
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
        if self._cmd_q is not None:
            self._cmd_q.put(msg)

    async def _recv(self, timeout: float = 60) -> dict | None:
        """Non-blocking wait for a response from the child process."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                return self._resp_q.get_nowait()  # type: ignore[union-attr]
            except queue.Empty:
                await asyncio.sleep(0.05)
        return None

    async def _request(self, msg: dict, timeout: float = 60) -> dict | None:
        """Send a command and wait for the matching response (thread-safe)."""
        async with self._lock:
            self._send(msg)
            return await self._recv(timeout)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        self._cmd_q = mp.Queue()
        self._resp_q = mp.Queue()
        self._process = mp.Process(
            target=_account_worker,
            args=(
                self.login,
                self.password,
                self.server,
                self.mt5_path,
                self._cmd_q,
                self._resp_q,
            ),
            name=f"mt5-acct-{self.login}",
            daemon=True,
        )
        self._process.start()

        self._send({"cmd": CMD_CONNECT})
        resp = await self._recv(timeout=90)

        if resp and resp.get("ok"):
            self._connected = True
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
        if self._process is not None and self._process.is_alive():
            self._send({"cmd": CMD_SHUTDOWN})
            await self._recv(timeout=10)
            self._process.join(timeout=5)
            if self._process.is_alive():
                self._process.terminate()
        self._connected = False
        self._process = None
        logger.info(
            "trader_account_disconnected",
            account_id=self.account_id,
            login=self.login,
        )

    @property
    def connected(self) -> bool:
        return self._connected

    async def check_health(self) -> bool:
        """Return True if the MT5 terminal process is still alive."""
        if not self._connected or self._process is None or not self._process.is_alive():
            self._connected = False
            return False
        resp = await self._request({"cmd": CMD_HEALTH}, timeout=15)
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
        resp = await self._request({
            "cmd": CMD_DEALS,
            "date_from": date_from_mt5,
            "date_to": date_to_mt5,
        })
        if not resp:
            return []
        deals = resp.get("data", [])
        for d in deals:
            d["account_id"] = self.account_id
            if d.get("time_msc"):
                d["time"] = self._from_mt5_milliseconds(int(d["time_msc"]))
            else:
                d["time"] = self._from_mt5_seconds(d["time"])
        return deals

    async def get_positions(self) -> list[dict[str, Any]]:
        resp = await self._request({"cmd": CMD_POSITIONS})
        if not resp:
            return []
        positions = resp.get("data", [])
        for p in positions:
            p["account_id"] = self.account_id
            p["time"] = self._from_mt5_seconds(p["time"])
            if p.get("time_update"):
                p["time_update"] = self._from_mt5_seconds(p["time_update"])
        return positions

    async def get_orders(self) -> list[dict[str, Any]]:
        resp = await self._request({"cmd": CMD_ORDERS})
        if not resp:
            return []
        return resp.get("data", [])

    async def get_account_info(self) -> dict[str, Any] | None:
        resp = await self._request({"cmd": CMD_ACCOUNT_INFO})
        if not resp:
            return None
        return resp.get("data")

"""Separate portable terminal for history; never attaches to a live terminal."""
from __future__ import annotations

import ntpath
import json
import subprocess

from src.mt5.connection import MT5Connection
from src.history_authority import HistoryAuthorityConflict


def existing_history_terminal(path):
    """Bounded, exact-path read. Never use the protected-launch/replacement API."""
    try:
        output = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command",
             "$ErrorActionPreference='Stop'; Get-CimInstance Win32_Process -Filter \"Name='terminal64.exe'\" | "
             "Select-Object ProcessId,ExecutablePath | ConvertTo-Json -Compress"],
            text=True, stderr=subprocess.DEVNULL, timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        rows = json.loads(output) if output.strip() else []
        if isinstance(rows, dict):
            rows = [rows]
        matches = [int(row["ProcessId"]) for row in rows
            if ntpath.normcase(ntpath.normpath(str(row.get("ExecutablePath") or "").strip())) == path
            and str(row.get("ProcessId", "")).isdigit()]
    except (OSError, subprocess.SubprocessError, ValueError, TypeError):
        matches = []
    if len(matches) != 1:
        raise HistoryAuthorityConflict("history_terminal_presence_unconfirmed")
    return matches[0]


def validate_history_path(settings) -> str:
    path = ntpath.normcase(ntpath.abspath(settings.history_mt5_path))
    live = ntpath.normcase(ntpath.abspath(settings.mt5_path))
    fleet = ntpath.normcase(ntpath.abspath(settings.mt5_portable_dir))
    if (not ntpath.isabs(settings.history_mt5_path)
            or ntpath.basename(path) != "terminal64.exe"
            or ntpath.dirname(path) == ntpath.dirname(live)
            or ntpath.commonpath([path, fleet]) == fleet):
        raise ValueError("History requires a dedicated absolute terminal path outside the live fleet")
    return path


class HistoryConnection(MT5Connection):
    """Only connection/catalogue reads; no trading module or order protocol."""

    def __init__(self, settings, *, attach_only=False):
        validate_history_path(settings)
        super().__init__(settings)
        self.attach_only = attach_only

    def connection_proof(self) -> bool:
        """A network outage is retryable; identity or permission drift is not."""
        import MetaTrader5 as mt5

        terminal, account = mt5.terminal_info(), mt5.account_info()
        path = validate_history_path(self._settings)
        if terminal is not None:
            if (ntpath.normcase(ntpath.abspath(terminal.data_path)) != ntpath.dirname(path)
                    or terminal.trade_allowed or not terminal.tradeapi_disabled):
                raise RuntimeError("History terminal identity or trading-disable proof failed")
        if account is not None and account.login != self._settings.mt5_login:
            raise RuntimeError("History terminal identity or trading-disable proof failed")
        return bool(terminal and account and terminal.connected)

    def _try_connect(self) -> bool:
        import MetaTrader5 as mt5
        from src.mt5.portable import start_terminal_protected

        s = self._settings
        path = validate_history_path(s)
        existing_pid = None
        if self.attach_only:
            existing_pid = existing_history_terminal(path)
        else:
            start_terminal_protected(s.history_mt5_path, portable=True,
                                     config_path=ntpath.join(ntpath.dirname(s.history_mt5_path), "history.ini"))
        options = dict(path=s.history_mt5_path, portable=True, timeout=s.mt5_timeout)
        if not self.attach_only:
            options.update(login=s.mt5_login, password=s.mt5_password, server=s.mt5_server)
        if not mt5.initialize(**options):
            return False
        if existing_pid is not None and existing_history_terminal(path) != existing_pid:
            mt5.shutdown()
            raise HistoryAuthorityConflict("history_terminal_identity_changed_during_attach")
        terminal = mt5.terminal_info()
        account = mt5.account_info()
        self.last_proof = dict(terminal_present=terminal is not None,
                               account_present=account is not None,
                               connected=bool(terminal and terminal.connected),
                               data_path_matches=bool(terminal and ntpath.normcase(ntpath.abspath(terminal.data_path)) == ntpath.dirname(path)),
                               login_matches=bool(account and account.login == s.mt5_login),
                               trade_allowed=bool(terminal and terminal.trade_allowed),
                               tradeapi_disabled=bool(terminal and terminal.tradeapi_disabled))
        # Fail closed on IPC attachment to another installation or account.
        try:
            return self.connection_proof()
        except RuntimeError:
            mt5.shutdown()
            raise

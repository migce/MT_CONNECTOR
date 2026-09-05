"""Separate portable terminal for history; never attaches to a live terminal."""
from __future__ import annotations

import ntpath

from src.mt5.connection import MT5Connection


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

    def __init__(self, settings):
        validate_history_path(settings)
        super().__init__(settings)

    def _try_connect(self) -> bool:
        import MetaTrader5 as mt5
        from src.mt5.portable import start_terminal_protected

        s = self._settings
        path = validate_history_path(s)
        start_terminal_protected(s.history_mt5_path, portable=True,
                                 config_path=ntpath.join(ntpath.dirname(s.history_mt5_path), "history.ini"))
        if not mt5.initialize(path=s.history_mt5_path, portable=True,
                              login=s.mt5_login, password=s.mt5_password,
                              server=s.mt5_server, timeout=s.mt5_timeout):
            return False
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
        if (terminal is None or account is None
                or ntpath.normcase(ntpath.abspath(terminal.data_path)) != ntpath.dirname(path)
                or account.login != s.mt5_login or not terminal.connected
                or terminal.trade_allowed or not terminal.tradeapi_disabled):
            mt5.shutdown()
            raise RuntimeError("History terminal identity or trading-disable proof failed")
        return True

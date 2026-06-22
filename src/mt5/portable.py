"""
Portable MT5 terminal provisioning.

Each trading account gets its own copy of the MT5 terminal under
``{MT5_PORTABLE_DIR}/{login}/`` so that multiple accounts can run
simultaneously without interfering with each other or the poller.

The source terminal (``MT5_PATH`` from settings) is copied once;
subsequent calls for the same login are no-ops.

Terminals are configured with no chart windows — they serve
purely as data / trading gateways.  After ``mt5.initialize()``
connects (which launches the terminal **with** credentials),
call ``minimize_terminal_window()`` to push it out of the way.
"""

from __future__ import annotations

import configparser
import ctypes
import ctypes.wintypes
import os
import shutil
import subprocess
import structlog

logger = structlog.get_logger(__name__)

# Files/dirs that are safe to skip when source is locked (caches, temp)
_IGNORABLE_SUFFIXES = frozenset({".LOCK", "LOCK", "Cookies", "Cookies-journal"})


def _ignore_locked(directory: str, entries: list[str]) -> set[str]:
    """shutil.copytree ignore callback — skip temp/lock files."""
    ignored: set[str] = set()
    for entry in entries:
        if entry in ("temp", "Tester"):
            ignored.add(entry)
    return ignored


def ensure_portable_terminal(
    login: int,
    source_path: str,
    portable_dir: str,
) -> str:
    """Ensure a portable terminal copy exists for *login*.

    Parameters
    ----------
    login : int
        MT5 account number (used as the folder name).
    source_path : str
        Path to the source ``terminal64.exe`` (the installed terminal).
    portable_dir : str
        Root directory for portable copies (e.g. ``C:\\MT5_Portable``).

    Returns
    -------
    str
        Full path to the portable ``terminal64.exe``.
    """
    dest_dir = os.path.join(portable_dir, str(login))
    dest_exe = os.path.join(dest_dir, "terminal64.exe")

    if os.path.isfile(dest_exe):
        return dest_exe

    source_dir = os.path.dirname(source_path)
    if not os.path.isfile(source_path):
        raise FileNotFoundError(
            f"Source MT5 terminal not found: {source_path}"
        )

    logger.info(
        "provisioning_portable_terminal",
        login=login,
        source=source_dir,
        dest=dest_dir,
    )

    os.makedirs(dest_dir, exist_ok=True)

    # Copy essential files — skip temp/tester dirs to save space and
    # avoid locked-file errors when the source terminal is running.
    shutil.copytree(
        source_dir,
        dest_dir,
        ignore=_ignore_locked,
        dirs_exist_ok=True,
    )

    if not os.path.isfile(dest_exe):
        raise RuntimeError(
            f"Copy succeeded but terminal64.exe not found at {dest_exe}"
        )

    logger.info(
        "portable_terminal_provisioned",
        login=login,
        path=dest_exe,
    )
    return dest_exe


# ------------------------------------------------------------------
# Headless (minimized, no charts) terminal configuration & launch
# ------------------------------------------------------------------

# INI section/key that controls chart count on startup
_CHARTS_INI_SECTION = "Charts"
_STARTUP_INI_SECTION = "StartUp"


def _write_terminal_ini(terminal_dir: str) -> None:
    """Create / patch ``terminal.ini`` to disable chart windows.

    MT5 reads ``terminal.ini`` (in the data directory, which equals
    the installation directory in portable mode) on startup.
    Setting ``MaxCharts=0`` and ``AutoUpdate=0`` keeps the terminal
    lean and prevents it from opening default chart tabs.
    """
    ini_path = os.path.join(terminal_dir, "terminal.ini")

    cfg = configparser.RawConfigParser()
    cfg.optionxform = str  # preserve case (MT5 is case-sensitive)

    if os.path.isfile(ini_path):
        cfg.read(ini_path, encoding="utf-16")

    if not cfg.has_section(_CHARTS_INI_SECTION):
        cfg.add_section(_CHARTS_INI_SECTION)
    cfg.set(_CHARTS_INI_SECTION, "MaxCharts", "0")
    cfg.set(_CHARTS_INI_SECTION, "ProfileLast", "")

    if not cfg.has_section(_STARTUP_INI_SECTION):
        cfg.add_section(_STARTUP_INI_SECTION)
    cfg.set(_STARTUP_INI_SECTION, "NewsEnable", "0")
    cfg.set(_STARTUP_INI_SECTION, "AutoUpdate", "0")

    try:
        with open(ini_path, "w", encoding="utf-16") as fh:
            cfg.write(fh)
        logger.debug("terminal_ini_written", path=ini_path)
    except PermissionError:
        logger.warning("terminal_ini_permission_denied", path=ini_path)


def prepare_terminal(mt5_path: str) -> None:
    """Write a chart-less ``terminal.ini`` before the terminal is launched.

    Call this **before** ``mt5.initialize()`` so the terminal starts
    with ``MaxCharts=0`` and no news/auto-update overhead.
    ``mt5.initialize()`` itself will launch the terminal with the
    correct login/password/server — no separate Popen needed.
    """
    terminal_dir = os.path.dirname(mt5_path)
    _write_terminal_ini(terminal_dir)


# ------------------------------------------------------------------
# Win32 helpers for minimizing terminal windows after connect
# ------------------------------------------------------------------

_user32 = ctypes.windll.user32  # type: ignore[attr-defined]
_SW_MINIMIZE = 6
_SW_SHOWMINNOACTIVE = 7


def _find_window_by_pid(pid: int) -> int | None:
    """Return the first top-level window handle owned by *pid*, or None."""
    result: list[int] = []

    @ctypes.WINFUNCTYPE(ctypes.wintypes.BOOL, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)
    def _enum_cb(hwnd: int, _lparam: int) -> bool:
        proc_id = ctypes.wintypes.DWORD()
        _user32.GetWindowThreadProcessId(hwnd, ctypes.byref(proc_id))
        if proc_id.value == pid and _user32.IsWindowVisible(hwnd):
            result.append(hwnd)
            return False  # stop enumeration
        return True

    _user32.EnumWindows(_enum_cb, 0)
    return result[0] if result else None


def minimize_terminal_window(mt5_path: str) -> None:
    """Find the MT5 terminal window by executable path and minimize it.

    Call this **after** ``mt5.initialize()`` / ``mt5.login()`` succeed.
    Uses Win32 API to find the process and minimize its window.
    """
    exe_name = os.path.basename(mt5_path).lower()
    try:
        out = subprocess.check_output(
            ["wmic", "process", "where",
             f"name='{exe_name}'", "get", "ProcessId,ExecutablePath",
             "/FORMAT:CSV"],
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        for line in out.strip().splitlines():
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            path_col = parts[1].strip()
            pid_col = parts[2].strip()
            if path_col.lower() == mt5_path.lower() and pid_col.isdigit():
                pid = int(pid_col)
                hwnd = _find_window_by_pid(pid)
                if hwnd:
                    _user32.ShowWindow(hwnd, _SW_SHOWMINNOACTIVE)
                    logger.info(
                        "terminal_window_minimized",
                        path=mt5_path,
                        pid=pid,
                    )
                return
    except Exception:
        logger.debug("minimize_terminal_failed", path=mt5_path, exc_info=True)

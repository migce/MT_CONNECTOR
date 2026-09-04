"""
Portable MT5 terminal provisioning.

Each trading account gets its own copy of the MT5 terminal under
``{MT5_PORTABLE_DIR}/{login}/`` so that multiple accounts can run
simultaneously without interfering with each other or the poller.

The source terminal (``MT5_PATH`` from settings) is copied once;
subsequent calls for the same login are no-ops.

Terminals are configured with no chart windows — they serve
purely as data / trading gateways. Protected prelaunch requests a
minimized, non-activating main window, and the post-connect Win32
minimize call remains as a fallback for terminals that ignore startup state.
"""

from __future__ import annotations

import configparser
import csv
import ctypes
import ctypes.wintypes
import os
import shutil
import subprocess
import time

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


def _terminal_processes(mt5_path: str) -> list[tuple[int, str]]:
    """Return exact-path terminal processes as ``(pid, command_line)``."""
    if os.name != "nt":
        return []
    try:
        out = subprocess.check_output(
            [
                "wmic",
                "process",
                "where",
                "name='terminal64.exe'",
                "get",
                "ProcessId,ExecutablePath,CommandLine",
                "/FORMAT:CSV",
            ],
            text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        logger.debug("terminal_process_query_failed", path=mt5_path, exc_info=True)
        return []

    normalized_path = os.path.normcase(os.path.normpath(mt5_path))
    matches: list[tuple[int, str]] = []
    for row in csv.DictReader(line for line in out.splitlines() if line.strip()):
        process_path = str(row.get("ExecutablePath") or "").strip()
        pid = str(row.get("ProcessId") or "").strip()
        if not process_path or not pid.isdigit():
            continue
        if os.path.normcase(os.path.normpath(process_path)) == normalized_path:
            matches.append((int(pid), str(row.get("CommandLine") or "")))
    return matches


def stop_terminal_process(mt5_path: str) -> None:
    """Stop only the terminal process whose executable path exactly matches."""
    if os.name != "nt":
        return
    for pid, _command_line in _terminal_processes(mt5_path):
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            logger.info("terminal_process_stopped", path=mt5_path, pid=pid)
        except Exception:
            logger.warning("terminal_process_stop_failed", path=mt5_path, pid=pid, exc_info=True)


def start_terminal_protected(mt5_path: str, *, portable: bool) -> bool:
    """Ensure the exact MT5 terminal is protected and starts minimized.

    Returns ``True`` when a new protected process was launched. An existing
    exact-path process is reused only if its command line already contains
    ``/skipupdate``; an unprotected instance is replaced before MT5 IPC binds.
    """
    existing = _terminal_processes(mt5_path)
    if existing and all("/skipupdate" in command.lower() for _, command in existing):
        return False
    if existing:
        stop_terminal_process(mt5_path)
        time.sleep(0.5)

    args = [mt5_path]
    if portable:
        args.append("/portable")
    args.append("/skipupdate")
    startupinfo = None
    if os.name == "nt":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        # Minimized without taking keyboard focus from an operator session.
        startupinfo.wShowWindow = _SW_SHOWMINNOACTIVE

    process = subprocess.Popen(
        args,
        cwd=os.path.dirname(mt5_path) or None,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        creationflags=(
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        ),
        startupinfo=startupinfo,
    )
    if os.name == "nt":
        # ``mt5.initialize`` can otherwise race the terminal bootstrap and
        # launch its own plain ``/portable`` process before the protected
        # instance is ready for IPC. Wait until the GUI process has completed
        # its initialization, while retaining a short fallback for unusual
        # Windows environments and test doubles.
        try:
            if _user32 is not None:
                _user32.WaitForInputIdle(int(process._handle), 10_000)  # type: ignore[attr-defined]
        except Exception:
            logger.debug("terminal_input_idle_wait_failed", path=mt5_path, exc_info=True)
        time.sleep(0.5)
    logger.info("terminal_process_started_protected", path=mt5_path, portable=portable)
    return True


# ------------------------------------------------------------------
# Win32 helpers for minimizing terminal windows after connect
# ------------------------------------------------------------------

_user32 = ctypes.windll.user32 if os.name == "nt" else None  # type: ignore[attr-defined]
_SW_MINIMIZE = 6
_SW_SHOWMINNOACTIVE = 7


def _find_window_by_pid(pid: int) -> int | None:
    """Return the first top-level window handle owned by *pid*, or None."""
    if _user32 is None:
        return None
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
    if _user32 is None:
        return
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

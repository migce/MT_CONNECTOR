"""
MT5 Connector — Process Supervisor.

Lightweight watchdog service that:
  - starts, stops and restarts the **poller** and **trader** processes
  - auto-restarts crashed processes (unless intentionally stopped)
  - exposes a REST API on port ``SUPERVISOR_PORT`` (default 9100)
  - prevents duplicate instances via PID tracking

Run::

    python -m src.supervisor          # foreground
    python -m src.supervisor --install # install Windows auto-start

API (default http://localhost:9100):

    GET  /status                        — overview of all services
    POST /services/{name}/start         — start a service
    POST /services/{name}/stop          — stop (will NOT auto-restart)
    POST /services/{name}/restart       — graceful restart
    POST /services/start-all            — start all services
    POST /services/stop-all             — stop all services
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUPERVISOR_PORT = int(os.environ.get("SUPERVISOR_PORT", "9100"))
WATCHDOG_INTERVAL = int(os.environ.get("SUPERVISOR_WATCHDOG_SEC", "5"))

# Project root = directory containing src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")

logger = structlog.get_logger("supervisor")


# ---------------------------------------------------------------------------
# Low-level PID helpers (used by ServiceDef, must be defined first)
# ---------------------------------------------------------------------------

def _pid_alive(pid: int) -> bool:
    """Check if a given PID is still running (Windows-compatible)."""
    import ctypes
    import ctypes.wintypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259

    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return False
    try:
        exit_code = ctypes.wintypes.DWORD()
        if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return exit_code.value == STILL_ACTIVE
        return False
    finally:
        kernel32.CloseHandle(handle)


# ---------------------------------------------------------------------------
# Service definitions
# ---------------------------------------------------------------------------

class DesiredState(str, Enum):
    RUNNING = "running"   # should be up — watchdog will restart on crash
    STOPPED = "stopped"   # intentionally stopped — watchdog ignores


@dataclass
class ServiceDef:
    """Describes a managed process."""

    name: str
    cmd: list[str]
    desired: DesiredState = DesiredState.STOPPED
    process: subprocess.Popen | None = field(default=None, repr=False)
    _adopted_pid: int | None = field(default=None, repr=False)
    exit_code: int | None = None
    started_at: float | None = None
    stopped_at: float | None = None
    restarts: int = 0

    # Lock files created by the child processes themselves
    lock_file: str = ""
    # Pattern to find existing process via command line scan
    cmd_pattern: str = ""
    log_file: str = ""
    _log_handle: Any | None = field(default=None, repr=False)

    @property
    def pid(self) -> int | None:
        if self.process and self.process.poll() is None:
            return self.process.pid
        if self._adopted_pid and _pid_alive(self._adopted_pid):
            return self._adopted_pid
        return None

    @property
    def alive(self) -> bool:
        if self.process is not None and self.process.poll() is None:
            return True
        if self._adopted_pid and _pid_alive(self._adopted_pid):
            return True
        return False

    @property
    def uptime_sec(self) -> float | None:
        if self.started_at and self.alive:
            return round(time.time() - self.started_at, 1)
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "desired": self.desired.value,
            "alive": self.alive,
            "pid": self.pid,
            "exit_code": self.exit_code,
            "uptime_sec": self.uptime_sec,
            "restarts": self.restarts,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "log_file": self.log_file,
        }


# Two managed services
SERVICES: dict[str, ServiceDef] = {
    "poller": ServiceDef(
        name="poller",
        cmd=[PYTHON, "-m", "src.poller_main", "--dashboard"],
        lock_file=".poller.lock",
        cmd_pattern="src.poller_main",
    ),
    "trader": ServiceDef(
        name="trader",
        cmd=[PYTHON, "-m", "src.trader_main"],
        lock_file=".trader.lock",
        cmd_pattern="src.trader_main",
    ),
}


# ---------------------------------------------------------------------------
# Process management helpers
# ---------------------------------------------------------------------------

def _read_lock_pid(svc: ServiceDef) -> int | None:
    """Read PID from an existing lock file (the poller writes its PID there)."""
    if not svc.lock_file:
        return None
    lock = PROJECT_ROOT / svc.lock_file
    if not lock.exists():
        return None
    try:
        text = lock.read_text().strip()
        return int(text) if text.isdigit() else None
    except (OSError, ValueError):
        return None


def _find_existing_pids(pattern: str) -> list[int]:
    """Find PIDs of processes whose command line contains *pattern*.

    Uses ``wmic`` on Windows.  Returns an empty list on failure.
    """
    try:
        out = subprocess.check_output(
            [
                "wmic", "process", "where",
                f"commandline like '%{pattern}%' and not commandline like '%wmic%'",
                "get", "processid",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        pids: list[int] = []
        for line in out.strip().splitlines():
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
        return pids
    except Exception:
        return []


def _adopt_existing(svc: ServiceDef) -> bool:
    """If a process is already running (started before supervisor), adopt it."""
    if not svc.cmd_pattern:
        return False
    pids = _find_existing_pids(svc.cmd_pattern)
    # Exclude the supervisor's own PID
    my_pid = os.getpid()
    pids = [p for p in pids if p != my_pid]
    if not pids:
        return False
    # Adopt the first found PID
    pid = pids[0]
    logger.info("adopting_existing_process", service=svc.name, pid=pid,
                extra_pids=pids[1:] if len(pids) > 1 else None)
    svc.desired = DesiredState.RUNNING
    svc.started_at = time.time()
    svc.exit_code = None
    svc._adopted_pid = pid
    return True

def _clean_lock(svc: ServiceDef) -> None:
    """Remove stale lock file if the process is dead."""
    if svc.lock_file:
        lock = PROJECT_ROOT / svc.lock_file
        if lock.exists():
            try:
                lock.unlink()
                logger.info("stale_lock_removed", service=svc.name, path=str(lock))
            except OSError:
                pass


def _close_log_handle(svc: ServiceDef) -> None:
    """Close the log file handle held for a spawned child process."""
    if svc._log_handle is not None:
        try:
            svc._log_handle.close()
        except OSError:
            pass
        svc._log_handle = None


def _start_process(svc: ServiceDef) -> None:
    """Spawn the child process (no-op if already alive)."""
    if svc.alive:
        logger.warning("already_running", service=svc.name, pid=svc.pid)
        return

    # Clear adopted PID if it was stale
    svc._adopted_pid = None
    _clean_lock(svc)

    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"{svc.name}.out.log"
    _close_log_handle(svc)
    svc._log_handle = log_path.open("a", encoding="utf-8", buffering=1)
    svc.log_file = str(log_path)

    logger.info("starting_service", service=svc.name, cmd=svc.cmd)
    svc.process = subprocess.Popen(
        svc.cmd,
        cwd=str(PROJECT_ROOT),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,  # allows CTRL_BREAK
        stdout=svc._log_handle,
        stderr=subprocess.STDOUT,
    )
    svc.started_at = time.time()
    svc.stopped_at = None
    svc.exit_code = None
    svc.desired = DesiredState.RUNNING
    logger.info("service_started", service=svc.name, pid=svc.process.pid)


def _stop_process(svc: ServiceDef, timeout: float = 10.0) -> None:
    """Gracefully stop a child process (spawned or adopted)."""
    svc.desired = DesiredState.STOPPED

    # --- Handle adopted process (no Popen handle) ---
    if svc._adopted_pid and _pid_alive(svc._adopted_pid):
        pid = svc._adopted_pid
        logger.info("stopping_adopted_service", service=svc.name, pid=pid)
        try:
            os.kill(pid, signal.CTRL_BREAK_EVENT)
        except OSError:
            pass
        # Wait for it to die
        deadline = time.time() + timeout
        while _pid_alive(pid) and time.time() < deadline:
            time.sleep(0.5)
        if _pid_alive(pid):
            logger.warning("force_killing_adopted", service=svc.name, pid=pid)
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
        svc._adopted_pid = None
        svc.stopped_at = time.time()
        _clean_lock(svc)
        _close_log_handle(svc)
        logger.info("adopted_service_stopped", service=svc.name)
        return

    # --- Handle spawned process (Popen handle) ---
    if not svc.alive:
        return

    logger.info("stopping_service", service=svc.name, pid=svc.pid)

    # Send CTRL_BREAK (Windows equivalent of SIGTERM for process groups)
    try:
        os.kill(svc.process.pid, signal.CTRL_BREAK_EVENT)  # type: ignore[union-attr]
    except OSError:
        pass

    try:
        svc.process.wait(timeout=timeout)  # type: ignore[union-attr]
    except subprocess.TimeoutExpired:
        logger.warning("force_killing", service=svc.name, pid=svc.pid)
        svc.process.kill()  # type: ignore[union-attr]
        svc.process.wait(timeout=5)  # type: ignore[union-attr]

    svc.exit_code = svc.process.returncode  # type: ignore[union-attr]
    svc.stopped_at = time.time()
    svc.process = None
    _clean_lock(svc)
    _close_log_handle(svc)
    logger.info("service_stopped", service=svc.name, exit_code=svc.exit_code)


def _restart_process(svc: ServiceDef) -> None:
    """Stop then start."""
    _stop_process(svc)
    svc.restarts += 1
    _start_process(svc)


# ---------------------------------------------------------------------------
# Watchdog loop
# ---------------------------------------------------------------------------

_shutdown_event = asyncio.Event()


async def _watchdog_loop() -> None:
    """Periodically check service health and restart crashed processes."""
    logger.info("watchdog_started", interval=WATCHDOG_INTERVAL)
    while not _shutdown_event.is_set():
        for svc in SERVICES.values():
            if svc.desired == DesiredState.RUNNING and not svc.alive:
                # Process crashed — collect exit code and restart
                if svc.process is not None:
                    svc.exit_code = svc.process.returncode
                    svc.process = None
                    _close_log_handle(svc)
                if svc._adopted_pid:
                    svc._adopted_pid = None
                svc.stopped_at = time.time()
                logger.warning(
                    "service_crashed",
                    service=svc.name,
                    exit_code=svc.exit_code,
                )
                svc.restarts += 1
                _start_process(svc)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=WATCHDOG_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("watchdog_stopped")


# ---------------------------------------------------------------------------
# Per-service operation locks (prevent concurrent start/stop/restart)
# ---------------------------------------------------------------------------

_service_locks: dict[str, asyncio.Lock] = {}


def _get_lock(name: str) -> asyncio.Lock:
    if name not in _service_locks:
        _service_locks[name] = asyncio.Lock()
    return _service_locks[name]


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="MT5 Supervisor",
    version="1.0.0",
    docs_url="/docs",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_svc(name: str) -> ServiceDef:
    svc = SERVICES.get(name)
    if svc is None:
        raise HTTPException(404, f"Unknown service '{name}'. Valid: {list(SERVICES)}")
    return svc


@app.get("/status", summary="Status of all managed services")
async def get_status() -> dict:
    locks_busy = {n: _get_lock(n).locked() for n in SERVICES}
    return {
        "supervisor_pid": os.getpid(),
        "watchdog_interval_sec": WATCHDOG_INTERVAL,
        "services": {
            name: {**svc.to_dict(), "operation_in_progress": locks_busy.get(name, False)}
            for name, svc in SERVICES.items()
        },
    }


@app.post("/services/{name}/start", summary="Start a service")
async def start_service(name: str) -> dict:
    svc = _get_svc(name)
    lock = _get_lock(name)
    if lock.locked():
        raise HTTPException(409, f"Operation already in progress for '{name}'")
    async with lock:
        if svc.alive:
            return {"status": "already_running", **svc.to_dict()}
        _start_process(svc)
        return {"status": "started", **svc.to_dict()}


@app.post("/services/{name}/stop", summary="Stop a service (won't auto-restart)")
async def stop_service(name: str) -> dict:
    svc = _get_svc(name)
    lock = _get_lock(name)
    if lock.locked():
        raise HTTPException(409, f"Operation already in progress for '{name}'")
    async with lock:
        _stop_process(svc)
        return {"status": "stopped", **svc.to_dict()}


@app.post("/services/{name}/restart", summary="Restart a service")
async def restart_service(name: str) -> dict:
    svc = _get_svc(name)
    lock = _get_lock(name)
    if lock.locked():
        raise HTTPException(409, f"Operation already in progress for '{name}'")
    async with lock:
        _restart_process(svc)
        return {"status": "restarted", **svc.to_dict()}


@app.post("/services/start-all", summary="Start all services")
async def start_all() -> dict:
    results = {}
    for name, svc in SERVICES.items():
        lock = _get_lock(name)
        if lock.locked():
            results[name] = "operation_in_progress"
            continue
        async with lock:
            if not svc.alive:
                _start_process(svc)
                results[name] = "started"
            else:
                results[name] = "already_running"
    return results


@app.post("/services/stop-all", summary="Stop all services (won't auto-restart)")
async def stop_all() -> dict:
    results = {}
    for name, svc in SERVICES.items():
        lock = _get_lock(name)
        if lock.locked():
            results[name] = "operation_in_progress"
            continue
        async with lock:
            _stop_process(svc)
            results[name] = "stopped"
    return results


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    """Adopt running processes or start fresh, then begin watchdog."""
    for svc in SERVICES.values():
        if _adopt_existing(svc):
            logger.info("adopted_service", service=svc.name, pid=svc._adopted_pid)
        else:
            _start_process(svc)
    asyncio.create_task(_watchdog_loop(), name="watchdog")
    logger.info(
        "supervisor_started",
        port=SUPERVISOR_PORT,
        services=list(SERVICES.keys()),
    )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Gracefully stop all children."""
    _shutdown_event.set()
    for svc in SERVICES.values():
        if svc.alive:
            _stop_process(svc)
    logger.info("supervisor_shutdown")


# ---------------------------------------------------------------------------
# Auto-start installer (Windows Startup folder)
# ---------------------------------------------------------------------------

def _install_autostart() -> None:
    """Create a shortcut in the Windows Startup folder."""
    try:
        import winshell  # type: ignore[import-untyped]
    except ImportError:
        print("Installing winshell...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "winshell"])
        import winshell  # type: ignore[import-untyped]

    startup = winshell.startup()
    link_path = os.path.join(startup, "MT5 Supervisor.lnk")

    try:
        from win32com.client import Dispatch  # type: ignore[import-untyped]
    except ImportError:
        print("Installing pywin32...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pywin32"])
        from win32com.client import Dispatch  # type: ignore[import-untyped]

    shell = Dispatch("WScript.Shell")
    sc = shell.CreateShortCut(link_path)
    sc.TargetPath = PYTHON
    sc.Arguments = "-m src.supervisor"
    sc.WorkingDirectory = str(PROJECT_ROOT)
    sc.Description = "MT5 Supervisor — auto-manages poller & trader"
    sc.save()
    print(f"✓ Autostart shortcut created: {link_path}")


# ---------------------------------------------------------------------------
# PowerShell install script generator
# ---------------------------------------------------------------------------

def _generate_install_script() -> None:
    """Generate scripts/install_supervisor.ps1."""
    script_dir = PROJECT_ROOT / "scripts"
    script_dir.mkdir(exist_ok=True)
    ps_path = script_dir / "install_supervisor.ps1"

    content = f'''\
# ── MT5 Supervisor — Windows Startup Installer ──
# Creates a shortcut in the user Startup folder so the supervisor
# starts automatically when you log in.

$python  = "{PYTHON}"
$workDir = "{PROJECT_ROOT}"
$startup = [Environment]::GetFolderPath("Startup")
$lnk     = Join-Path $startup "MT5 Supervisor.lnk"

$shell = New-Object -ComObject WScript.Shell
$sc = $shell.CreateShortcut($lnk)
$sc.TargetPath       = $python
$sc.Arguments         = "-m src.supervisor"
$sc.WorkingDirectory  = $workDir
$sc.Description       = "MT5 Supervisor — auto-manages poller and trader"
$sc.WindowStyle       = 7   # minimized
$sc.Save()

Write-Host "Shortcut created: $lnk" -ForegroundColor Green
Write-Host "The supervisor will start automatically at next logon."
'''
    ps_path.write_text(content, encoding="utf-8")
    print(f"✓ Install script generated: {ps_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="MT5 Process Supervisor")
    parser.add_argument(
        "--install", action="store_true",
        help="Install Windows auto-start shortcut and exit",
    )
    parser.add_argument(
        "--generate-script", action="store_true",
        help="Generate scripts/install_supervisor.ps1 and exit",
    )
    parser.add_argument(
        "--port", type=int, default=SUPERVISOR_PORT,
        help=f"API port (default: {SUPERVISOR_PORT})",
    )
    args = parser.parse_args()

    if args.install:
        _install_autostart()
        _generate_install_script()
        return

    if args.generate_script:
        _generate_install_script()
        return

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()

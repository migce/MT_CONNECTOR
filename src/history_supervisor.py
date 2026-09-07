"""Bounded supervision of ONE owned history Python child, never terminal PIDs.

The scheduler starts this process. Transient status loss is handled in-process
by the worker; an actual crash/native timeout has at most three retries. Safety
conflicts and exhausted budgets persist a circuit-open state across scheduler
restarts. Re-arming requires explicit operator review of the saved state.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path


class HistorySupervisor:
    def __init__(self, root, *, spawn=None, clock=time.time, sleep=time.sleep,
                 startup_grace=60, heartbeat_deadline=30, stable_seconds=600):
        self.root = Path(root)
        self.clock, self.sleep = clock, sleep
        self.startup_grace = startup_grace
        self.heartbeat_deadline = heartbeat_deadline
        self.stable_seconds = stable_seconds
        self.spawn = spawn or self._spawn
        self.path = self.root / ".runtime" / "history-supervisor.json"
        self.child_health = self.root / ".runtime" / "history-worker-health.json"
        self.failures = 0
        self.child = None
        self.log = logging.getLogger("history_supervisor")

    def _spawn(self):
        return subprocess.Popen(
            [sys.executable, "-m", "src.history_main"], cwd=self.root,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

    def record(self, state, **fields):
        data = dict(state=state, observed_at=self.clock(), supervisor_pid=os.getpid(),
                    failures=self.failures, **fields)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(data), encoding="utf8")
        temporary.replace(self.path)
        self.log.info(json.dumps(data))

    def health(self):
        try:
            data = json.loads(self.child_health.read_text(encoding="utf8"))
            age = self.clock() - float(data["observed_at"])
            if data["pid"] != self.child.pid or not 0 <= age < self.heartbeat_deadline:
                return None
            return data
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def stop_owned_child(self):
        # Popen retains a process handle. No PID lookup, taskkill /T, terminal
        # discovery or arbitrary process-tree termination is permitted here.
        if self.child is not None and self.child.poll() is None:
            self.child.kill()
            self.child.wait(timeout=5)

    def run(self):
        self.path.parent.mkdir(exist_ok=True)
        if self.path.exists():
            try:
                previous = json.loads(self.path.read_text(encoding="utf8"))
                self.failures = max(0, int(previous.get("failures", 0)))
                if previous.get("state") == "circuit_open" or self.failures > 3:
                    self.record("circuit_open", reason="operator_review_required")
                    return 78
            except (ValueError, TypeError):
                self.record("circuit_open", reason="supervisor_state_invalid")
                return 78
        try:
            while True:
                try:
                    self.child = self.spawn()
                except OSError:
                    self.record("circuit_open", reason="history_spawn_failed")
                    return 78
                started = self.clock()
                last_progress = started
                stable_since = None
                self.record("running", child_pid=self.child.pid)
                while self.child.poll() is None:
                    now = self.clock()
                    health = self.health()
                    if health:
                        last_progress = now
                    if health and health.get("connected") is True:
                        stable_since = stable_since if stable_since is not None else now
                        if self.failures and now - stable_since >= self.stable_seconds:
                            self.failures = 0
                            self.record("running", child_pid=self.child.pid, reason="stable_budget_reset")
                    else:
                        stable_since = None
                    if now - started >= self.startup_grace and now - last_progress >= self.heartbeat_deadline:
                        self.record("terminating_owned_child", child_pid=self.child.pid,
                                    reason="history_progress_missing")
                        self.stop_owned_child()
                        break
                    self.sleep(1)
                code = self.child.wait(timeout=5)
                self.failures += 1
                if code in (0, 78) or self.failures > 3:
                    self.record("circuit_open", child_exit=code,
                                reason="safety_conflict" if code == 78 else "retry_budget_exhausted")
                    return 78
                delay = (10, 30, 60)[self.failures - 1]
                self.record("backoff", child_exit=code, retry_after_seconds=delay)
                self.sleep(delay)
        finally:
            self.stop_owned_child()


def main():
    if os.name != "nt":
        raise RuntimeError("History supervisor requires Windows")
    import msvcrt
    root = Path(__file__).resolve().parents[1]
    # Prevent two schedulers from spawning competing generations or resetting
    # each other's retry state. This is separate from the child's history lock.
    with (root / ".history-supervisor.lock").open("a+b") as lock:
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        logs = root / "logs"
        logs.mkdir(exist_ok=True)
        logger = logging.getLogger("history_supervisor")
        logger.setLevel(logging.INFO)
        logger.addHandler(RotatingFileHandler(logs / "history-supervisor.log",
                                             maxBytes=1024*1024, backupCount=3, encoding="utf8"))
        return HistorySupervisor(root).run()


if __name__ == "__main__":
    sys.exit(main())

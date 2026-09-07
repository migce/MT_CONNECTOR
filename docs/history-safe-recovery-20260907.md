# History-only safe recovery

The 2026-09-07 outage was an intentional history exit after a short-lived
diagnostic Poller key lost its isolation flag. The ten-second status key is
published after sequential API/DB probes; missing evidence was incorrectly
conflated with explicit ownership conflict. Exact triggering probe and Windows
scheduler retry events were not retained. Do not describe this as proven LAN
failure or MT5 native hang.

## Contracts

- HistoryAuthority atomically reads the control Redis value plus PTTL. Missing,
  malformed, expired, or unavailable proof pauses admission. Every history
  native call revalidates, including calls within a multi-chunk job. There is
  no cached-success fallback for admitting new calls, no extra executor, no
  permanently true isolation flag. Local proof expiry never exceeds Redis TTL.
- Explicit false or a changed Poller generation is a sticky conflict. The
  worker exits78, as it does on broker permission/identity drift or another
  history OS/DB owner. It never automatically switches authority generations.
- During transient loss, worker health is reconnecting/disconnected while the
  DB singleton lease remains supervised separately. On proof return the same
  generation resumes. Previously submitted native calls retain their original
  timeout; already received partial data is not deleted. Existing job deadlines
  still apply: prolonged outages may finish a job as failed rather than wait
  forever or silently claim full coverage.
- NativeCallBudget has an optional admission hook set only in history_main.
  Poller and Trader retain the default None, and their running processes are
  not reloaded. Source changes to shared code do not enable history gating for
  live traffic on a future restart either.
- History service now attaches only to exactly one existing dedicated terminal
  found by a bounded five-second process read. No protected-launch replacement
  helper, terminal stop, update or explicit login credentials are used on this
  attach path. MT5 initialize is still required to bind the Python IPC; process
  identity and broker account/permission checks follow it. If the terminal is
  absent or duplicated, fail closed and request operator review.
- The scheduler starts history_supervisor, which owns only its Popen child.
  Windows venv redirectors are bypassed using CPython's base-interpreter launch
  convention and __PYVENV_LAUNCHER__ to preserve the venv. The owned process
  handle PID must equal the actual worker heartbeat PID. Real Windows process
  tests prove PID equality, venv dependency imports, and owned-child termination.
  Missing worker progress kills only that handle, never a terminal or a process
  tree. Three retries use 10/30/60-second backoff. Ten minutes of connected
  progress resets the budget. Conflict/exhaustion opens a persistent circuit;
  scheduler restarts cannot silently reset it. Circuit reopening requires
  explicit review/re-arming. No unlimited restart storm.
- Worker publishes local progress even while authority is missing, so safe
  waiting is not mistaken for an event-loop hang. Individual Windows file
  sharing failures do not terminate the worker; persistent missing progress
  is bounded by the supervisor deadline.

## Deployment and rollback

`scripts/history_recovery_release.py` stages/qualifies/deploys/verifies the exact
six-file native delta from d8c67c6. It requires the old task Ready, zero history
processes/lease owners/unfinished jobs, current live isolation and the expected
responsive five-terminal fleet. It backs up each replaced file and task XML,
briefly disables only the already-stopped history task during replacement,
then updates its action and starts it. No Monitor, Poller, Trader, terminal,
DB/Redis/WSL restart or bulk history request is part of this driver.

Windows backups and verification: `.codex/releases/history-recovery-20260907/`.
The first rollout connected but exposed a Windows venv launcher/actual PID
mismatch that the initial mocked Popen tests missed. Its launcher-only
verification was invalid and is superseded. Only the history task was stopped;
both actual and launcher processes were confirmed absent. No terminal or live
trading process was stopped.

`scripts/history_recovery_attempt2.py` is the separate, reviewed one-shot
correction: it retains the original artifacts, qualifies real-process tests,
backs up and replaces only history_supervisor.py, and archives the reviewed
first-attempt retry state (backoff/failures1) without discarding jobs. It checks
all versioned Python executable names, both live owner processes and their venv
launchers, three Trader children, and all five terminals. Final verification
requires the actual history heartbeat/Redis/Popen PID to match, parent identity
to be the actual supervisor, and no extra history child. Observe beyond the
original sixty-second false-timeout threshold before declaring success.

`baseline.json` is a one-shot guard: do not rerun deploy after an attempt.
If interrupted before start, inspect actual state before enabling the task.
Rollback needs explicit history-role authority: stop only the verified owned
history processes, restore backed-up source/task XML, preserve all job data,
and reverify current isolation/lease/terminal identity before a new launch.
Never use old generic rollout scripts that restart Poller/Trader.

Global web maintenance is not applicable: no web container or schema is
changed. An independent minimal Poller heartbeat would further remove the
diagnostic-probe dependency, but requires separate qualification and an
authorized Poller transition; it is not included in this history-only repair.

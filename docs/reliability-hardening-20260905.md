# Connector reliability release — 2026-09-05

Status: installed and running with explicit user authorization; final runtime source `6986f1d` in branch `codex/reliability-hardening-20260905`, based on deployed baseline `d2b1baa`. See the installed-release update below and the joint production report for final images/tests and residual risks. Do not mix this native parent/child with the old IPC protocol.

## Safety changes

- Correlated request UUID, command, fixed child generation and protocol; bounded IPC queues and total queue/execution deadline.
- Unknown/late broker outcome never triggers blind resend; unknown journal commands block that account until explicit broker reconciliation.
- Fixed HTTP command authority: retries keep UUID/payload/expiry; expired existing command can still be acknowledged without extending its authority.
- Account login proof before interpreting an empty native position result and during post-send reconciliation; original exact-ticket/identifier/symbol/type/magic/volume guards preserved.
- Only definitive IOC partial fill permits another close of the verified remainder. RETURN/PLACED/TIMEOUT/LOCKED/CONNECTION uncertainty is not retry authority.
- Atomic fresh/complete position snapshot for Monitor; invalid/missing proof returns unavailable, not an empty list.
- Per-account dispatch, atomic account claim lock, attempt fencing, continuous stale-claim quarantine, pre-dispatch durable audit, reserved command DB pool.
- Default SQLite WAL/FULL tick spool, ACK after DB and watermarks; quota raises instead of deleting accepted ticks. Physical SQLite/WAL size exceeds logical payload quota. Set a stable absolute spool path at deployment.
- Shared native-read lane has admission/deadline/quarantine; this does **not** interrupt an already blocked C/native thread.
- Durable queued backfill polling and atomic claims; tick insert/upsert returns actual PostgreSQL counts.

## Verification

193 passed / 4 skipped in the full Connector suite, with a localhost disposable PostgreSQL16 fixture for command concurrency/fencing/recovery and row counts. Synthetic broker, queue, cancellation and persistence tests contain no real order sends. Live tests require explicit isolated-host opt-in; old embedded test credential removed from this candidate, not from repository history.

Windows native Python spool microbenchmark used its own temporary directory, 500 synthetic rows, FULL commit mean0.440ms/p950.712ms/max1.489ms; all500 reopened. This is not power-loss/MT5/demo soak qualification.

Candidate Dockerfile.reliability overlays reviewed sources onto the exact inspected production dependency image `sha256:70fd50694485b1b0cd30d42f730132d7311b6e8dadeccf818915bf6c86243c51`. The local source image tag must resolve to that baseline. Dependencies were not upgraded. Sandbox import/snapshot-route smoke ran with network disabled and no application lifespan.

## Deployment gate

07:10MSK cutover reconciliation found native-only startup settlement, bounded tick batches and fatal-history callback missing from the API baseline. Preserved native backfill.py/poller_main.py in this candidate, retained the callback contract, and added SIGBREAK handling for the Supervisor's Windows stop signal. Native heavy_reads.py is also required by the new repository import. This correction must be built/retested before installation; the earlier 15-file parity check was insufficient for the native transitive dependency surface.

A coordinated native Poller/Trader replacement is necessary, potentially interrupting trading sessions. Require an explicit window, fresh command/intent/account state, preserved rollback sources/images/configuration, and no unresolved broker outcome. Never clear commands/spool or send real orders to test recovery.

Connector/native atomic snapshot must be active before the new Monitor consumer. Publish a lease-aware embedded Monitor controller before enabling its standalone replacement. Preserve policy/allowlist/MT5 build, secrets and trading semantics. Production fault injection is not authorized.

The complete staged cutover/rollback and R1–R11 residual-risk matrix live in the companion Monitor candidate at `docs/reliability-hardening-20260905.md`.

Still open: killable native live/history process separation, physical command-store independence from Windows Timescale/WSL, persistent Redis control separation and running-job leases, time/coverage/revision migration, off-host backup/PITR/restore and long-duration qualification. Unknown-resolution is deliberately not an automatic mutation.

## Reproduce tests safely

Use a dedicated local PostgreSQL fixture named database `reliability_test`, bound to127.0.0.1 on a non5432 port. Set RELIABILITY_TEST_DSN to that fixture and run the full pytest suite. Never point it at mt5_data/production. Leave CONNECTOR_ENABLE_LIVE_FIXTURE_TESTS unset unless an explicitly approved isolated API/MT5 fixture exists.
# Installed release update — 2026-09-05 07:42 MSK

Пакет установлен с разрешения пользователя. Runtime source `6986f1d`, image `213719dc...`, 18 native files verified; Poller29204/Trader32168, required3/active3/standby3, 12 positions still open. Tests200passed/4skipped. Final validation:60 snapshots without errors, current Poller errors0 (6705 previous_session preserved), spool drains. Candle matrix now respects native admission; snapshot future skew bounded2s, stale30s and commandTTL unchanged. No DB/Redis/WSL restart, trade command, policy change or journal/spool deletion. Joint report: `/Users/migce/pyProjects/mt5-monitor/.codex/releases/reliability-20260905/production-result.md`. The original candidate qualification below is historical; remaining physical isolation/restore risks are still open.

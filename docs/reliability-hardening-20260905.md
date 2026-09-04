# Connector reliability candidate — 2026-09-05

Status: implemented/tested in branch `codex/reliability-hardening-20260905` from deployed baseline `d2b1baa`; **not deployed**. Do not mix candidate native parent/child with the old IPC protocol.

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

A coordinated native Poller/Trader replacement is necessary, potentially interrupting trading sessions. Require an explicit window, fresh command/intent/account state, preserved rollback sources/images/configuration, and no unresolved broker outcome. Never clear commands/spool or send real orders to test recovery.

Connector/native atomic snapshot must be active before the new Monitor consumer. Publish a lease-aware embedded Monitor controller before enabling its standalone replacement. Preserve policy/allowlist/MT5 build, secrets and trading semantics. Production fault injection is not authorized.

The complete staged cutover/rollback and R1–R11 residual-risk matrix live in the companion Monitor candidate at `docs/reliability-hardening-20260905.md`.

Still open: killable native live/history process separation, physical command-store independence from Windows Timescale/WSL, persistent Redis control separation and running-job leases, time/coverage/revision migration, off-host backup/PITR/restore and long-duration qualification. Unknown-resolution is deliberately not an automatic mutation.

## Reproduce tests safely

Use a dedicated local PostgreSQL fixture named database `reliability_test`, bound to127.0.0.1 on a non5432 port. Set RELIABILITY_TEST_DSN to that fixture and run the full pytest suite. Never point it at mt5_data/production. Leave CONNECTOR_ENABLE_LIVE_FIXTURE_TESTS unset unless an explicitly approved isolated API/MT5 fixture exists.

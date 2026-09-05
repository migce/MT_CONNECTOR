# Dedicated history isolation — production result

Verified 2026-09-05 09:30 MSK. This record supersedes preparation notes and the previous 512 MiB candidate. User explicitly authorized an additional portable terminal/broker history session and service installation. No order actions, position changes, execution-policy changes, journal clearing, DB/Redis/WSL restarts, or unrelated dirty-tree releases were performed.

## Installed scope

- Connector API image `sha256:df4f77271d84378f1a75fecbe2921212ee46817f4d6b13015a19a474a486b1b7`, tag `mt-connector-api:history-isolated-20260905`.
- Ten native files match the exact staged manifest. API health/schema changes are image-only because the native schema contained unrelated differences. Both API files match candidate SHA-256.
- Live Poller PID31412 publishes `history_isolated=true`. Its native executor and terminal no longer execute startup/reconnect/on-demand history work. Known gap ranges are persisted before advancing live watermarks; history cannot move watermarks backwards.
- History task `MT5 History Worker`, running Python PID39108, generation `6e456a2d710d48efb6cf542daffded8e`; one reserved PostgreSQL advisory lease771205069 and OS singleton lock. Interactive user-logon startup, single instance, at most three one-minute failure retries. This is not an unattended pre-logon Windows service.
- Dedicated terminal `C:\MT5_History\terminal64.exe`, PID30388, build6140. Expected total fleet five; four original PIDs25524/29680/31496/44028 unchanged and responsive. History path/login verified. Terminal AutoTrading and Python trading disabled (`trade_allowed=false`, `tradeapi_disabled=true`). This uses existing Poller broker credentials in a separate terminal session, not broker-enforced investor credentials.
- History reads are sequential, tick chunks one minute, candle chunks 1000 timeframe intervals, pre-expansion cap100000 rows/16MiB, Python process JobObject cap1024MiB, numerical threads1. A tracked overdue native call exits only the history Python generation. A hung MT5 GUI process, underlying host/storage failure and terminal memory are not covered by the Python cap.
- Worker health uses fresh bounded-age observations; health/status routes share a reader and retain previous valid proof only until its original30-second age limit. New negative proof overrides healthy proof; missing/expired proof is not recovery. Small future skew tolerance2seconds does not extend stale lifetime.
- Windows role-aware collector installed SHA256 `f8ec6687ee1102270647bb13965b14dc3837bdca530fbcbd40cfad36b6b01a8c`. No agent restart. Central sample06:28:31UTC has no collector errors, expected5/actual5/historyconnectedtrue.

## Verification

- Connector full suite228 passed/4 skipped, including actual disposable PostgreSQL transaction/monotonic-cursor tests and an actual spawned fake hung-native process exiting70. No production broker fault injection or trade test.
- Monitor telemetry tests3 passed; Windows parser test skips on Mac and passes on actual Windows PowerShell. Staged collector was also executed successfully on Windows before the final copy.
- Full Windows worker-import/OS-limit qualification: DB read true, Redis ping true, ten broker bars read, identity/read-only controls true. Full startup reached expected legacy-ownership fence before cutover.
- Twenty consecutive read-only batches: no request failures, all five core/history flags true; sixty complete position snapshots. Accounts67/68/84 remain4/6/2 positions, generation stable, account_info advancing. Final position proofs06:29:41–42UTC. Configured6, required/active3, healthy standby3. Trader PID32168 unchanged.
- History generation stable; observations and last_success advance; pending/overdue native calls0. Sample history Python56.1MB working/101.5MB private, terminal134.0MB working/344.5MB private. Not a long-duration memory soak.
- Poller errors6705→6705, all attributed to previous_session; current-session errors0. Sampled buffer0–11, DB3.2–5.9ms. No unresolved accepted/retry/claimed/unknown commands; no pending Monitor execution intents. One execution owner771205068, embedded=false, policy revision12 unchanged.
- Current history generation processed98 jobs, fourteen succeeded and eighty-four partial; none remained running in the SQL snapshot. Partial rows remain explicitly partial, not silently successful: requested ends extend into Saturday while latest broker coverage ends around Friday close; exact full calendar correctness was not established. Counters report18900+56 rows_written (not proof of that many newly inserted unique rows). No failed-generation fatal/timeout markers after final cutover.
- Monitor API/execution images remain7877e970, frontend a2606b55. API/frontend healthy, restart0; standalone execution stopped/restarted deliberately within each cutover and now healthy with one owner. DB/Redis unchanged.
- Maintenance inactive, public version `e6fdca90aaf192e6154b5440bc916f7a0d0fa1cb55fe2f11082191880c7ec40b`; HTTP/WebSocket metadata match. All140 public files match running frontend bytes. Post-final-cutover sampled logs: API439 HTTP200+10 HTTP202, no5xx/tracebacks; frontend591 HTTP200/4 HTTP304/2 HTTP499/10 HTTP202, no5xx; execution no error/traceback. These are short-window observations, not an uptime guarantee.

## Rollout regressions and recovery

1. Attempt1 maintenance05:58:31–06:00:11UTC: full worker could not create threads under512MiB despite the smaller terminal probe passing. Automatic rollback restored old Connector API/Poller and Monitor execution. Fixed1GiB limit, one numerical thread, full-import+DB+Redis qualification and sanitized startup failure logging.
2. Attempt2 maintenance06:10:18–06:11:02UTC: history processed jobs, but final verification received503, triggering rollback. Exact original503 cause was not proven. Native Windows clock was subsequently measured0.28–0.41s behind API, so future-clock skew is not an established explanation. Added shared bounded-age status proof/cache, HTTP regression tests and bounded GET-only verifier retries with endpoint/stack reporting. No claim that LAN failure caused this event.
3. Attempt3 completed with history installed; maintenance ended normally06:21:19.400425UTC. Original trading terminals and Trader preserved throughout. No cutover command was reissued after the wrapper finished.
4. Post-cutover Windows collector validation found an unparenthesized `try` expression in an intermediate hash literal. This produced `powershell_exit:1` central samples through06:27:27UTC. Fixed only that expression, qualified with actual PowerShell parser and runtime, then atomically copied. Central telemetry is clean again by06:28:31UTC. Original and faulty collector retained in rollback directory.

## Important residual risks / next gates

- **LiveUpdate warning:** new history terminal logs report available/download/success markers08:38:28–30 local time and advertised build6180, despite runtime `/skipupdate=true`. Executable is still6140, PID unchanged; a restart/later dialog was observed during setup. Treat this as downloaded/update-pending-restart evidence, not applied update and not confirmed protection. No update was applied manually. Qualify broker update control separately before a future terminal restart; do not delete staged artifacts or alter the trading fleet automatically.
- Terminal-level disabled trading is verified but not a substitute for broker-issued investor/read-only credentials. Provisioning such credentials requires the owner's decision.
- Physical Windows DB/Redis/storage remain shared; control Redis still shares an evicting cache. Process isolation does not solve a WSL/kernel/DB outage, reconnect startup ordering beyond the bounded retries, cross-host restore/PITR, or realtime replay.
- No market-open stress, long soak, power-loss, or demo-broker trade failure test. Do not assert all architecture findings closed or guarantee no future hangs.

## Provenance and rollback

Candidates: both `.codex/worktrees/reliability-20260905` branches; unrelated primary checkouts preserved. Current Windows Compose overlay remains `C:\pyProjects\MT_Connector\.codex\releases\reliability-20260905\compose.release.yml`; generic primary-tree Compose may revert the installed architecture and must not be used blindly.

Exact before/after manifest and guarded drivers are in this directory. Windows rollback bundles are `.codex\rollback\history-20260905`, `history-20260905-attempt2`, and `history-20260905-attempt3`. Original Connector API213719dc is retained. Rollback requires a newly checked empty command journal and an authorized maintenance-attached window: pause sole Monitor execution, disable/stop only history task, stop only Poller, restore exact backed-up native files/env/overlay/collector, restore prior API, start Poller, restore sole execution and verify health/positions. Never use a blind same-day driver rerun as rollback. Additional history terminal must be reconciled explicitly if disabling the role; do not automatically kill it or count it as healthy legacy fleet.

Removed only owned disposable PostgreSQL container90c04178824299a74cc3dcf7f2835d0464274e2ef8acdd88df902260ee7f4ec2 (tmpfs synthetic data, recreatable) and temporary scheduled task `MT5 History Qualification 20260905`. Working task/terminal, all production volumes/journals/spools, candidate images and rollback bundles retained.

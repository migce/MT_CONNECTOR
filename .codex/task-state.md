# A3C-v6 Completed-Minute Drift Bars

## Active Task - 2026-08-24 A3C-v7 Visual Chart Presets

- Goal: expose A3C-v7 through Connector REST/WebSocket custom candles using
  four fixed chart-only presets analogous to M5, M15, M30 and M60 density.
- Status: completed, committed, pushed, deployed and production-verified on
  branch `codex/a3c-v7-chart-presets`.
- Decisions: preset codes must remain distinct from A1000 v2; no arbitrary
  user-tunable V7 inputs; no ATS, execution, database persistence, strategy
  eligibility or raw-tick delivery to Monitor.
- Result: frozen mappings are `V7M5=(5m, 0.4)`, `V7M15=(15m, 1.0)`,
  `V7M30=(30m, 2.0)` and `V7M60=(60m, 3.0)`. REST returns ready OHLCV
  bars and WebSocket uses the matching connection-local builder. The parser
  accepts only these four codes. Minute-state caching preserves exact output
  counts while reducing a one-million-tick replay from about 9.1s to 3.2s.
- Production parity: the V7 parser was applied over the exact running
  production config contract, preserving its existing trading, database and
  backfill settings; no ATS or execution values changed.
- Verification: 195/195 non-service Connector tests pass; the focused V1-V7,
  parser and custom-candle suite passes 135/135; full calibration repeats the
  exact selected budgets and density errors.
- Publication: commit `4f94e78` is pushed. Production runs image
  `sha256:724e7d86...` (`mt_connector-api:a3c-v7-chart-presets-20260824`)
  with rollback `mt_connector-api:rollback-before-a3c-v7-20260824`.
  Only `mt5_api` was recreated.
- Production verification: V7M5/V7M15/V7M30/V7M60 each returned 50/50
  completed, correctly labelled bars with `strategy_eligible=false`; V7M5 live
  WebSocket delivered developing frames; A1000 still returned its unchanged v2
  algorithm. API, MT5, Trader, DB and Redis are healthy; 3/3 accounts are
  healthy; no release-window errors were logged. Terminal PIDs remain exactly
  25048, 25524, 29680 and 31496.
- Next: user visual comparison in Market Monitor; no Connector follow-up needed
  unless the visual evaluation finds a research issue.
- Blockers: none.
- Last Updated: 2026-08-24 19:09 MSK

## Active Task - 2026-08-24 A3C-v7 Dual Clock Bars

- Goal: reduce v6 structural over-compression by combining a more responsive
  completed-minute displacement CUSUM with a causal trend-duration boundary,
  while retaining neutral compression and quote-density invariance.
- Status: completed as a research-only builder on local branch
  `codex/a3c-v7-dual-clock-bars`.
- Decisions: offline research only; inherit v6 completed-minute macro state and
  truthful OHLC/gap semantics; no intraminute evidence; calibration must select
  from a frozen budget x trend-duration family using independent M5 structure,
  not indicator output.
- Result: frozen confidence-floor directional CUSUM plus separate active-trend
  duration clock. Calibration-selected downstream configuration is evidence
  budget 2.0 and 60 completed minutes; full snapshot emits 2,326 completed bars,
  of which 2,248 (96.65%) close on purposeful evidence/trend boundaries.
- Files: `src/information_bars_a3c_v7.py`, focused tests and frozen protocol
  under `research/a3c_v7_dual_clock/`.
- Verification: focused v1-v7 regression suite passes 80/80; scoped Ruff,
  format, compile and diff checks pass; downstream independent validator proves
  exact OHLC/tick-volume partition for every tick assigned to completed bars;
  the final still-forming tail is intentionally not emitted.
- Next: preserve this offline implementation unchanged and validate the frozen
  bar-plus-C12 observer on a new untouched period or market before any product
  integration request.
- Blockers: none.
- Last Updated: 2026-08-24 18:12 MSK

- Status: completed negative prototype; do not expose or retune.
- Base: Connector A3C-v5 commit `b8db385`.
- Scope: offline builder, synthetic causal tests and research artifacts only.
- Frozen protocol: `research/a3c_v6_minute_drift/A3C_V6_PROTOCOL.md`.
- Primary frontier: T1000 descriptive trend tracking with fewer undesired TM2
  switches and preserved capture/onset.
- Not authorized: Connector route, Monitor UI, ATS, execution, database,
  production deployment or historical backfill.
- Data limit: only the outcome-exposed EURUSD snapshot is locally available.
- Verification: focused v1-v6 suite passes 70/70. Full study selects budget 2,
  emits 327 evidence closures, 592 duration guards and 46 gap resets; mechanism
  works but density remains far below M35.

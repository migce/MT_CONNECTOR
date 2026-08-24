# A3C-v6 Completed-Minute Drift Bars

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

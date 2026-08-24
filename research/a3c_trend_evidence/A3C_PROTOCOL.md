# A3C Causal Trend-Evidence Bars Protocol

Status: frozen before synthetic or real-data outcome calculation on 2026-08-24.

## Objective

Implement a strictly causal, indicator-independent bar clock that emits more
bars during sustained directional movement and fewer bars during inactive or
two-sided movement. A3C must improve the trend/noise allocation frontier rather
than merely sit between a fast and a slow conventional chart.

The implementation is offline research only. It is not wired into REST,
WebSocket, Monitor, ATS, execution, database or production services.

## Causal contract

- Only ticks already received may affect the current state or boundary.
- Completed bars are immutable. No retrospective split, merge or timestamp
  revision is permitted.
- Sampling intensity may change inside the forming bar.
- Raw selected-price OHLC and tick count remain truthful.
- Duplicate quotes update raw bar volume and duration but provide no directional
  evidence and cannot close a bar except through the hard safety guard.
- A gap over five minutes closes the prior segment at its last pre-gap tick,
  resets all evidence state and starts a fresh segment with the new tick.
- TM2, ACDC, MOMO or any other downstream indicator may not choose boundaries.

## Evidence clock

The selected price is `bid` for parity with the prior research.

For each distinct price change, A3C calculates a clipped standardized log
return using only the previous causal EWMA absolute-return scale. Positive and
negative one-sided CUSUM accumulators subtract an allowance on every event, so
alternating movement cancels instead of accumulating bar progress.

Directional quality combines:

- path efficiency, `abs(sum(return)) / sum(abs(return))`;
- standardized drift strength, `abs(sum(return)) / sqrt(sum(return^2))`;
- sign agreement across trailing physical-time windows of 1, 5 and 15 minutes.

The short window retains 70% weight so a new move can be recognized before the
old long window has fully turned. The remaining 30% rewards multi-scale
agreement.

The state machine is `neutral`, `uptrend` or `downtrend`:

- neutral enters a direction only when both its CUSUM and trend score cross the
  frozen entry thresholds;
- the trend state persists through a lower exit threshold and requires 16
  consecutive weak price events to return to neutral;
- sufficiently strong opposite evidence reverses the state directly;
- every state transition closes the current bar on the confirming tick;
- within a trend, movement along the state adds progress and counter-movement
  removes progress with a 1.5 penalty; the bar closes at the evidence budget.

Neutral bars close only at 45 physical minutes, 1,500 meaningful price changes,
or the 100,000 raw-tick safety guard. Trend bars additionally close at ten
minutes or 400 meaningful price changes. These are liveness bounds, not primary
trend triggers.

## Frozen common parameters

- physical windows: 60 / 300 / 900 seconds;
- minimum events per eligible window: 8;
- EWMA absolute-return scale span: 256 meaningful events;
- regime warmup: 256 meaningful events;
- CUSUM allowance: 0.15 standardized-return units;
- trend-score entry / exit: 0.60 / 0.30;
- weak-state exit confirmation: 16 meaningful events;
- standardized-return clip: 4.0;
- counter-movement penalty: 1.5;
- neutral maximum: 45 minutes or 1,500 meaningful events;
- trend maximum: 10 minutes or 400 meaningful events;
- raw-tick safety maximum: 100,000;
- gap reset: 300 seconds.

## Frozen calibration family

The first 45% of elapsed EURUSD snapshot time is calibration-only. Twelve
candidate combinations are evaluated:

- entry CUSUM: 6, 8 or 10;
- trend evidence budget: 12, 18, 24 or 32;
- reversal CUSUM is 75% of entry CUSUM.

The selected candidate must first keep its total completed-bar count within
25% of the exact M35 calibration count. Among eligible candidates, select the
highest trend/neutral bar-rate ratio, then the lower neutral bar rate, then the
shorter median trend-onset cut delay, then the larger evidence budget and entry
CUSUM. If no candidate passes density, select the smallest density error before
the same tie-breaks.

No indicator result or evaluation-period outcome is available to selection.

## Synthetic gates

1. Equal-duration, equal-event monotonic movement produces at least four times
   as many evidence/state bars as alternating movement after identical warmup.
2. A dense unchanged-price quote storm produces no more bars than a sparse
   unchanged-price stream covering the same sub-45-minute duration.
3. A sustained opposite move cuts an established trend within the frozen
   reversal-event envelope.
4. Completed bars are prefix/replay invariant and every completed OHLC lies on
   the exact assigned raw tick path.
5. A large time gap does not enter duration, evidence or OHLC of either adjacent
   segment.

## Frozen real-data evaluation

The remaining 55% is evaluated once on the same immutable EURUSD snapshot used
by the A1000 study. This is engineering evidence only because EURUSD and the
period are outcome-exposed from prior work.

Primary structural gates:

- total bar density within 25% of M35;
- trend/neutral bar-rate ratio at least 3.0;
- neutral raw quote intensity has no material positive relationship with bar
  rate after controlling for meaningful price changes: the partial correlation
  of log bar rate and log raw quote rate, residualizing both against log
  meaningful-price-event rate, must be at most 0.15. Rates are measured in UTC
  60-minute buckets containing at least 30 observed neutral M5 minutes; a 0.5
  bar and one-event pseudocount are applied before taking logs;
- median cut delay from independent M5 trend onset no worse than A1000.

Downstream frontier gates on one common observed M5 grid:

- TM2 alignment improves at least 3 percentage points over A1000 or trend
  capture improves at least 5 percentage points;
- neutral flip rate is at most 17 per 100 observed hours;
- onset success is at least 75%;
- unchanged bar geometry plus TM2/ACDC/MOMO does not worsen the causal raw-price
  walk-forward log-loss baseline.

Failure is reported rather than tuned away. Production visualization may be
considered only after causal/synthetic gates and a materially improved
structural frontier. Prospective EURUSD and untouched-instrument confirmation
remain mandatory for any general or operational claim.

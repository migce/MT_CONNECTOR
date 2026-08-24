# A3C-v4 Continuous Directional-Evidence Bars Protocol

Status: frozen before any A3C-v4 synthetic or real-data outcome calculation on
2026-08-24.

## Objective and v3 diagnosis

A3C-v4 must emit repeated truthful bars during coherent directional movement
and compress alternating or inactive movement without waiting for a binary
trend-admission state. A3C-v3 was technically causal but scientifically failed:
only two of 3,123 bars closed on its trend evidence budget, so its neutral
liveness guards still controlled the chart.

V4 is a materially different clock, not a retuning of v3 thresholds. It is
offline research only and is not wired into REST, WebSocket, Monitor, ATS,
execution, a database or production services.

The prior EURUSD diagnostic interval is fully outcome-exposed. Reusing it can
compare engineering behavior but cannot create a new holdout or generalization
claim. A future period or untouched market remains mandatory for scientific or
operational promotion.

## Causal contract

- Only ticks already received may affect state or a boundary.
- Completed bars are immutable; no retrospective split, merge or timestamp
  revision is allowed.
- Raw bid OHLC, tick count and exact start/end/availability times remain
  truthful.
- Duplicate selected-price quotes update raw volume and duration but never
  advance directional evidence.
- A gap over five minutes closes the prior segment on its last pre-gap tick,
  resets scale, physical windows and evidence, and starts the next segment on
  the first post-gap tick.
- TM2, ACDC, MOMO and every downstream indicator are forbidden from boundary
  construction and calibration selection.

## Continuous competing evidence clocks

For every distinct bid-price change, compute a clipped standardized log return
using only the causal EWMA absolute-return scale available before that return.
Update the scale only after standardization.

Trailing physical-time windows of 1, 5 and 15 minutes calculate signed path
efficiency:

`window signed efficiency = sum(log return) / sum(abs(log return))`.

Positive directional quality is the eligible-window weighted mean of the
positive part of signed efficiency. Negative quality is the weighted mean of
its negative part. Eligible weights are renormalized; a window requires eight
price events. The current event may enter this causal quality estimate.

Every forming bar owns positive and negative one-sided clocks. On a positive
standardized return:

- the positive clock gains return magnitude multiplied by
  `quality floor + quality gain * positive quality`, then pays the fixed
  allowance;
- the negative clock loses the counter-movement penalty times return magnitude
  plus the allowance.

The negative-return calculation is symmetric. Both clocks are floored at zero.
Thus coherent continuation accelerates one clock, while alternating movement
cancels both rather than creating two independent activity counters.

A bar closes immediately when either clock reaches the evidence budget. Scale
and physical quality persist across ordinary bar boundaries, while both
bar-local clocks reset to zero. Therefore a long coherent move can produce
many consecutive evidence bars without another regime-entry event.

To preserve a meaningful swing that reverses before reaching the full budget,
the bar also closes when:

- the former leading clock previously reached at least 45% of budget;
- the opposite clock becomes the leader; and
- the opposite clock reaches two standardized evidence units.

This swing rule is causal and closes on the confirming tick; it never relocates
the boundary to the historical extreme.

## Frozen common parameters

- selected price: bid;
- physical windows: 60 / 300 / 900 seconds;
- physical weights: 0.50 / 0.30 / 0.20;
- minimum eligible window events: 8;
- EWMA absolute-return scale span: 128 meaningful events;
- standardized-return clip: 4.0;
- scale floor: `1e-10`;
- quality floor: 0.25;
- quality gain: 1.75;
- counter-movement penalty: 1.50;
- swing arm fraction: 0.45 of evidence budget;
- opposite swing confirmation: 2.0 evidence units;
- liveness maximum: 90 minutes or 3,000 meaningful price events;
- raw-tick safety maximum: 100,000;
- gap reset: 300 seconds.

There is no regime warmup, binary trend state, trend-duration guard or separate
post-admission progress clock.

## Frozen calibration family

The first 45% of elapsed time in the immutable EURUSD snapshot remains
calibration-only. Twelve combinations are evaluated:

- evidence allowance: 0.10, 0.20 or 0.35 standardized units;
- evidence budget: 8, 12, 18 or 26 units.

The candidate must first keep completed-bar count within 25% of exact M35 on
calibration. Among eligible candidates select the highest independent-M5
trend/neutral bar-rate ratio, then lower neutral bar rate, then shorter median
trend-onset cut delay, then larger evidence budget and larger allowance. If no
candidate is density-eligible, first minimize absolute density error and apply
the same tie-breaks.

No indicator output, v3 result or post-split v4 outcome may enter selection.

## Frozen synthetic gates

1. Equal-duration/equal-event monotonic movement produces at least four
   completed evidence bars and at least four times as many completed bars as
   alternating movement after identical scale preparation.
2. A dense unchanged-price quote storm produces no more bars than a sparse
   unchanged-price stream covering the same sub-90-minute duration.
3. A partially developed coherent swing followed by sustained opposite
   movement closes on the causal reversal-confirming tick before the opposite
   clock can traverse a full budget from zero.
4. Repeated continuation after an ordinary evidence boundary produces another
   evidence boundary without scale/window warmup or trend re-entry.
5. Completed bars are prefix/replay invariant, `availability_time=end_time`,
   and every OHLC/tick count is the exact assigned raw path.
6. A gap does not enter evidence, duration or OHLC of either adjacent segment.

## Frozen engineering diagnostic gates

Use the same immutable snapshot, independent raw-price M5 trend definition and
common observed M5 grid as v3. Compare A3C-v4 with A3C-v3, A1000, T1000 and M35.

Structural gates:

- evaluation bar density within 25% of M35;
- trend/neutral bar-rate ratio at least 3.0;
- directional-evidence plus swing-reversal closures at least 20% of all A3C-v4
  completed bars;
- neutral raw quote partial correlation at most 0.15 after controlling for
  meaningful-price-event rate, using the v3 frozen UTC-hour definition;
- median independent-M5 trend-onset cut delay no worse than A1000.

Downstream gates with unchanged TM2/ACDC/MOMO:

- TM2 alignment improves at least 3 percentage points over A1000 or trend
  capture improves at least 5 percentage points;
- neutral flip rate is at most 17 per 100 observed hours;
- onset success is at least 75%;
- chart geometry plus unchanged indicators does not worsen the causal raw-price
  walk-forward log-loss baseline.

Failure is reported, never tuned away. Passing these exposed diagnostics would
justify only a visually inspectable research series, not ATS, execution,
strategy eligibility or a predictive claim.

# A3C-v6 Completed-Minute Drift Bars Protocol

Status: refrozen before any real-data outcome calculation on 2026-08-24 after
the first synthetic quote-density gate exposed an internal inconsistency in the
intraminute version. No market outcome had been calculated.

## Objective and v5 diagnosis

A3C-v6 tests whether a clock derived entirely from completed physical-minute
endpoints can preserve the descriptive trend-tracking strength of fixed T1000
while reducing its useless TM2 switching. Intraminute ticks preserve truthful
OHLC and volume but may not advance evidence; raw tick count and raw-tick path
efficiency may not define the state or boundary.

V5 failed mechanically: tick-by-tick signed path efficiency was nearly zero
under bid bounce, no candidate reached an evidence boundary, and the output
collapsed into 180-minute liveness bars. V6 replaces that macro estimator; it
does not retune v5 budgets.

V6 remains offline research only. It is not wired into REST, WebSocket,
Monitor, ATS, execution, a database or production services.

The available EURUSD interval is outcome-exposed by earlier work. It may
diagnose engineering behavior but cannot establish generalization. A future
interval or untouched market remains mandatory before promotion.

## Causal contract

- Only received ticks may affect state or a boundary.
- Completed bars are immutable; no retrospective split, merge or timestamp
  revision is allowed.
- Raw bid OHLC, tick count and exact start/end/availability times remain
  truthful.
- Duplicate bid quotes update raw volume and duration but never advance
  evidence.
- A current physical minute is never part of its own macro direction,
  volatility scale or evidence. Its last received bid becomes an endpoint only
  after a tick from a later minute arrives.
- A gap over five minutes closes the prior segment on its last pre-gap tick and
  resets minute endpoints, returns, scale, direction and progress.
- TM2, ACDC, MOMO, future returns and every downstream indicator are forbidden
  from boundary construction and candidate selection.

## Completed-minute macro state

The builder stores the last received bid of the current UTC minute. When a tick
from a later minute arrives, the prior minute endpoint becomes complete. From
the second completed endpoint onward, append its log return to a causal deque.

For each eligible horizon `n` in 5, 15 and 60 completed minute returns, define:

`drift_n = sum(r) / sqrt(n * sum(r^2))`.

This signed endpoint displacement divided by minute RMS is bounded to [-1, 1].
It is insensitive to how many quotes or bid bounces occurred inside each
minute. Horizons have weights 0.20 / 0.30 / 0.50. Only fully populated
horizons are eligible and their weights are renormalized. The weighted value is
the macro-drift score. The past-minute volatility scale is
`sqrt(mean(r^2))` over up to 60 completed minute returns and requires at least
five returns.

The minute just finalized updates evidence against the macro direction and
scale that existed before its return. Only after evidence is updated does that
return enter the macro windows and scale for later minutes. The endpoint was
already fixed by a previously received tick; no future value enters.

## Completed-minute displacement clock

Every newly completed one-minute endpoint return is divided by the past
completed-minute RMS, clipped to [-2, 2], and evaluated against the sign and
confidence of the macro score that existed before that return.

The forming bar owns one non-negative progress value and one evidence
direction. If the macro sign changes, unfinished progress is reset to zero and
the new sign becomes the evidence direction. With stable sign:

`delta = macro_direction * clipped_minute_return / minute_rms * abs(macro_score)`

and:

`progress = max(0, progress + delta)`.

Aligned minute-endpoint displacement advances progress; retracement cancels it.
Intraminute bid bounce cannot enter progress at all. Confidence attenuates
uncertain cross-horizon states continuously; there is no separate binary
admission threshold.

A bar closes on the first tick of a later minute when the newly available prior
minute return makes progress reach the evidence budget. That current tick is
included in the closing bar, so `availability_time=end_time` remains exact.
Minute state and scale persist across ordinary boundaries so a sustained trend
can emit repeated bars. Only bar-local progress and evidence direction reset.

There is no swing-reversal or meaningful-event-count boundary. A neutral
liveness boundary closes a bar after 120 observed minutes. A 100,000 raw-tick
cap is a safety guard only.

## Frozen common parameters

- selected price: bid;
- completed-minute horizons: 5 / 15 / 60 returns;
- horizon weights: 0.20 / 0.30 / 0.50;
- scale lookback: up to 60 completed minute returns;
- scale eligibility: at least 5 completed minute returns;
- scale floor: `1e-10`;
- normalized completed-minute-return clip: 2.0;
- neutral liveness maximum: 120 minutes;
- raw-tick safety maximum: 100,000;
- gap reset: 300 seconds.

## Frozen calibration family

The first 45% of elapsed time in the immutable EURUSD snapshot is
calibration-only. Four evidence budgets are evaluated: 2, 4, 8 and 16. All
other parameters remain fixed.

The candidate must first keep completed-bar count within 25% of exact M35 on
calibration. Among eligible candidates select the highest independent-M5
trend/neutral bar-rate ratio, then lower neutral bar rate, shorter median
trend-onset cut delay and larger evidence budget. If no candidate is density
eligible, first minimize absolute density error and apply the same tie-breaks.

No indicator output, v3/v4/v5 result or post-split v6 outcome may enter
selection.

## Frozen synthetic gates

1. Identical minute endpoints produce identical macro state and evidence-bar
   count despite sparse quotes versus dense alternating intraminute bid bounce.
2. After identical completed-minute preparation, monotonic continuation emits
   at least four evidence bars and at least four times as many bars as an
   equal-duration path with unchanged minute endpoints.
3. An extreme tick inside the current minute cannot alter macro drift,
   past-minute RMS or progress until that minute is later completed.
4. Retracement reduces progress; a macro-sign change resets unfinished
   progress without directly emitting a boundary.
5. Repeated continuation after an evidence boundary produces another evidence
   boundary without minute-state warmup or trend re-entry.
6. Completed bars are prefix/replay invariant, `availability_time=end_time`,
   and every OHLC/tick count is the exact assigned raw path.
7. A gap does not enter minute returns, evidence, duration or OHLC of either
   adjacent segment.

## Frozen evaluation definitions

All indicator comparisons use unchanged library implementations sampled on a
common observed M5 grid. The independent price state remains the frozen raw-M5
60-minute signed-return / realized-energy state with thresholds +1 and -1.

TM2 direction is exactly `sign(fast_ema - slow_ema)`. A TM2 switch is a change
between +1 and -1 after warmup.

A **neutral switch** is initiated while the independent price state is zero.
A **trend interruption** is a switch opposite to the current non-neutral
independent trend followed by a return to that trend direction before the
independent trend episode ends and within six observed M5 steps. The primary
undesired-switch count is the union of neutral-switch and trend-interruption
initiators. Report its rate per 100 observed hours.

Also report all TM2 switches, 30-minute whipsaws, alignment, return-weighted
trend capture, false direction, trend-onset success and capped 120-minute onset
delay.

ACDC has no canonical TM2-like directional state. Report only switches of its
published `extreme_level_color_state`, labelled ACDC level-color switches.
MOMO has no published discrete trend state. Report only changes in the sign of
its published displacement `momentum`, labelled MOMO displacement-sign
switches. Neither enters the primary trend-switch gate.

## Frozen engineering gates

Compare A3C-v6 with A3C-v5, A3C-v4, A1000, fixed T1000 and exact M35. T1000 is
the primary frontier because it has the best exposed descriptive trend capture,
alignment and onset success among the previously tested clocks.

Structural gates:

- evaluation bar density within 25% of M35;
- trend/neutral bar-rate ratio at least 1.50;
- completed-minute drift evidence closures at least 40% of v6 bars;
- neutral raw-quote partial correlation at most 0.15 after controlling for
  meaningful-price-event rate;
- median independent-M5 trend-onset cut delay no more than 10 minutes worse
  than T1000.

Primary unchanged-TM2 gates:

- undesired-switch rate at least 25% below T1000;
- total TM2 switch rate no higher than T1000;
- trend capture no more than 3 percentage points below T1000;
- alignment no more than 3 percentage points below T1000;
- onset success no more than 5 percentage points below T1000;
- median capped onset delay no more than 10 minutes worse than T1000.

The identical causal raw-price walk-forward comparison is reported as a guard
and uncertainty diagnostic, not a promotion gate, because this task concerns
descriptive trend tracking rather than price forecasting.

Failure is reported and not retuned. Passing on the exposed EURUSD interval
would justify obtaining independent ticks and a one-shot validation; it would
not justify Monitor exposure, ATS, execution or predictive claims.

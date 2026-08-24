# A3C-v5 Macro-Drift Clock Bars Protocol

Status: frozen before any A3C-v5 synthetic or real-data outcome calculation on
2026-08-24.

## Objective and v4 diagnosis

A3C-v5 tests whether a slow physical-time drift clock can emit repeated bars
inside sustained movement while absorbing counter-movement and low-information
activity. The downstream objective is fewer nuisance switches from unchanged
TM2 without losing trend capture or materially delaying trend recognition.

V4 proved that continuous competing evidence can be causal, but its 1/5/15
minute quality windows, per-event allowance and swing-reversal boundary left
most bars controlled by liveness guards. V5 is a different clock, not a
threshold retune of v4.

V5 remains offline research only. It is not wired into REST, WebSocket,
Monitor, ATS, execution, a database or production services.

The available EURUSD interval is fully outcome-exposed by earlier A1000, v3
and v4 work. It may diagnose engineering behavior but cannot establish
generalization. Promotion requires a later one-shot test on a future interval
or a market not used to design the bar clock.

## Causal contract

- Only ticks already received may affect state or a boundary.
- Completed bars are immutable; no retrospective split, merge or timestamp
  revision is allowed.
- Raw bid OHLC, tick count and exact start/end/availability times remain
  truthful.
- Duplicate selected-price quotes update raw volume and duration but never
  advance drift evidence.
- A gap over five minutes closes the prior segment on its last pre-gap tick,
  resets scale, drift windows and bar progress, and starts the next segment on
  the first post-gap tick.
- TM2, ACDC, MOMO, future returns and every downstream indicator are forbidden
  from boundary construction and calibration selection.

## Prior macro-drift clock

For every distinct bid-price change, compute a clipped standardized log return
using the causal EWMA absolute-return scale available before that return. The
scale is updated only after standardization.

Trailing physical-time windows of 5, 15 and 60 minutes maintain:

`signed path efficiency = sum(log return) / sum(abs(log return))`.

The windows have weights 0.20 / 0.30 / 0.50 and require 8 / 16 / 32 price
events respectively. Eligible weights are renormalized. Their weighted signed
efficiency is the macro-drift score in [-1, 1].

Crucially, the current standardized return is scored against the macro-drift
value known immediately before that return. The current return enters the
windows only after bar-progress calculation. This prevents the event from
manufacturing its own confirming state.

Each forming bar has one non-negative progress clock. Let `z` be the current
standardized return and `d` the prior macro-drift score. Define alignment
`a = z * d`.

- If `a > 0`, progress increases by
  `a * (0.25 + 1.75 * abs(d))`.
- If `a < 0`, progress decreases by `1.50 * abs(a)`.
- Progress is floored at zero.

Thus sustained movement in the established macro direction repeatedly fills
the clock. Counter-movement cancels partially accumulated evidence instead of
closing a reversal bar. Cross-horizon disagreement makes `d` small and slows
the clock continuously; there is no binary trend-admission state.

A bar closes when progress reaches its evidence budget. The EWMA scale and
physical drift windows persist across ordinary boundaries so a long trend can
produce consecutive bars without re-entry or warmup. Only the bar-local clock
resets.

There is deliberately no swing-reversal closure and no meaningful-price-event
guard. A neutral liveness boundary closes a bar after 180 observed minutes.
A 100,000 raw-tick cap is a safety guard only.

## Frozen common parameters

- selected price: bid;
- physical windows: 300 / 900 / 3,600 seconds;
- physical weights: 0.20 / 0.30 / 0.50;
- minimum window events: 8 / 16 / 32;
- EWMA absolute-return scale span: 128 meaningful events;
- standardized-return clip: 4.0;
- scale floor: `1e-10`;
- aligned quality floor: 0.25;
- aligned quality gain: 1.75;
- counter-movement penalty: 1.50;
- neutral liveness maximum: 180 minutes;
- raw-tick safety maximum: 100,000;
- gap reset: 300 seconds.

## Frozen calibration family

The first 45% of elapsed time in the immutable EURUSD snapshot is
calibration-only. Four evidence budgets are evaluated: 24, 48, 96 and 192.
All other parameters remain fixed.

The candidate must first keep completed-bar count within 25% of exact M35 on
calibration. Among eligible candidates select the highest independent-M5
trend/neutral bar-rate ratio, then the lower neutral bar rate, shorter median
trend-onset cut delay and larger evidence budget. If no candidate is density
eligible, first minimize absolute density error and apply the same tie-breaks.

No indicator output, v3/v4 result or post-split v5 outcome may enter selection.

## Frozen synthetic gates

1. After identical scale and 60-minute window preparation, monotonic movement
   produces at least four evidence bars and at least four times as many bars as
   alternating movement of equal duration and price-event count.
2. A dense unchanged-price quote storm produces no more bars than a sparse
   unchanged-price stream covering the same sub-180-minute duration.
3. Counter-movement reduces already accumulated progress and cannot directly
   create a boundary.
4. Repeated continuation after an evidence boundary produces another evidence
   boundary without scale/window warmup or trend re-entry.
5. Completed bars are prefix/replay invariant, `availability_time=end_time`,
   and every OHLC/tick count is the exact assigned raw path.
6. A gap does not enter evidence, duration or OHLC of either adjacent segment.

## Frozen evaluation definitions

All indicator comparisons use unchanged library implementations sampled on a
common observed M5 grid. The independent price state remains the frozen raw-M5
60-minute signed-return / realized-energy state with thresholds +1 and -1.

TM2 direction is exactly `sign(fast_ema - slow_ema)`. A TM2 switch is a change
between +1 and -1 after warmup.

A **30-minute TM2 whipsaw** is a switch followed by a return to its previous
TM2 direction within six observed M5 steps while the independent price state
never adopts the intermediate TM2 direction. Count the initiating switch once.

A **trend interruption** is a TM2 switch opposite to the current non-neutral
independent trend followed by a return to that trend direction before the
independent trend episode ends and within 30 minutes. Count the initiating
switch once.

The primary nuisance-switch rate is unique whipsaw-or-interruption initiating
switches per 100 observed hours. Also report total TM2 switches, neutral TM2
flips, alignment, return-weighted trend capture, false direction, trend-onset
success and capped 120-minute onset delay.

ACDC has no canonical TM2-like directional state. Report only switches of its
published `extreme_level_color_state`, labelled ACDC level-color switches.
MOMO has no published discrete trend state. Report only changes in the sign of
its published displacement `momentum`, labelled MOMO displacement-sign
switches. Neither is used as a primary trend-switch gate.

## Frozen engineering gates

Compare A3C-v5 with A3C-v4, A1000, T1000 and exact M35.

Structural gates:

- evaluation bar density within 25% of M35;
- trend/neutral bar-rate ratio at least 3.0;
- macro-drift evidence closures at least 40% of completed v5 bars;
- neutral raw-quote partial correlation at most 0.15 after controlling for
  meaningful-price-event rate;
- median independent-M5 trend-onset cut delay no worse than A1000.

Primary unchanged-indicator gates:

- TM2 nuisance-switch rate at least 20% below A1000;
- TM2 nuisance-switch rate below A3C-v4;
- TM2 trend capture no more than 3 percentage points below A1000;
- TM2 alignment no more than 3 percentage points below A1000;
- TM2 onset success no more than 5 percentage points below A1000;
- median capped TM2 onset delay no more than 10 minutes worse than A1000.

Guard only:

- chart geometry plus unchanged TM2/ACDC/MOMO does not worsen the identical
  causal raw-price walk-forward log-loss baseline.

Failure is reported and not retuned. Passing on the exposed EURUSD interval
would justify obtaining independent ticks and a later one-shot validation; it
would not justify Monitor exposure, ATS, execution or predictive claims.

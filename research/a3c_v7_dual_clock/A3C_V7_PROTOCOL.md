# A3C-v7 Completed-Minute Dual Clock Protocol

Status: frozen before any A3C-v7 real-data bar construction or market-outcome
calculation on 2026-08-24.

## Objective

A3C-v6 remained structurally over-compressed: its smallest frozen evidence
budget still produced far fewer bars than M35, most boundaries were the
120-minute liveness guard, and faster TM2 settings could not recover T1000
capture/onset while preserving the cleaning gate.

A3C-v7 tests one specific remedy: retain v6's quote-density-invariant
completed-minute macro state, but combine a more responsive directional CUSUM
with a separate causal trend-duration boundary. Sustained trend should emit
multiple truthful bars; neutral movement should remain compressed.

V7 is offline research only. It is not wired into REST, WebSocket, Monitor,
ATS, execution, database, backfill or production.

## Causal and data contract

- Use received bid ticks only. OHLC, tick count, start/end/availability time and
  completed-bar partition remain exact.
- A forming physical minute cannot affect its own macro state or boundary.
- Only a tick from a later minute finalizes the prior minute endpoint.
- Intraminute price path and raw quote count update truthful bar fields but
  never advance evidence or trend state.
- A gap over five minutes closes the prior segment at its last pre-gap tick and
  resets endpoint/macro/progress state. The gap jump enters neither bar.
- Completed bars are immutable and prefix/replay invariant.
- Indicators and future outcomes are forbidden from construction and v7 clock
  calibration.

## Completed-minute macro state

Reuse v6 exactly:

- endpoint-return horizons 5 / 15 / 60;
- horizon weights 0.20 / 0.30 / 0.50;
- `drift_n = sum(r) / sqrt(n * sum(r^2))`, clipped to [-1, 1];
- renormalize weights across available full horizons;
- prior-minute RMS over up to 60 completed returns, at least five required;
- normalized completed-minute return clipped to [-2, 2].

The finalized minute is scored against macro direction and RMS that existed
before that return. Only afterward does it enter the macro windows.

## Confidence-floor directional CUSUM

Let `m` be the prior macro score, `z` the finalized minute return divided by
the prior RMS and clipped to [-2, 2], and `d = sign(m)`.

`confidence = 0.35 + 0.65 * abs(m)`

`aligned = d * z`

`delta = confidence * (max(aligned, 0) - 1.25 * max(-aligned, 0))`

`progress = max(0, progress + delta)`

The confidence floor prevents a directionally coherent but cross-horizon-muted
trend from stalling completely. Counter-direction returns cancel progress 25%
faster than aligned returns accumulate. A macro sign change resets unfinished
bar progress and adopts the new direction without emitting a boundary.

A bar closes with `completed_minute_dual_evidence` when progress reaches its
candidate evidence budget.

## Causal trend-duration boundary

After the newly finalized return has updated the macro window, a trend is
active only when all are true:

- at least five completed-minute returns exist;
- `abs(current_macro_drift) >= 0.25`;
- evidence direction equals the current macro sign;
- bar-local progress is positive.

If active and the current bar reaches the candidate trend-duration maximum, it
closes with `completed_minute_trend_duration`. This boundary uses only finalized
minute state available on the closing tick. It is not a time-bar boundary in
neutral conditions.

Neutral/liveness maximum remains 120 minutes with reason
`neutral_duration_guard`. Raw-tick safety remains 100,000.

## Frozen common parameters

- confidence floor: 0.35;
- counter-return penalty: 1.25;
- active-trend macro threshold: 0.25;
- active-trend minimum completed returns: 5;
- neutral maximum duration: 120 minutes;
- raw-tick maximum: 100,000;
- gap reset: 300 seconds;
- all v6 macro/RMS parameters unchanged.

## Frozen calibration family

Cross the following values on the first 45% elapsed EURUSD calibration period:

- evidence budget: 0.75 / 1.00 / 1.50 / 2.00;
- active-trend maximum duration: 30 / 45 / 60 minutes.

There are 12 candidates. No additional value may be introduced after a real
v7 outcome is calculated.

Build exact M35 from the same ticks. A candidate is density eligible when its
completed calibration-bar count is within 25% of M35. Among eligible candidates
select lexicographically:

1. highest independent-M5 trend/neutral bar-rate ratio;
2. shortest median independent trend-onset cut delay;
3. lowest neutral bar rate;
4. smallest absolute density error;
5. larger evidence budget;
6. longer trend-duration maximum.

If none is density eligible, first minimize density error and then apply the
same remaining order. No TM2, F7, ACDC, MOMO or evaluation-period value may
enter v7 clock selection.

## Frozen synthetic gates

1. Identical completed-minute endpoints yield identical boundary minute buckets
   and reasons under sparse quotes and dense alternating intraminute bounce.
   Exact `end_time` remains the truthful first received tick on which the
   completed-minute boundary becomes available and may differ within that
   minute across quote streams.
2. Sustained monotonic continuation emits at least four purposeful v7 bars and
   at least four times as many bars as equal-duration flat endpoints.
3. With an unreachable evidence budget, an active trend still closes on the
   configured trend-duration boundary while neutral endpoints do not.
4. Counter returns reduce progress and a macro-sign change resets it.
5. Current-minute extremes cannot alter macro/RMS/progress until finalized.
6. Repeated continuation emits repeated boundaries without macro re-warmup.
7. Prefix/replay invariance, exact OHLC/tick partition and
   `availability_time=end_time` hold.
8. Gap reset excludes the jump and restarts completed-minute state.

## Evaluation boundary

The later cross-method benchmark may compare v7 with A1000 v2 and A3C-v3/v4/v5/v6
using Standard TM2 and calibration-selected fast TM2 observers. That benchmark
cannot change v7 construction or candidate selection.

The EURUSD interval is outcome-exposed by prior work. Passing engineering gates
would support only a future untouched symbol or immutable future snapshot.

# Adaptive information bars v2 experiment

## Status

This is a research-only chart model. Connector exposes it as `A<n>` through
REST and WebSocket, while every trading/execution path remains excluded.

## Causal clock

`A<n>` means a neutral target of approximately `n` ticks. Before the first tick
of each new bar, the builder freezes a target derived only from previously
observed ticks:

```text
score = arrival_ratio^0.40
      * activity_ratio^0.55
      * efficiency_ratio^0.65

target_ticks = clip(round(n / score), 0.25n, 4.00n)
```

- `arrival_ratio` compares fast and slow EWMA log interarrival time.
- `activity_ratio` compares fast and slow EWMA absolute log return.
- `efficiency_ratio` compares 256-tick directional efficiency with the frozen
  neutral level `0.08`.
- Each component is clipped to `[0.25, 4.0]`.
- The first 4,096 ticks are neutral warmup bars.
- Interarrival observations above 300 seconds are treated as gaps and do not
  pollute the next regime estimate.

The target never changes while its bar is forming. Completed bars therefore
have `tick_volume == target_tick_count`.

## Frozen train/validation experiment

The research runner read 600,000 existing ticks each for EURUSD, USDJPY, and
XAUUSD without backfill. The first chronological half selected one of nine
parameter candidates; the second half was untouched validation. The moderate
configuration above won. No candidate limit was hit on the validation half.

| Validation | v1 volume median / CV | v2 p05 / median / p95 | v2 CV | Slow-bar duration / fast-bar duration |
| --- | ---: | ---: | ---: | ---: |
| EURUSD | 3,087 / 3.4% | 735 / 1,478 / 2,453 | 36.6% | 3.40x |
| USDJPY | 2,887 / 4.6% | 626 / 1,183 / 2,486 | 42.0% | 3.25x |
| XAUUSD | 2,833 / 5.1% | 633 / 1,290 / 2,641 | 42.9% | 3.22x |

For the lowest-volume versus highest-volume quartiles, realized directional
efficiency was higher in all three validation symbols. Price path per second
was also higher by 1.57x on EURUSD, 1.63x on USDJPY, and 1.09x on XAUUSD. This
supports the intended expansion of faster, more directional flow and
compression of slower flow, while showing that XAUUSD price-path separation is
weaker and needs visual acceptance.

## Reproduction

`scripts/research_information_bars_v2.py` prints aggregate JSON and performs no
write or backfill. Run it in an environment with the Connector repository and
database settings available.

## Remaining gates

- visual comparison in Monitor against `T1000` and deployed `I1000` v1;
- bar-to-bar target smoothness review on difficult sessions;
- matched TM2, ACDC, and MOMO behavior evaluation;
- a stable persisted anchor/revision contract before any live integration.

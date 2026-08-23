"""One-pass matched-window experiment for offline information-bars v2.

The script reads bounded existing ticks through the Connector repository and
prints aggregate JSON only.  It does not backfill, publish, or mutate data.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import json
import math
import os
import statistics
from datetime import datetime
from typing import Any

from src.db.repository import query_information_bar_ticks
from src.information_bars import InformationBarConfig, build_information_bars

with contextlib.suppress(ModuleNotFoundError):
    from src.information_bars_v2 import InformationBarV2Config, build_information_bars_v2

# The runner may be streamed after the prototype module into an otherwise
# unchanged production image.  In that mode the definitions are already
# present in ``__main__`` even though the import above is suppressed.


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) < 3 or len(left) != len(right):
        return None
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    left_ss = sum((x - left_mean) ** 2 for x in left)
    right_ss = sum((y - right_mean) ** 2 for y in right)
    if left_ss <= 0 or right_ss <= 0:
        return None
    return numerator / math.sqrt(left_ss * right_ss)


def _fixed_tick_bars(ticks: list[dict[str, Any]], tick_count: int) -> list[dict[str, Any]]:
    bars: list[dict[str, Any]] = []
    for start in range(0, len(ticks) - tick_count + 1, tick_count):
        chunk = ticks[start : start + tick_count]
        prices = [float(tick.get("bid") or 0) for tick in chunk]
        if not prices or min(prices) <= 0:
            continue
        start_time = chunk[0]["time_msc"]
        end_time = chunk[-1]["time_msc"]
        bars.append(
            {
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "tick_volume": tick_count,
                "duration_ms": max(0, round((end_time - start_time).total_seconds() * 1_000)),
                "is_complete": True,
            }
        )
    return bars


def _summary(
    bars: list[dict[str, Any]],
    *,
    minimum_target: int | None = None,
    maximum_target: int | None = None,
) -> dict[str, Any]:
    complete = [bar for bar in bars if bar.get("is_complete")]
    warmed = [bar for bar in complete if bar.get("regime_warmed", True)]
    selected = warmed or complete
    volumes = [float(bar["tick_volume"]) for bar in selected]
    durations = [float(bar.get("duration_ms") or 0) / 1_000 for bar in selected]
    ranges = [
        (float(bar["high"]) - float(bar["low"])) / float(bar["open"]) * 1_000_000
        for bar in selected
        if float(bar["open"]) > 0
    ]
    paths = [float(bar.get("path_log_return") or 0) * 1_000_000 for bar in selected]
    path_per_tick = [path / volume for path, volume in zip(paths, volumes, strict=True)]
    path_per_second = [
        path / duration if duration > 0 else 0.0
        for path, duration in zip(paths, durations, strict=True)
    ]
    realized_efficiencies = [
        float(bar.get("realized_directional_efficiency") or 0) for bar in selected
    ]
    if not volumes:
        return {"bars": 0}
    mean_volume = statistics.fmean(volumes)
    cv = statistics.pstdev(volumes) / mean_volume if mean_volume else 0.0
    order = sorted(range(len(volumes)), key=volumes.__getitem__)
    quartile = max(1, len(order) // 4)
    low_indices = order[:quartile]
    high_indices = order[-quartile:]
    low_range = statistics.fmean(ranges[index] for index in low_indices)
    high_range = statistics.fmean(ranges[index] for index in high_indices)
    low_duration = statistics.fmean(durations[index] for index in low_indices)
    high_duration = statistics.fmean(durations[index] for index in high_indices)
    low_path_per_tick = statistics.fmean(path_per_tick[index] for index in low_indices)
    high_path_per_tick = statistics.fmean(path_per_tick[index] for index in high_indices)
    low_path_per_second = statistics.fmean(path_per_second[index] for index in low_indices)
    high_path_per_second = statistics.fmean(path_per_second[index] for index in high_indices)
    low_efficiency = statistics.fmean(realized_efficiencies[index] for index in low_indices)
    high_efficiency = statistics.fmean(realized_efficiencies[index] for index in high_indices)
    result: dict[str, Any] = {
        "bars": len(selected),
        "warmup_bars_excluded": len(complete) - len(selected),
        "volume": {
            "min": min(volumes),
            "p05": _percentile(volumes, 0.05),
            "median": _percentile(volumes, 0.50),
            "p95": _percentile(volumes, 0.95),
            "max": max(volumes),
            "mean": mean_volume,
            "cv": cv,
        },
        "duration_seconds": {
            "median": _percentile(durations, 0.50),
            "p95": _percentile(durations, 0.95),
        },
        "range_ppm": {
            "median": _percentile(ranges, 0.50),
            "p95": _percentile(ranges, 0.95),
        },
        "correlation": {
            "volume_range": _correlation(volumes, ranges),
            "volume_duration": _correlation(volumes, durations),
            "volume_path_per_tick": _correlation(volumes, path_per_tick),
            "volume_path_per_second": _correlation(volumes, path_per_second),
            "volume_realized_efficiency": _correlation(volumes, realized_efficiencies),
        },
        "quartiles": {
            "low_volume_mean_range_ppm": low_range,
            "high_volume_mean_range_ppm": high_range,
            "range_ratio_low_over_high": low_range / high_range if high_range else None,
            "low_volume_mean_duration_seconds": low_duration,
            "high_volume_mean_duration_seconds": high_duration,
            "duration_ratio_high_over_low": high_duration / low_duration if low_duration else None,
            "low_volume_path_per_tick_ppm": low_path_per_tick,
            "high_volume_path_per_tick_ppm": high_path_per_tick,
            "path_per_tick_ratio_low_over_high": (
                low_path_per_tick / high_path_per_tick if high_path_per_tick else None
            ),
            "low_volume_path_per_second_ppm": low_path_per_second,
            "high_volume_path_per_second_ppm": high_path_per_second,
            "path_per_second_ratio_low_over_high": (
                low_path_per_second / high_path_per_second if high_path_per_second else None
            ),
            "low_volume_realized_efficiency": low_efficiency,
            "high_volume_realized_efficiency": high_efficiency,
        },
    }
    if minimum_target is not None and maximum_target is not None:
        result["bounds"] = {
            "minimum_fraction": sum(value <= minimum_target for value in volumes) / len(volumes),
            "maximum_fraction": sum(value >= maximum_target for value in volumes) / len(volumes),
        }
    return result


def _candidate_configs(neutral_ticks: int) -> list[tuple[str, Any]]:
    candidates = [
        ("moderate-e08", 0.40, 0.55, 0.65, 0.08),
        ("moderate-e10", 0.40, 0.55, 0.65, 0.10),
        ("moderate-e12", 0.40, 0.55, 0.65, 0.12),
        ("balanced-e08", 0.55, 0.75, 0.85, 0.08),
        ("balanced-e10", 0.55, 0.75, 0.85, 0.10),
        ("balanced-e12", 0.55, 0.75, 0.85, 0.12),
        ("strong-e08", 0.70, 0.95, 1.00, 0.08),
        ("strong-e10", 0.70, 0.95, 1.00, 0.10),
        ("strong-e12", 0.70, 0.95, 1.00, 0.12),
    ]
    return [
        (
            name,
            InformationBarV2Config(
                neutral_ticks=neutral_ticks,
                arrival_exponent=arrival,
                activity_exponent=activity,
                efficiency_exponent=efficiency,
                neutral_efficiency=neutral_efficiency,
            ),
        )
        for name, arrival, activity, efficiency, neutral_efficiency in candidates
    ]


def _selection_penalty(summaries: list[dict[str, Any]], neutral_ticks: int) -> float:
    penalties: list[float] = []
    for summary in summaries:
        volume = summary["volume"]
        bounds = summary["bounds"]
        quartiles = summary["quartiles"]
        median_ratio = float(volume["median"]) / neutral_ticks
        cv = float(volume["cv"])
        saturation = float(bounds["minimum_fraction"]) + float(bounds["maximum_fraction"])
        path_ratio = max(0.2, float(quartiles["path_per_tick_ratio_low_over_high"] or 0.2))
        penalty = (
            2.5 * abs(math.log(max(0.1, median_ratio)))
            + 1.5 * abs(cv - 0.45)
            + 2.0 * saturation
            - 0.50 * math.log(path_ratio)
        )
        penalties.append(penalty)
    return statistics.fmean(penalties)


async def main() -> None:
    symbols = [value.strip().upper() for value in os.getenv("INFO_V2_SYMBOLS", "EURUSD,USDJPY,XAUUSD").split(",") if value.strip()]
    source_limit = int(os.getenv("INFO_V2_SOURCE_LIMIT", "600000"))
    neutral_ticks = int(os.getenv("INFO_V2_NEUTRAL_TICKS", "1000"))
    candidates = _candidate_configs(neutral_ticks)
    train_results: dict[str, list[dict[str, Any]]] = {name: [] for name, _config in candidates}
    validation_ticks: dict[str, list[dict[str, Any]]] = {}
    baselines: dict[str, Any] = {}

    for symbol in symbols:
        ticks = await query_information_bar_ticks(symbol, source_limit=source_limit)
        split = len(ticks) // 2
        train = ticks[:split]
        validation = ticks[split:]
        validation_ticks[symbol] = validation
        baselines[symbol] = {
            "source_ticks": len(ticks),
            "first_time": ticks[0]["time_msc"].isoformat() if ticks else None,
            "split_time": validation[0]["time_msc"].isoformat() if validation else None,
            "last_time": ticks[-1]["time_msc"].isoformat() if ticks else None,
            "train": {
                "T1000": _summary(_fixed_tick_bars(train, neutral_ticks)),
                "I1000_v1": _summary(
                    build_information_bars(train, InformationBarConfig(budget=neutral_ticks))
                ),
            },
        }
        for name, config in candidates:
            summary = _summary(
                build_information_bars_v2(train, config),
                minimum_target=config.min_target_ticks,
                maximum_target=config.max_target_ticks,
            )
            train_results[name].append(summary)
        del ticks, train
        gc.collect()

    ranking = sorted(
        (
            {
                "candidate": name,
                "penalty": _selection_penalty(summaries, neutral_ticks),
                "symbols": dict(zip(symbols, summaries, strict=True)),
            }
            for name, summaries in train_results.items()
        ),
        key=lambda row: row["penalty"],
    )
    selected_name = ranking[0]["candidate"]
    selected_config = next(config for name, config in candidates if name == selected_name)

    validation_results: dict[str, Any] = {}
    for symbol in symbols:
        ticks = validation_ticks.pop(symbol)
        validation_results[symbol] = {
            "T1000": _summary(_fixed_tick_bars(ticks, neutral_ticks)),
            "I1000_v1": _summary(
                build_information_bars(ticks, InformationBarConfig(budget=neutral_ticks))
            ),
            "I1000_v2": _summary(
                build_information_bars_v2(ticks, selected_config),
                minimum_target=selected_config.min_target_ticks,
                maximum_target=selected_config.max_target_ticks,
            ),
        }
        del ticks
        gc.collect()

    compact_ranking = [
        {
            "candidate": row["candidate"],
            "penalty": row["penalty"],
            "symbols": {
                symbol: {
                    "median": summary["volume"]["median"],
                    "p05": summary["volume"]["p05"],
                    "p95": summary["volume"]["p95"],
                    "cv": summary["volume"]["cv"],
                    "path_per_tick_ratio_low_over_high": summary["quartiles"]["path_per_tick_ratio_low_over_high"],
                    "duration_ratio_high_over_low": summary["quartiles"]["duration_ratio_high_over_low"],
                    "bounds": summary["bounds"],
                }
                for symbol, summary in row["symbols"].items()
            },
        }
        for row in ranking
    ]
    output = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "protocol": {
            "symbols": symbols,
            "source_limit_per_symbol": source_limit,
            "split": "first chronological half train, second half validation",
            "database_access": "bounded read-only existing ticks; no backfill",
        },
        "baselines": baselines,
        "training_ranking": compact_ranking if os.getenv("INFO_V2_COMPACT") == "1" else ranking,
        "selected": {
            "name": selected_name,
            "config": selected_config.metadata(),
        },
        "validation": validation_results,
    }
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), default=str))


if __name__ == "__main__":
    asyncio.run(main())

"""
Live terminal dashboard for the MT5 Poller.

Uses **Rich** ``Live`` display with a table-based layout that auto-refreshes
every second.  Activated by ``python -m src.poller_main --dashboard``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.metrics import PollerMetrics

# Refresh interval (seconds)
_REFRESH_INTERVAL = 1.0

# Stale threshold (seconds)
_STALE_THRESHOLD = 30.0


def _ts_ago(ts: float, now: float) -> str:
    """Format seconds-ago for monotonic timestamps."""
    if ts <= 0:
        return "-"
    diff = now - ts
    if diff < 0:
        diff = 0
    if diff < 60:
        return f"{diff:.0f}s ago"
    m, s = divmod(int(diff), 60)
    return f"{m}m{s:02d}s ago"


def _dt_short(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    return dt.strftime("%H:%M:%S")


def _health_dot(ok: bool) -> Text:
    if ok:
        return Text("●", style="green bold")
    return Text("●", style="red bold")


def _rate_color(rate: float) -> str:
    if rate > 50:
        return "green"
    if rate > 10:
        return "yellow"
    return "dim"


# -----------------------------------------------------------------------
# Panel builders
# -----------------------------------------------------------------------

def _build_status_panel(m: PollerMetrics) -> Panel:
    """Panel 1: Connection & Tasks."""
    tbl = Table.grid(padding=(0, 2))
    tbl.add_column(style="bold cyan", justify="right")
    tbl.add_column()

    tbl.add_row("MT5", _health_dot(m.mt5_connected))
    tbl.add_row("Uptime", Text(m.uptime_str()))
    tbl.add_row("Reconnects", Text(str(m.reconnect_count), style="yellow" if m.reconnect_count else "green"))

    # Tasks
    for task_name, alive in sorted(m.task_alive.items()):
        tbl.add_row(f"  {task_name}", _health_dot(alive))

    # Backfill phase
    if m.backfill_phase:
        tbl.add_row("Backfill", Text(f"{m.backfill_phase}: {m.backfill_current}", style="magenta"))

    return Panel(tbl, title="[bold]Status[/bold]", border_style="blue")


def _build_errors_panel(m: PollerMetrics) -> Panel:
    """Panel 2: Error counters & stats."""
    tbl = Table.grid(padding=(0, 2))
    tbl.add_column(style="bold cyan", justify="right")
    tbl.add_column()

    total_err = m.total_errors()
    err_style = "red bold" if total_err > 0 else "green"
    tbl.add_row("Total Errors", Text(str(total_err), style=err_style))

    for cat in ("tick_loop", "candle_loop", "flush", "publish", "heartbeat", "gap_scan", "backfill"):
        cnt = m.errors.get(cat, 0)
        style = "red" if cnt > 0 else "dim"
        tbl.add_row(f"  {cat}", Text(str(cnt), style=style))

    # Gap scan
    tbl.add_row("", Text(""))
    tbl.add_row("Last Gap Scan", Text(_dt_short(m.last_gap_scan_time)))
    tbl.add_row("Gaps Found", Text(str(m.gaps_found), style="yellow" if m.gaps_found else "dim"))

    return Panel(tbl, title="[bold]Errors & Health[/bold]", border_style="blue")


def _build_symbols_panel(m: PollerMetrics) -> Panel:
    """Panel 3: Per-symbol tick table."""
    import time as _time

    tbl = Table(
        show_header=True,
        header_style="bold",
        padding=(0, 1),
        expand=True,
    )
    tbl.add_column("Symbol", style="cyan", no_wrap=True)
    tbl.add_column("Bid", justify="right")
    tbl.add_column("Ask", justify="right")
    tbl.add_column("Spread", justify="right")
    tbl.add_column("Ticks", justify="right")
    tbl.add_column("Last", justify="right")

    now_mono = _time.monotonic()
    stale_set = set(m.stale_symbols(_STALE_THRESHOLD))

    for sym in sorted(m.symbol_ticks.keys()):
        info = m.symbol_ticks[sym]
        # Spread in pips: JPY pairs use 3-digit, others 5-digit
        if "JPY" in sym:
            spread = (info.ask - info.bid) * 1_000  # 3-digit
        else:
            spread = (info.ask - info.bid) * 100_000  # 5-digit
        if spread < 0:
            spread = 0

        row_style = "red" if sym in stale_set else ""
        stale_marker = " ⚠" if sym in stale_set else ""

        tbl.add_row(
            f"{sym}{stale_marker}",
            f"{info.bid:.5f}",
            f"{info.ask:.5f}",
            f"{spread:.1f}",
            str(info.count),
            _ts_ago(info.last_tick_ts, now_mono),
            style=row_style,
        )

    return Panel(tbl, title="[bold]Symbols[/bold]", border_style="green")


def _build_throughput_panel(m: PollerMetrics) -> Panel:
    """Panel 4: DB throughput & rates."""
    rate = m.update_peak_rate()

    tbl = Table.grid(padding=(0, 2))
    tbl.add_column(style="bold cyan", justify="right")
    tbl.add_column()

    tbl.add_row("Ticks Total", Text(f"{m.ticks_total:,}"))
    tbl.add_row("Candles Total", Text(f"{m.candles_total:,}"))
    tbl.add_row("Flushed", Text(f"{m.ticks_flushed_total:,}"))
    tbl.add_row("Buffer", Text(str(m.tick_buffer_depth)))
    tbl.add_row("", Text(""))
    tbl.add_row("Ticks/sec", Text(f"{rate:.1f}", style=_rate_color(rate)))
    tbl.add_row("Peak t/s", Text(f"{m.peak_ticks_sec:.1f}", style="bold"))
    tbl.add_row("", Text(""))
    tbl.add_row("Redis Pub", Text(f"{m.redis_pub_count:,}"))
    tbl.add_row("Flushes", Text(f"{m.flush_count:,}"))
    tbl.add_row("Avg Flush", Text(f"{m.avg_flush_ms():.1f} ms"))
    tbl.add_row("Last Flush", Text(f"{m.last_flush_ms:.1f} ms"))

    return Panel(tbl, title="[bold]Throughput[/bold]", border_style="green")


def _build_ondemand_panel(m: PollerMetrics) -> Panel:
    """Panel 5: On-demand backfill log (last 10 entries)."""
    tbl = Table(
        show_header=True,
        header_style="bold",
        padding=(0, 1),
        expand=True,
    )
    tbl.add_column("Time", no_wrap=True)
    tbl.add_column("Symbol")
    tbl.add_column("Type")
    tbl.add_column("TF")
    tbl.add_column("Rows", justify="right")
    tbl.add_column("Sec", justify="right")
    tbl.add_column("Status")

    entries = list(m.on_demand_log)[-10:]
    for e in reversed(entries):
        st_style = "green" if e.status == "ok" else "red"
        tbl.add_row(
            _dt_short(e.ts),
            e.symbol,
            e.data_type,
            e.timeframe or "-",
            str(e.rows),
            f"{e.elapsed_sec:.2f}",
            Text(e.status, style=st_style),
        )

    if not entries:
        tbl.add_row("-", "-", "-", "-", "-", "-", "-")

    return Panel(tbl, title="[bold]On-Demand Backfill[/bold]", border_style="magenta")


# -----------------------------------------------------------------------
# Full layout
# -----------------------------------------------------------------------

def _build_layout(m: PollerMetrics) -> Layout:
    """Build the full dashboard layout."""
    layout = Layout()

    layout.split_column(
        Layout(name="top", size=14),
        Layout(name="middle", ratio=3),
        Layout(name="bottom", ratio=2),
        Layout(name="footer", size=3),
    )

    # Top row: Status + Errors
    layout["top"].split_row(
        Layout(_build_status_panel(m), name="status"),
        Layout(_build_errors_panel(m), name="errors"),
    )

    # Middle: Symbols table
    layout["middle"].update(_build_symbols_panel(m))

    # Bottom row: Throughput + On-demand
    layout["bottom"].split_row(
        Layout(_build_throughput_panel(m), name="throughput"),
        Layout(_build_ondemand_panel(m), name="ondemand"),
    )

    # Footer
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    footer = Text(
        f"  MT5 Poller Dashboard  |  {now}  |  Uptime {m.uptime_str()}  |  Ctrl+C to quit",
        style="bold white on dark_blue",
    )
    layout["footer"].update(Panel(footer, style="on dark_blue"))

    return layout


# -----------------------------------------------------------------------
# Async runner
# -----------------------------------------------------------------------

async def run_dashboard() -> None:
    """
    Non-blocking Rich Live loop — runs as an ``asyncio.Task``.

    Renders the dashboard every ``_REFRESH_INTERVAL`` seconds.
    """
    m = PollerMetrics()
    console = Console()

    with Live(
        _build_layout(m),
        console=console,
        refresh_per_second=1,
        screen=True,
    ) as live:
        try:
            while True:
                live.update(_build_layout(m))
                await asyncio.sleep(_REFRESH_INTERVAL)
        except asyncio.CancelledError:
            pass

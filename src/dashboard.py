"""
Live terminal dashboard for the MT5 Poller.

Uses **Rich** ``Live`` display with a modern Catppuccin-inspired colour
palette, rounded borders, and Unicode status icons.

Activated by ``python -m src.poller_main --dashboard``.
"""

from __future__ import annotations

import asyncio
import time as _time
from datetime import datetime, timezone

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from rich.text import Text

from src.metrics import PollerMetrics

# ── Refresh ──────────────────────────────────────────────────────────────
_REFRESH_INTERVAL = 1.0
_STALE_THRESHOLD = 30.0

# ── Catppuccin Mocha palette (soft & muted) ─────────────────────────────
_BASE      = "#1e1e2e"   # deep background
_SURFACE0  = "#313244"   # panel bg
_SURFACE1  = "#45475a"   # lighter surface
_OVERLAY   = "#6c7086"   # muted / dim
_TEXT      = "#cdd6f4"   # primary text
_SUBTEXT   = "#a6adc8"   # secondary text

_BLUE      = "#89b4fa"   # soft blue
_GREEN     = "#a6e3a1"   # soft green
_YELLOW    = "#f9e2af"   # soft amber
_RED       = "#f38ba8"   # soft rose
_MAUVE     = "#cba6f7"   # soft purple
_PEACH     = "#fab387"   # soft orange
_TEAL      = "#94e2d5"   # soft teal
_SAPPHIRE  = "#74c7ec"   # light blue
_FLAMINGO  = "#f2cdcd"   # pink

# ── Reusable styles ─────────────────────────────────────────────────────
S_LABEL   = Style(color=_OVERLAY, bold=False)
S_VALUE   = Style(color=_TEXT)
S_ACCENT  = Style(color=_BLUE, bold=True)
S_OK      = Style(color=_GREEN)
S_WARN    = Style(color=_YELLOW)
S_ERR     = Style(color=_RED)
S_DIM     = Style(color=_OVERLAY)
S_MUTED   = Style(color=_SUBTEXT)
S_MAUVE   = Style(color=_MAUVE)
S_PEACH   = Style(color=_PEACH)
S_TEAL    = Style(color=_TEAL)
S_SAPH    = Style(color=_SAPPHIRE)

# ── Unicode glyphs ──────────────────────────────────────────────────────
ICO_OK     = Text("●", style=S_OK)
ICO_FAIL   = Text("●", style=S_ERR)
ICO_CONN   = "◈"
ICO_CLOCK  = "◷"
ICO_BOLT   = "⚡"
ICO_DB     = "◇"
ICO_REDIS  = "◆"
ICO_ARROW  = "▸"
ICO_WARN   = "△"
ICO_SCAN   = "⟳"
ICO_PKG    = "▪"
ICO_TASK   = "›"
ICO_GAUGE  = "▏"

# ── Mini-bar helper (for rates, buffer, etc.) ───────────────────────────
_BAR_CHARS = " ▏▎▍▌▋▊▉█"


def _mini_bar(value: float, max_val: float, width: int = 8) -> Text:
    """Return a soft gradient bar."""
    if max_val <= 0:
        return Text(_BAR_CHARS[0] * width, style=S_DIM)
    ratio = min(value / max_val, 1.0)
    filled = ratio * width
    full_blocks = int(filled)
    frac = filled - full_blocks
    idx = int(frac * (len(_BAR_CHARS) - 1))

    bar = _BAR_CHARS[-1] * full_blocks
    if full_blocks < width:
        bar += _BAR_CHARS[idx]
        bar += " " * (width - full_blocks - 1)

    if ratio > 0.7:
        color = _GREEN
    elif ratio > 0.3:
        color = _SAPPHIRE
    else:
        color = _OVERLAY
    return Text(bar, style=Style(color=color))


# ── Formatting helpers ──────────────────────────────────────────────────

def _ts_ago(ts: float, now: float) -> Text:
    if ts <= 0:
        return Text("—", style=S_DIM)
    diff = now - ts
    if diff < 0:
        diff = 0
    if diff < 60:
        style = S_OK if diff < 5 else (S_WARN if diff < 15 else S_ERR)
        return Text(f"{diff:.0f}s", style=style)
    m, s = divmod(int(diff), 60)
    return Text(f"{m}m{s:02d}s", style=S_ERR)


def _dt_short(dt: datetime | None) -> Text:
    if dt is None:
        return Text("—", style=S_DIM)
    return Text(dt.strftime("%H:%M:%S"), style=S_MUTED)


def _num(n: int | float, fmt: str = ",") -> Text:
    return Text(f"{n:{fmt}}", style=S_VALUE)


def _kv_row(tbl: Table, icon: str, label: str, value: Text) -> None:
    """Add a label → value row with icon prefix."""
    tbl.add_row(
        Text(f" {icon} {label}", style=S_LABEL),
        value,
    )


# ═══════════════════════════════════════════════════════════════════════
# Panel builders
# ═══════════════════════════════════════════════════════════════════════

def _build_status_panel(m: PollerMetrics) -> Panel:
    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1, justify="right")

    # MT5 connection
    dot = ICO_OK if m.mt5_connected else ICO_FAIL
    lbl = Text(" Connected", style=S_OK) if m.mt5_connected else Text(" Disconnected", style=S_ERR)
    mt5_line = Text.assemble(f" {ICO_CONN} MT5  ", dot, lbl)
    tbl.add_row(mt5_line, Text(""))

    # Uptime
    _kv_row(tbl, ICO_CLOCK, "Uptime", Text(m.uptime_str(), style=S_SAPH))

    # Reconnects
    rc_style = S_WARN if m.reconnect_count else S_OK
    _kv_row(tbl, ICO_SCAN, "Reconnects", Text(str(m.reconnect_count), style=rc_style))

    # Separator
    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Tasks
    for task_name, alive in sorted(m.task_alive.items()):
        dot = ICO_OK if alive else ICO_FAIL
        tbl.add_row(
            Text.assemble(f"  {ICO_TASK} ", Text(task_name, style=S_MUTED), " ", dot),
            Text(""),
        )

    # Backfill phase
    if m.backfill_phase:
        tbl.add_row(Text(""), Text(""))
        phase_text = Text.assemble(
            f"  {ICO_BOLT} ",
            Text(m.backfill_phase, style=S_MAUVE),
            Text(f" {m.backfill_current}", style=S_DIM),
        )
        tbl.add_row(phase_text, Text(""))

    return Panel(
        tbl,
        title=f"[{_BLUE}]  {ICO_CONN} Status [/{_BLUE}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


def _build_health_panel(m: PollerMetrics) -> Panel:
    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1, justify="right")

    total_err = m.total_errors()
    if total_err == 0:
        _kv_row(tbl, "✓", "All Clear", Text("0 errors", style=S_OK))
    else:
        _kv_row(tbl, ICO_WARN, "Total Errors", Text(str(total_err), style=S_ERR))

    tbl.add_row(Text(""), Text(""))

    categories = [
        ("tick_loop",   "Tick Loop"),
        ("candle_loop", "Candle Loop"),
        ("flush",       "Flush"),
        ("publish",     "Publish"),
        ("heartbeat",   "Heartbeat"),
        ("gap_scan",    "Gap Scan"),
        ("backfill",    "Backfill"),
    ]
    for key, label in categories:
        cnt = m.errors.get(key, 0)
        style = S_ERR if cnt > 0 else S_DIM
        icon = "✗" if cnt > 0 else "·"
        tbl.add_row(
            Text(f"   {icon} {label}", style=S_LABEL),
            Text(str(cnt), style=style),
        )

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Gap scan info
    _kv_row(tbl, ICO_SCAN, "Last Scan", _dt_short(m.last_gap_scan_time))
    gap_style = S_WARN if m.gaps_found else S_DIM
    _kv_row(tbl, ICO_WARN, "Gaps", Text(str(m.gaps_found), style=gap_style))

    return Panel(
        tbl,
        title=f"[{_RED}]  {ICO_WARN} Health [/{_RED}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


def _build_symbols_panel(m: PollerMetrics) -> Panel:
    tbl = Table(
        show_header=True,
        header_style=Style(color=_OVERLAY, bold=True),
        show_lines=False,
        padding=(0, 1),
        expand=True,
        row_styles=[Style(color=_TEXT), Style(color=_SUBTEXT)],
        border_style=Style(color=_SURFACE1),
        show_edge=False,
    )
    tbl.add_column("Symbol", style=Style(color=_TEAL, bold=True), no_wrap=True)
    tbl.add_column("Bid", justify="right", style=S_VALUE)
    tbl.add_column("Ask", justify="right", style=S_VALUE)
    tbl.add_column("Sprd", justify="right")
    tbl.add_column("Ticks", justify="right", style=S_MUTED)
    tbl.add_column("Age", justify="right")
    tbl.add_column("", justify="left", width=3)

    now_mono = _time.monotonic()
    stale_set = set(m.stale_symbols(_STALE_THRESHOLD))

    for sym in sorted(m.symbol_ticks.keys()):
        info = m.symbol_ticks[sym]
        # Spread in pips: JPY pairs use 3-digit, others 5-digit
        if "JPY" in sym:
            spread = (info.ask - info.bid) * 1_000
        else:
            spread = (info.ask - info.bid) * 100_000
        if spread < 0:
            spread = 0

        is_stale = sym in stale_set
        sprd_style = S_WARN if spread > 30 else (S_OK if spread < 15 else S_MUTED)
        status_icon = Text(f" {ICO_WARN}", style=S_WARN) if is_stale else Text(" ●", style=S_OK)
        age = _ts_ago(info.last_tick_ts, now_mono)

        tbl.add_row(
            sym,
            f"{info.bid:.5f}",
            f"{info.ask:.5f}",
            Text(f"{spread:.1f}", style=sprd_style),
            f"{info.count:,}",
            age,
            status_icon,
        )

    if not m.symbol_ticks:
        tbl.add_row(
            Text("waiting for data…", style=S_DIM),
            "", "", "", "", "", "",
        )

    return Panel(
        tbl,
        title=f"[{_TEAL}]  {ICO_BOLT} Live Prices [/{_TEAL}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 0),
    )


def _build_throughput_panel(m: PollerMetrics) -> Panel:
    rate = m.update_peak_rate()

    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=2, justify="right")

    _kv_row(tbl, ICO_DB, "Ticks", _num(m.ticks_total))
    _kv_row(tbl, ICO_DB, "Candles", _num(m.candles_total))
    _kv_row(tbl, "↓", "Flushed", _num(m.ticks_flushed_total))
    _kv_row(tbl, ICO_GAUGE, "Buffer", Text(str(m.tick_buffer_depth), style=S_PEACH if m.tick_buffer_depth > 100 else S_MUTED))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Tick rate with mini-bar
    rate_color = S_OK if rate > 50 else (S_SAPH if rate > 10 else S_DIM)
    rate_text = Text.assemble(
        Text(f"{rate:.1f}", style=rate_color),
        Text(" t/s ", style=S_DIM),
        _mini_bar(rate, max(m.peak_ticks_sec, 100), width=6),
    )
    _kv_row(tbl, ICO_BOLT, "Rate", rate_text)
    _kv_row(tbl, "↑", "Peak", Text(f"{m.peak_ticks_sec:.1f} t/s", style=S_MAUVE))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    _kv_row(tbl, ICO_REDIS, "Pub", _num(m.redis_pub_count))
    _kv_row(tbl, ICO_PKG, "Flushes", _num(m.flush_count))

    flush_color = S_WARN if m.avg_flush_ms() > 50 else S_OK
    _kv_row(tbl, ICO_CLOCK, "Avg", Text(f"{m.avg_flush_ms():.1f} ms", style=flush_color))
    _kv_row(tbl, ICO_CLOCK, "Last", Text(f"{m.last_flush_ms:.1f} ms", style=S_MUTED))

    return Panel(
        tbl,
        title=f"[{_PEACH}]  {ICO_DB} Throughput [/{_PEACH}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


def _build_ondemand_panel(m: PollerMetrics) -> Panel:
    tbl = Table(
        show_header=True,
        header_style=Style(color=_OVERLAY, bold=True),
        show_lines=False,
        padding=(0, 1),
        expand=True,
        border_style=Style(color=_SURFACE1),
        show_edge=False,
    )
    tbl.add_column("Time", no_wrap=True, style=S_DIM)
    tbl.add_column("Sym", style=Style(color=_TEAL))
    tbl.add_column("Type", style=S_MUTED)
    tbl.add_column("TF", style=S_MUTED)
    tbl.add_column("Rows", justify="right", style=S_VALUE)
    tbl.add_column("Sec", justify="right", style=S_MUTED)
    tbl.add_column("", justify="center", width=3)

    entries = list(m.on_demand_log)[-8:]
    for e in reversed(entries):
        st = Text("✓", style=S_OK) if e.status == "ok" else Text("✗", style=S_ERR)
        tbl.add_row(
            _dt_short(e.ts),
            e.symbol,
            e.data_type[:3],
            e.timeframe or "—",
            f"{e.rows:,}",
            f"{e.elapsed_sec:.2f}",
            st,
        )

    if not entries:
        tbl.add_row(
            Text("no requests yet", style=S_DIM), "", "", "", "", "", "",
        )

    return Panel(
        tbl,
        title=f"[{_MAUVE}]  {ICO_SCAN} On-Demand [/{_MAUVE}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 0),
    )


# ═══════════════════════════════════════════════════════════════════════
# Header & Footer
# ═══════════════════════════════════════════════════════════════════════

def _build_header(m: PollerMetrics) -> Panel:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    title = Text.assemble(
        Text("  ◈ ", style=Style(color=_SAPPHIRE, bold=True)),
        Text("MT5 POLLER", style=Style(color=_BLUE, bold=True)),
        Text("  DASHBOARD", style=Style(color=_OVERLAY)),
        Text("   │   ", style=Style(color=_SURFACE1)),
        Text(f"{ICO_CLOCK} ", style=Style(color=_OVERLAY)),
        Text(now, style=Style(color=_SUBTEXT)),
        Text("   │   ", style=Style(color=_SURFACE1)),
        Text(f"{ICO_CLOCK} ", style=Style(color=_OVERLAY)),
        Text(f"up {m.uptime_str()}", style=Style(color=_TEAL)),
    )
    return Panel(
        title,
        style=Style(color=_TEXT),
        border_style=Style(color=_SURFACE1),
        padding=(0, 0),
    )


def _build_footer() -> Text:
    return Text.assemble(
        Text(f"  {ICO_TASK} ", style=Style(color=_SURFACE1)),
        Text("Ctrl+C", style=Style(color=_OVERLAY, bold=True)),
        Text(" to quit", style=Style(color=_OVERLAY)),
        Text("   │   ", style=Style(color=_SURFACE1)),
        Text("Refresh 1s", style=Style(color=_OVERLAY)),
        Text("   │   ", style=Style(color=_SURFACE1)),
        Text("MT5 Connector", style=Style(color=_SURFACE1)),
    )


# ═══════════════════════════════════════════════════════════════════════
# Full layout
# ═══════════════════════════════════════════════════════════════════════

def _build_layout(m: PollerMetrics) -> Layout:
    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top", size=14),
        Layout(name="middle", minimum_size=5, ratio=3),
        Layout(name="bottom", minimum_size=5, ratio=2),
        Layout(name="footer", size=1),
    )

    layout["header"].update(_build_header(m))

    layout["top"].split_row(
        Layout(_build_status_panel(m), name="status"),
        Layout(_build_health_panel(m), name="health"),
    )

    layout["middle"].update(_build_symbols_panel(m))

    layout["bottom"].split_row(
        Layout(_build_throughput_panel(m), name="throughput"),
        Layout(_build_ondemand_panel(m), name="ondemand"),
    )

    layout["footer"].update(_build_footer())

    return layout


# ═══════════════════════════════════════════════════════════════════════
# Async runner
# ═══════════════════════════════════════════════════════════════════════

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

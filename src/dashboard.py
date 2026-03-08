"""
Live terminal dashboard for the MT5 Poller.

Uses **Rich** ``Live`` display with a modern Catppuccin-inspired colour
palette, rounded borders, and Unicode status icons.

Layout (6 panels + live-prices table):

    ┌──────────────┬──────────────┐
    │ MT5 Status   │  API Health  │  top
    ├──────────────┼──────────────┤
    │ Tick Stats   │ Candle Stats │  mid-upper
    ├──────────────┼──────────────┤
    │ DB / Redis   │ Backfill Log │  mid-lower
    ├──────────────┴──────────────┤
    │       Live Prices           │  bottom
    └─────────────────────────────┘

Activated by ``python -m src.poller_main --dashboard``.
"""

from __future__ import annotations

import asyncio
import time as _time
from datetime import datetime, timezone

from rich.console import Console
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
_BASE      = "#1e1e2e"
_SURFACE0  = "#313244"
_SURFACE1  = "#45475a"
_OVERLAY   = "#6c7086"
_TEXT      = "#cdd6f4"
_SUBTEXT   = "#a6adc8"

_BLUE      = "#89b4fa"
_GREEN     = "#a6e3a1"
_YELLOW    = "#f9e2af"
_RED       = "#f38ba8"
_MAUVE     = "#cba6f7"
_PEACH     = "#fab387"
_TEAL      = "#94e2d5"
_SAPPHIRE  = "#74c7ec"
_FLAMINGO  = "#f2cdcd"

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
ICO_API    = "⬡"

# ── Mini-bar helper ─────────────────────────────────────────────────────
_BAR_CHARS = " ▏▎▍▌▋▊▉█"


def _mini_bar(value: float, max_val: float, width: int = 8) -> Text:
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
    tbl.add_row(
        Text(f" {icon} {label}", style=S_LABEL),
        value,
    )


def _health_dot(ok: bool) -> Text:
    return ICO_OK if ok else ICO_FAIL


def _latency_text(ms: float) -> Text:
    if ms <= 0:
        return Text("—", style=S_DIM)
    style = S_OK if ms < 50 else (S_WARN if ms < 200 else S_ERR)
    return Text(f"{ms:.0f} ms", style=style)


# ═══════════════════════════════════════════════════════════════════════
# Panel 1 — MT5 Connection & Tasks
# ═══════════════════════════════════════════════════════════════════════

def _build_mt5_panel(m: PollerMetrics) -> Panel:
    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1, justify="right")

    # MT5 connection
    dot = _health_dot(m.mt5_connected)
    lbl = Text(" Connected", style=S_OK) if m.mt5_connected else Text(" Disconnected", style=S_ERR)
    tbl.add_row(Text.assemble(f" {ICO_CONN} MT5  ", dot, lbl), Text(""))

    # Uptime
    _kv_row(tbl, ICO_CLOCK, "Uptime", Text(m.uptime_str(), style=S_SAPH))

    rc_style = S_WARN if m.reconnect_count else S_OK
    _kv_row(tbl, ICO_SCAN, "Reconnects", Text(str(m.reconnect_count), style=rc_style))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Tasks
    for task_name, alive in sorted(m.task_alive.items()):
        dot = _health_dot(alive)
        tbl.add_row(
            Text.assemble(f"  {ICO_TASK} ", Text(task_name, style=S_MUTED), " ", dot),
            Text(""),
        )

    # Backfill phase
    if m.backfill_phase:
        tbl.add_row(Text(""), Text(""))
        tbl.add_row(
            Text.assemble(
                f"  {ICO_BOLT} ",
                Text(m.backfill_phase, style=S_MAUVE),
                Text(f" {m.backfill_current}", style=S_DIM),
            ),
            Text(""),
        )

    return Panel(
        tbl,
        title=f"[{_BLUE}]  {ICO_CONN} MT5 Connection [/{_BLUE}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


# ═══════════════════════════════════════════════════════════════════════
# Panel 2 — API Health
# ═══════════════════════════════════════════════════════════════════════

def _build_api_panel(m: PollerMetrics) -> Panel:
    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1, justify="right")

    # API status
    dot = _health_dot(m.api_healthy)
    lbl = Text(" Healthy", style=S_OK) if m.api_healthy else Text(" Unreachable", style=S_ERR)
    tbl.add_row(Text.assemble(f" {ICO_API} API  ", dot, lbl), Text(""))
    _kv_row(tbl, ICO_CLOCK, "Latency", _latency_text(m.api_latency_ms))
    _kv_row(tbl, ICO_CLOCK, "Avg (1h)", _latency_text(m.api_avg_latency_ms))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Request counts
    _kv_row(tbl, ICO_ARROW, "Req 1h", _num(m.api_requests_1h))
    _kv_row(tbl, ICO_ARROW, "Req 12h", _num(m.api_requests_12h))
    _kv_row(tbl, ICO_ARROW, "Req 24h", _num(m.api_requests_24h))

    # Errors
    err_style = S_ERR if m.api_errors_1h > 0 else S_OK
    _kv_row(tbl, ICO_WARN, "Err 1h", Text(str(m.api_errors_1h), style=err_style))

    return Panel(
        tbl,
        title=f"[{_SAPPHIRE}]  {ICO_API} API Health [/{_SAPPHIRE}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


# ═══════════════════════════════════════════════════════════════════════
# Panel 3 — Tick Collection
# ═══════════════════════════════════════════════════════════════════════

def _build_tick_panel(m: PollerMetrics) -> Panel:
    rate = m.update_peak_rate()

    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=2, justify="right")

    _kv_row(tbl, ICO_DB, "Total", _num(m.ticks_total))
    _kv_row(tbl, "↓", "Flushed", _num(m.ticks_flushed_total))
    _kv_row(tbl, ICO_GAUGE, "Buffer", Text(
        str(m.tick_buffer_depth),
        style=S_PEACH if m.tick_buffer_depth > 100 else S_MUTED,
    ))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Time windows
    t1h = m.ticks_in_window(3600)
    t12h = m.ticks_in_window(43200)
    t24h = m.ticks_in_window(86400)
    t7d = m.ticks_in_window(604800)
    _kv_row(tbl, ICO_CLOCK, "1 h", _num(t1h))
    _kv_row(tbl, ICO_CLOCK, "12 h", _num(t12h))
    _kv_row(tbl, ICO_CLOCK, "24 h", _num(t24h))
    _kv_row(tbl, ICO_CLOCK, "7 d", _num(t7d))

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

    return Panel(
        tbl,
        title=f"[{_PEACH}]  {ICO_BOLT} Tick Collection [/{_PEACH}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


# ═══════════════════════════════════════════════════════════════════════
# Panel 4 — Candle Collection
# ═══════════════════════════════════════════════════════════════════════

def _build_candle_panel(m: PollerMetrics) -> Panel:
    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=2, justify="right")

    _kv_row(tbl, ICO_DB, "Total", _num(m.candles_total))
    _kv_row(tbl, ICO_REDIS, "Pub", _num(m.redis_pub_count))
    _kv_row(tbl, ICO_PKG, "Flushes", _num(m.flush_count))

    flush_color = S_WARN if m.avg_flush_ms() > 50 else S_OK
    _kv_row(tbl, ICO_CLOCK, "Avg flush", Text(f"{m.avg_flush_ms():.1f} ms", style=flush_color))
    _kv_row(tbl, ICO_CLOCK, "Last flush", Text(f"{m.last_flush_ms:.1f} ms", style=S_MUTED))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Time windows
    c1h = m.candles_in_window(3600)
    c12h = m.candles_in_window(43200)
    c24h = m.candles_in_window(86400)
    c7d = m.candles_in_window(604800)
    _kv_row(tbl, ICO_CLOCK, "1 h", _num(c1h))
    _kv_row(tbl, ICO_CLOCK, "12 h", _num(c12h))
    _kv_row(tbl, ICO_CLOCK, "24 h", _num(c24h))
    _kv_row(tbl, ICO_CLOCK, "7 d", _num(c7d))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Error overview
    total_err = m.total_errors()
    if total_err == 0:
        _kv_row(tbl, "✓", "Errors", Text("0", style=S_OK))
    else:
        _kv_row(tbl, ICO_WARN, "Errors", Text(str(total_err), style=S_ERR))

    # Gap scan info
    _kv_row(tbl, ICO_SCAN, "Last scan", _dt_short(m.last_gap_scan_time))
    gap_style = S_WARN if m.gaps_found else S_DIM
    _kv_row(tbl, ICO_WARN, "Gaps", Text(str(m.gaps_found), style=gap_style))

    return Panel(
        tbl,
        title=f"[{_MAUVE}]  {ICO_DB} Candle Collection [/{_MAUVE}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


# ═══════════════════════════════════════════════════════════════════════
# Panel 5 — Database & Redis
# ═══════════════════════════════════════════════════════════════════════

def _build_infra_panel(m: PollerMetrics) -> Panel:
    tbl = Table.grid(padding=(0, 1), expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1, justify="right")

    # DB
    dot = _health_dot(m.db_healthy)
    lbl = Text(" OK", style=S_OK) if m.db_healthy else Text(" Down", style=S_ERR)
    tbl.add_row(Text.assemble(f" {ICO_DB} TimescaleDB  ", dot, lbl), Text(""))
    _kv_row(tbl, ICO_CLOCK, "Latency", _latency_text(m.db_latency_ms))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Redis
    dot = _health_dot(m.redis_healthy)
    lbl = Text(" OK", style=S_OK) if m.redis_healthy else Text(" Down", style=S_ERR)
    tbl.add_row(Text.assemble(f" {ICO_REDIS} Redis  ", dot, lbl), Text(""))
    _kv_row(tbl, ICO_CLOCK, "Latency", _latency_text(m.redis_latency_ms))

    tbl.add_row(Text("   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─", style=S_DIM), Text(""))

    # Error breakdown
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

    return Panel(
        tbl,
        title=f"[{_GREEN}]  {ICO_DB} Database & Redis [/{_GREEN}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 1),
    )


# ═══════════════════════════════════════════════════════════════════════
# Panel 6 — Backfill / On-Demand Log
# ═══════════════════════════════════════════════════════════════════════

def _build_backfill_panel(m: PollerMetrics) -> Panel:
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
        title=f"[{_FLAMINGO}]  {ICO_SCAN} Backfill Log [/{_FLAMINGO}]",
        title_align="left",
        border_style=Style(color=_SURFACE1),
        padding=(0, 0),
    )


# ═══════════════════════════════════════════════════════════════════════
# Live Prices table
# ═══════════════════════════════════════════════════════════════════════

def _build_prices_panel(m: PollerMetrics) -> Panel:
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
        Layout(name="row1", size=12),
        Layout(name="row2", size=14),
        Layout(name="row3", size=14),
        Layout(name="prices", minimum_size=5, ratio=2),
        Layout(name="footer", size=1),
    )

    layout["header"].update(_build_header(m))

    layout["row1"].split_row(
        Layout(_build_mt5_panel(m), name="mt5"),
        Layout(_build_api_panel(m), name="api"),
    )

    layout["row2"].split_row(
        Layout(_build_tick_panel(m), name="ticks"),
        Layout(_build_candle_panel(m), name="candles"),
    )

    layout["row3"].split_row(
        Layout(_build_infra_panel(m), name="infra"),
        Layout(_build_backfill_panel(m), name="backfill"),
    )

    layout["prices"].update(_build_prices_panel(m))

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

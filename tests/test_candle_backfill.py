"""
Tests verifying the historical candle backfill fixes.

These tests validate that:
1. /candles/{symbol} with from/to triggers on-demand backfill and returns data
2. /candles/custom/{symbol} with to+bars returns recent bars, not ancient ones
3. POST /backfill works as an explicit preload endpoint
4. /coverage shows all configured symbols and correct metadata
5. _needs_backfill_candles heuristics handle all edge cases
6. _estimate_from_for_limit uses reference_time correctly
7. query_candles SQL returns correct rows for to-only vs from/to vs no-range
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure test settings load before any project imports
os.environ.setdefault("MT5_LOGIN", "0")
os.environ.setdefault("MT5_PASSWORD", "test")
os.environ.setdefault("MT5_SERVER", "test")
os.environ.setdefault("DB_PASSWORD", "test")

from src.api.services.backfill_helper import (
    _CANDLE_GAP_TOLERANCE_SEC,
    _estimate_from_for_limit,
    _needs_backfill_candles,
    _recent_backfills,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_candle_row(
    t: datetime,
    symbol: str = "EURUSD",
    tf: str = "M1",
    price: float = 1.1000,
) -> dict[str, Any]:
    """Create a fake candle row dict matching repo.query_candles output."""
    return {
        "time": t,
        "symbol": symbol,
        "timeframe": tf,
        "open": price,
        "high": price + 0.001,
        "low": price - 0.001,
        "close": price + 0.0005,
        "tick_volume": 100,
        "real_volume": 0,
        "spread": 2,
    }


def _make_rows(
    start: datetime,
    count: int,
    interval_sec: int = 60,
    symbol: str = "EURUSD",
    tf: str = "M1",
) -> list[dict[str, Any]]:
    """Generate a list of candle rows starting from *start*."""
    return [
        _make_candle_row(
            start + timedelta(seconds=i * interval_sec),
            symbol=symbol,
            tf=tf,
        )
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# 1. _estimate_from_for_limit — reference_time support
# ---------------------------------------------------------------------------

class TestEstimateFromForLimit:
    """Checklist prerequisite: backfill range must be relative to dt_to."""

    def test_defaults_to_now(self):
        result = _estimate_from_for_limit("M1", 100)
        expected_lookback_sec = int(60 * 100 * 1.5)
        now = datetime.now(timezone.utc)
        # Result should be roughly `now - lookback`
        diff = abs((now - result).total_seconds() - expected_lookback_sec)
        assert diff < 5, f"Expected ~{expected_lookback_sec}s ago, got diff={diff}"

    def test_respects_reference_time(self):
        ref = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        result = _estimate_from_for_limit("M1", 100, reference_time=ref)
        expected_lookback_sec = int(60 * 100 * 1.5)
        expected = ref - timedelta(seconds=expected_lookback_sec)
        assert result == expected

    def test_h1_lookback(self):
        ref = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        result = _estimate_from_for_limit("H1", 24, reference_time=ref)
        expected_lookback_sec = int(3600 * 24 * 1.5)
        expected = ref - timedelta(seconds=expected_lookback_sec)
        assert result == expected


# ---------------------------------------------------------------------------
# 2. _needs_backfill_candles — comprehensive edge cases
# ---------------------------------------------------------------------------

class TestNeedsBackfillCandles:
    """Checklist items 1 & 2: ensure backfill triggers correctly."""

    def test_empty_rows_with_from_triggers(self):
        """Empty result for explicit from → backfill needed."""
        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        assert _needs_backfill_candles([], dt_from, limit=1000) is True

    def test_empty_rows_no_from_triggers(self):
        """Empty result with no from but limit > 0 → backfill needed."""
        assert _needs_backfill_candles([], None, limit=100) is True

    def test_enough_rows_no_from_no_to(self):
        """Enough rows, no range specified → no backfill."""
        rows = _make_rows(datetime(2025, 1, 1, tzinfo=timezone.utc), 100)
        assert _needs_backfill_candles(rows, None, limit=100) is False

    def test_fewer_rows_no_from_triggers(self):
        """Fewer rows than limit with no from → backfill needed."""
        rows = _make_rows(datetime(2025, 1, 1, tzinfo=timezone.utc), 50)
        assert _needs_backfill_candles(rows, None, limit=100) is True

    def test_gap_at_start_triggers(self):
        """First row is far from requested from → backfill needed."""
        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        # First row is 10 minutes after from (> 2 min tolerance)
        rows = _make_rows(dt_from + timedelta(minutes=10), 50)
        assert _needs_backfill_candles(rows, dt_from, limit=100) is True

    def test_small_gap_at_start_ok(self):
        """First row is within tolerance of from → no backfill."""
        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        rows = _make_rows(dt_from + timedelta(seconds=60), 50)
        assert _needs_backfill_candles(rows, dt_from, limit=100) is False

    def test_to_only_rows_far_from_to_triggers(self):
        """With dt_to set but no from, if latest row is far from to → trigger."""
        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        # Rows end at 2025-07-16 20:00 — 1 hour gap from dt_to
        rows = _make_rows(
            datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc), 60,
        )
        # 60 rows × 1 min = last row at 19:59, gap to 21:00 = 61 min
        assert _needs_backfill_candles(
            rows, None, limit=60, timeframe="M1", dt_to=dt_to,
        ) is True

    def test_to_only_rows_close_to_to_ok(self):
        """With dt_to set, if latest row is close to to → no backfill."""
        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        rows = _make_rows(
            datetime(2025, 7, 16, 20, 0, tzinfo=timezone.utc), 60,
        )
        # 60 rows × 1 min = last row at 20:59 — 1 min gap from to
        assert _needs_backfill_candles(
            rows, None, limit=60, timeframe="M1", dt_to=dt_to,
        ) is False

    def test_naive_datetime_handled(self):
        """Naive datetimes should be treated as UTC without crashing."""
        dt_from = datetime(2025, 7, 16, 19, 0)  # naive
        rows = _make_rows(datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc), 50)
        # Should not raise
        result = _needs_backfill_candles(rows, dt_from, limit=100)
        assert result is False


# ---------------------------------------------------------------------------
# 3. maybe_backfill_candles — integration with mocked repo + requester
# ---------------------------------------------------------------------------

class TestMaybeBackfillCandles:
    """Checklist item 1: from/to triggers on-demand backfill."""

    @pytest.fixture(autouse=True)
    def _clear_cooldown(self):
        _recent_backfills.clear()
        yield
        _recent_backfills.clear()

    async def test_triggers_backfill_when_db_empty(self):
        """GET /candles with from/to on empty DB → triggers backfill."""
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        july_rows = _make_rows(dt_from, 120)

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 120,
        }

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            # First call: empty (before backfill) → second call: populated
            mock_repo.query_candles = AsyncMock(side_effect=[[], july_rows])

            rows = await maybe_backfill_candles(
                symbol="EURUSD",
                timeframe="M1",
                dt_from=dt_from,
                dt_to=dt_to,
                limit=50000,
            )

        assert len(rows) == 120
        # Verify backfill was requested with the correct range
        call_args = mock_requester.request_and_wait.call_args
        assert call_args.kwargs["dt_from"] == dt_from
        assert call_args.kwargs["dt_to"] == dt_to
        assert call_args.kwargs["timeframe"] == "M1"

    async def test_no_backfill_when_data_covers_range(self):
        """If DB already has data covering from → no backfill triggered."""
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        rows = _make_rows(dt_from, 120)

        with patch("src.api.services.backfill_helper.repo") as mock_repo:
            mock_repo.query_candles = AsyncMock(return_value=rows)
            result = await maybe_backfill_candles(
                symbol="EURUSD",
                timeframe="M1",
                dt_from=dt_from,
                dt_to=dt_to,
                limit=50000,
            )

        assert len(result) == 120
        # query_candles should have been called only once (no re-query)
        mock_repo.query_candles.assert_called_once()

    async def test_to_only_triggers_backfill_with_correct_range(self):
        """Checklist #2: to-only request estimates from relative to to."""
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 100,
        }

        july_rows = _make_rows(
            datetime(2025, 7, 16, 19, 20, tzinfo=timezone.utc), 100,
        )

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            mock_repo.query_candles = AsyncMock(side_effect=[[], july_rows])

            rows = await maybe_backfill_candles(
                symbol="EURUSD",
                timeframe="M1",
                dt_from=None,
                dt_to=dt_to,
                limit=5000,
            )

        assert len(rows) == 100
        call_args = mock_requester.request_and_wait.call_args
        # bf_from should be relative to dt_to, not to now()
        bf_from = call_args.kwargs["dt_from"]
        bf_to = call_args.kwargs["dt_to"]
        assert bf_to == dt_to
        assert bf_from < dt_to, "bf_from must be before bf_to"
        # bf_from should be roughly 5000 * 60 * 1.5 = 450000 sec ≈ 5.2 days before dt_to
        expected_lookback = timedelta(seconds=int(60 * 5000 * 1.5))
        assert bf_from == dt_to - expected_lookback

    async def test_cooldown_recorded_after_attempt(self):
        """Cooldown should be set AFTER the backfill attempt, not before."""
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = None  # timeout

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            mock_repo.query_candles = AsyncMock(return_value=[])

            await maybe_backfill_candles(
                symbol="EURUSD",
                timeframe="M1",
                dt_from=dt_from,
                dt_to=dt_to,
                limit=1000,
            )

        # Cooldown should be recorded even on timeout (to prevent flood)
        assert "EURUSD:M1" in _recent_backfills


# ---------------------------------------------------------------------------
# 4. Candle endpoint — bars parameter and pagination
# ---------------------------------------------------------------------------

class TestCandleEndpoint:
    """Checklist items 1 & 2: verify endpoint parameter handling."""

    async def test_bars_overrides_limit(self):
        """The bars parameter should override limit."""
        from src.api.routes.candles import get_candles

        mock_rows = _make_rows(
            datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc), 50,
        )

        with (
            patch("src.api.routes.candles.maybe_backfill_candles", new_callable=AsyncMock) as mock_bf,
            patch("src.api.routes.candles.validate_symbol", return_value="EURUSD"),
        ):
            mock_bf.return_value = mock_rows

            result = await get_candles(
                symbol="EURUSD",
                timeframe="M1",
                from_dt=datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc),
                to_dt=datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc),
                limit=1000,
                bars=50,
            )

        # bars=50 → effective_limit=50 → fetch_limit=51 (from is set)
        assert mock_bf.call_args.kwargs["limit"] == 51

    async def test_to_only_uses_latest_n_path(self):
        """to-only (no from): use_latest_n=True, no +1 fetch trick."""
        from src.api.routes.candles import get_candles

        mock_rows = _make_rows(
            datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc), 100,
        )

        with (
            patch("src.api.routes.candles.maybe_backfill_candles", new_callable=AsyncMock) as mock_bf,
            patch("src.api.routes.candles.validate_symbol", return_value="EURUSD"),
        ):
            mock_bf.return_value = mock_rows

            result = await get_candles(
                symbol="EURUSD",
                timeframe="M1",
                from_dt=None,
                to_dt=datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc),
                limit=100,
                bars=None,
            )

        # use_latest_n=True → fetch_limit=effective_limit (no +1)
        assert mock_bf.call_args.kwargs["limit"] == 100
        assert result.count == 100


# ---------------------------------------------------------------------------
# 5. Custom candle endpoint — bars parameter
# ---------------------------------------------------------------------------

class TestCustomCandleEndpoint:
    """Checklist item 2: /candles/custom respects to+bars."""

    async def test_bars_param_accepted(self):
        """bars parameter should be accepted and used as effective_limit."""
        from src.api.routes.custom_candles import get_custom_candles

        mock_rows = _make_rows(
            datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc), 200,
        )

        with (
            patch("src.api.routes.custom_candles.maybe_backfill_candles", new_callable=AsyncMock) as mock_bf,
            patch("src.api.routes.custom_candles.validate_symbol", return_value="EURUSD"),
        ):
            mock_bf.return_value = mock_rows

            result = await get_custom_candles(
                symbol="EURUSD",
                timeframe="M1",
                from_dt=None,
                to_dt=datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc),
                limit=1000,
                bars=200,
                price="bid",
                include_incomplete=False,
            )

        # bars=200 overrides limit=1000
        assert mock_bf.call_args.kwargs["limit"] == 200
        assert result.count == 200

    async def test_information_bars_are_built_backend_side_with_research_meta(self):
        """I<n> returns ready candles and an explicit chart-only contract."""
        from src.api.routes.custom_candles import get_custom_candles

        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        ticks = [
            {
                "time_msc": start + timedelta(milliseconds=index),
                "symbol": "EURUSD",
                "bid": 1.0 + index * 0.00001,
                "ask": 1.00002 + index * 0.00001,
                "last": 1.0 + index * 0.00001,
                "volume": 1,
                "flags": 6,
            }
            for index in range(400)
        ]

        with (
            patch("src.api.routes.custom_candles.maybe_backfill_ticks", new_callable=AsyncMock),
            patch("src.api.routes.custom_candles.validate_symbol", return_value="EURUSD"),
            patch(
                "src.api.routes.custom_candles.repo.query_information_bar_ticks",
                new_callable=AsyncMock,
                return_value=ticks,
            ) as query,
        ):
            result = await get_custom_candles(
                symbol="EURUSD",
                timeframe="I100",
                from_dt=None,
                to_dt=None,
                limit=3,
                bars=None,
                price="bid",
                include_incomplete=False,
            )

        assert result.count == 3
        assert all(item.timeframe == "I100" for item in result.data)
        assert all(item.is_complete is True for item in result.data)
        assert result.meta is not None
        assert result.meta["strategy_eligible"] is False
        assert result.meta["bar_model"]["algorithm"] == "adaptive-information-bars-v1"
        assert query.call_args.kwargs["source_limit"] > 400


# ---------------------------------------------------------------------------
# 6. Backfill endpoint
# ---------------------------------------------------------------------------

class TestBackfillEndpoint:
    """Checklist item 3: POST /backfill triggers explicit preload."""

    async def test_successful_backfill(self):
        from src.api.routes.backfill import trigger_backfill, BackfillRequest

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 5000, "error": None,
        }

        with (
            patch("src.api.routes.backfill.validate_symbol", return_value="EURUSD"),
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
        ):
            req = BackfillRequest(
                symbol="EURUSD",
                timeframe="M1",
                **{"from": datetime(2025, 7, 1, tzinfo=timezone.utc),
                   "to": datetime(2025, 8, 1, tzinfo=timezone.utc)},
            )
            result = await trigger_backfill(req)

        assert result.status == "ok"
        assert result.rows == 5000
        assert result.symbol == "EURUSD"

    async def test_backfill_timeout(self):
        from src.api.routes.backfill import trigger_backfill, BackfillRequest

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = None  # timeout

        with (
            patch("src.api.routes.backfill.validate_symbol", return_value="EURUSD"),
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
        ):
            req = BackfillRequest(
                symbol="EURUSD",
                timeframe="M1",
                **{"from": datetime(2025, 7, 1, tzinfo=timezone.utc),
                   "to": datetime(2025, 8, 1, tzinfo=timezone.utc)},
            )
            result = await trigger_backfill(req)

        assert result.status == "timeout"
        assert result.rows == 0

    async def test_backfill_invalid_range(self):
        from src.api.routes.backfill import trigger_backfill, BackfillRequest
        from fastapi import HTTPException

        with (
            patch("src.api.routes.backfill.validate_symbol", return_value="EURUSD"),
            pytest.raises(HTTPException) as exc_info,
        ):
            req = BackfillRequest(
                symbol="EURUSD",
                timeframe="M1",
                **{"from": datetime(2025, 8, 1, tzinfo=timezone.utc),
                   "to": datetime(2025, 7, 1, tzinfo=timezone.utc)},
            )
            await trigger_backfill(req)

        assert exc_info.value.status_code == 400

    async def test_backfill_no_requester(self):
        from src.api.routes.backfill import trigger_backfill, BackfillRequest
        from fastapi import HTTPException

        with (
            patch("src.api.routes.backfill.validate_symbol", return_value="EURUSD"),
            patch("src.api.app.get_backfill_requester", return_value=None),
            pytest.raises(HTTPException) as exc_info,
        ):
            req = BackfillRequest(
                symbol="EURUSD",
                timeframe="M1",
                **{"from": datetime(2025, 7, 1, tzinfo=timezone.utc),
                   "to": datetime(2025, 8, 1, tzinfo=timezone.utc)},
            )
            await trigger_backfill(req)

        assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# 7. Coverage endpoint
# ---------------------------------------------------------------------------

class TestCoverageEndpoint:
    """Checklist item 5: coverage shows all configured symbols."""

    async def test_coverage_includes_configured_symbols(self):
        from src.api.routes.coverage import get_coverage

        with (
            patch("src.api.routes.coverage.repo") as mock_repo,
            patch("src.api.routes.coverage.get_settings") as mock_settings,
        ):
            mock_settings.return_value.symbols = [
                "EURUSD", "EURGBP", "USDJPY", "AUDCAD", "XAUUSD",
            ]
            mock_repo.query_candle_coverage = AsyncMock(return_value=[
                {"symbol": "EURUSD", "timeframe": "M1",
                 "first_bar": datetime(2023, 6, 8, tzinfo=timezone.utc),
                 "last_bar": datetime(2025, 12, 18, tzinfo=timezone.utc),
                 "total": 100000},
            ])
            mock_repo.query_tick_coverage = AsyncMock(return_value=[])
            mock_repo.query_all_sync_states = AsyncMock(return_value=[])

            result = await get_coverage()

        symbol_names = [s.symbol for s in result.symbols]
        # All configured symbols should appear, even those with no data
        assert "EURUSD" in symbol_names
        assert "EURGBP" in symbol_names
        assert "USDJPY" in symbol_names
        assert "AUDCAD" in symbol_names
        assert "XAUUSD" in symbol_names
        assert result.configured_symbols == [
            "AUDCAD", "EURGBP", "EURUSD", "USDJPY", "XAUUSD",
        ]

    async def test_coverage_has_note(self):
        from src.api.routes.coverage import get_coverage

        with (
            patch("src.api.routes.coverage.repo") as mock_repo,
            patch("src.api.routes.coverage.get_settings") as mock_settings,
        ):
            mock_settings.return_value.symbols = ["EURUSD"]
            mock_repo.query_candle_coverage = AsyncMock(return_value=[])
            mock_repo.query_tick_coverage = AsyncMock(return_value=[])
            mock_repo.query_all_sync_states = AsyncMock(return_value=[])

            result = await get_coverage()

        assert "on-demand" in result.note
        assert "/api/v1/backfill" in result.note

    async def test_coverage_empty_symbol_has_no_candles(self):
        """Symbols with no data should appear with empty candles list."""
        from src.api.routes.coverage import get_coverage

        with (
            patch("src.api.routes.coverage.repo") as mock_repo,
            patch("src.api.routes.coverage.get_settings") as mock_settings,
        ):
            mock_settings.return_value.symbols = ["AUDCAD"]
            mock_repo.query_candle_coverage = AsyncMock(return_value=[])
            mock_repo.query_tick_coverage = AsyncMock(return_value=[])
            mock_repo.query_all_sync_states = AsyncMock(return_value=[])

            result = await get_coverage()

        assert len(result.symbols) == 1
        assert result.symbols[0].symbol == "AUDCAD"
        assert result.symbols[0].candles == []
        assert result.symbols[0].ticks.total_ticks == 0


# ---------------------------------------------------------------------------
# 8. SQL query logic — query_candles branch selection
# ---------------------------------------------------------------------------

class TestQueryCandlesSQLBranch:
    """
    Verify the SQL branch logic in query_candles.
    This tests the condition logic without hitting a real DB.
    """

    def test_from_to_uses_asc(self):
        """With from+to, query should use ASC ordering (forward scan)."""
        from src.db.repository import query_candles
        import inspect

        source = inspect.getsource(query_candles)
        # The function should branch on dt_from for ASC vs DESC
        assert "if dt_from:" in source
        assert "ORDER BY time ASC LIMIT :limit" in source
        assert "ORDER BY time DESC LIMIT :limit" in source

    def test_no_from_uses_desc_subquery(self):
        """Without from (to-only or no-range), query returns latest N."""
        from src.db.repository import query_candles
        import inspect

        source = inspect.getsource(query_candles)
        # The else branch should wrap in a subquery for DESC → ASC
        assert "sub ORDER BY time ASC" in source


# ---------------------------------------------------------------------------
# 9. End-to-end scenario: July 2025 data retrieval
# ---------------------------------------------------------------------------

class TestJuly2025Scenario:
    """
    Simulates the exact failing scenario from the bug report:
    Account 2 has trades from 2025-07-16, but candles were empty.
    """

    @pytest.fixture(autouse=True)
    def _clear_cooldown(self):
        _recent_backfills.clear()
        yield
        _recent_backfills.clear()

    async def test_from_to_july2025_triggers_backfill_returns_data(self):
        """
        GET /candles/EURUSD?timeframe=M1&from=2025-07-16T19:00:00Z
            &to=2025-07-16T21:00:00Z&limit=50000

        Before fix: returned 200 with data: []
        After fix: triggers backfill, returns July 2025 bars
        """
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        july_bars = _make_rows(dt_from, 120)

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 120,
        }

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            mock_repo.query_candles = AsyncMock(side_effect=[[], july_bars])

            rows = await maybe_backfill_candles(
                symbol="EURUSD",
                timeframe="M1",
                dt_from=dt_from,
                dt_to=dt_to,
                limit=50000,
            )

        # Verify: non-empty result
        assert len(rows) > 0
        # Verify: all bars are in the requested range
        for r in rows:
            assert dt_from <= r["time"] <= dt_to
        # Verify: sorted ascending
        times = [r["time"] for r in rows]
        assert times == sorted(times)

    async def test_custom_to_bars_no_ancient_data(self):
        """
        GET /candles/custom/EURUSD?timeframe=M1
            &to=2025-07-16T21:00:00Z&bars=5000

        Before fix: returned bars from 2023-06-08
        After fix: returns bars ending near 2025-07-16T21:00:00Z
        """
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_to = datetime(2025, 7, 16, 21, 0, tzinfo=timezone.utc)
        # Simulate July 2025 data being available after backfill
        # 4980 M1 bars ending right before dt_to
        expected_start = datetime(2025, 7, 13, 10, 0, tzinfo=timezone.utc)
        july_bars = _make_rows(expected_start, 4980)

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 4980,
        }

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            # First call returns empty, second returns data
            mock_repo.query_candles = AsyncMock(side_effect=[[], july_bars])

            rows = await maybe_backfill_candles(
                symbol="EURUSD",
                timeframe="M1",
                dt_from=None,
                dt_to=dt_to,
                limit=5000,
            )

        # Verify: data is returned
        assert len(rows) > 0
        # Verify: no bars from 2023
        for r in rows:
            assert r["time"].year >= 2025, f"Got ancient bar: {r['time']}"
        # Verify: last bar is not after dt_to
        last_bar_time = rows[-1]["time"]
        assert last_bar_time <= dt_to

    async def test_eurgbp_backfill_covers_july(self):
        """
        Checklist item 4: EURGBP backfill should cover July 2025,
        not start from 2025-12-18.
        """
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_from = datetime(2025, 7, 16, 19, 0, tzinfo=timezone.utc)
        dt_to = datetime(2025, 7, 16, 21, 30, tzinfo=timezone.utc)
        july_bars = _make_rows(dt_from, 150, symbol="EURGBP")

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 150,
        }

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            mock_repo.query_candles = AsyncMock(side_effect=[[], july_bars])

            rows = await maybe_backfill_candles(
                symbol="EURGBP",
                timeframe="M1",
                dt_from=dt_from,
                dt_to=dt_to,
                limit=50000,
            )

        assert len(rows) == 150
        # Backfill was requested for the correct range
        call_kwargs = mock_requester.request_and_wait.call_args.kwargs
        assert call_kwargs["symbol"] == "EURGBP"
        assert call_kwargs["dt_from"] == dt_from
        assert call_kwargs["dt_to"] == dt_to

    async def test_usdjpy_backfill_covers_july(self):
        """
        Checklist item 4: USDJPY backfill should cover July 2025.
        """
        from src.api.services.backfill_helper import maybe_backfill_candles

        dt_from = datetime(2025, 7, 16, 20, 0, tzinfo=timezone.utc)
        dt_to = datetime(2025, 7, 17, 4, 0, tzinfo=timezone.utc)
        july_bars = _make_rows(dt_from, 480, symbol="USDJPY")

        mock_requester = AsyncMock()
        mock_requester.request_and_wait.return_value = {
            "status": "ok", "rows": 480,
        }

        mock_limiter = AsyncMock()

        with (
            patch("src.api.services.backfill_helper.repo") as mock_repo,
            patch("src.api.app.get_backfill_requester", return_value=mock_requester),
            patch("src.api.services.validation.backfill_limiter", mock_limiter),
        ):
            mock_repo.query_candles = AsyncMock(side_effect=[[], july_bars])

            rows = await maybe_backfill_candles(
                symbol="USDJPY",
                timeframe="M1",
                dt_from=dt_from,
                dt_to=dt_to,
                limit=50000,
            )

        assert len(rows) == 480
        call_kwargs = mock_requester.request_and_wait.call_args.kwargs
        assert call_kwargs["symbol"] == "USDJPY"
        assert call_kwargs["dt_from"] == dt_from

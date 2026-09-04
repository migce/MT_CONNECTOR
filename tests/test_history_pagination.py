from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.api.routes import custom_candles as route
from src.db.heavy_reads import HistoryBudgetExceeded


@pytest.mark.asyncio
@pytest.mark.parametrize('ranged', [False, True])
async def test_large_tick_request_is_paged_on_complete_bar_boundaries(ranged):
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    def rows(**kwargs):
        return [dict(time=start+timedelta(seconds=i), symbol='EURUSD', timeframe='T4000',
                     open=1.,high=1.,low=1.,close=1.,tick_volume=4000) for i in range(kwargs['limit'])]
    with patch.object(route, 'maybe_backfill_ticks', new_callable=AsyncMock), patch.object(
        route.repo, 'query_tick_bars', new=AsyncMock(side_effect=rows)
    ) as query:
        response = await route.get_custom_candles(symbol='EURUSD', timeframe='T4000',
            from_dt=start if ranged else None,to_dt=None,limit=1000,bars=None,price='bid',include_incomplete=False)
    assert query.call_args.kwargs['limit'] * 4000 <= 300_000
    assert response.count == (74 if ranged else 75)
    assert response.meta['source_truncated'] is True
    assert response.has_more
    assert (response.next_from is not None) == ranged


@pytest.mark.asyncio
async def test_oversized_single_bar_is_rejected_before_backfill():
    with patch.object(route, 'maybe_backfill_ticks', new_callable=AsyncMock) as backfill:
        with pytest.raises(HistoryBudgetExceeded):
            await route.get_custom_candles(symbol='EURUSD',timeframe='T999999',from_dt=None,to_dt=None,
                limit=1,bars=None,price='bid',include_incomplete=False)
    backfill.assert_not_awaited()

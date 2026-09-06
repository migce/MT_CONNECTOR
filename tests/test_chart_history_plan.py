from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.api.services.history_plan import ChartHistoryRequest, bounded_window, chart_history_plan
from src.api.routes.symbol_management import ChartHistoryStart, start_chart_history, BackfillJobCreate, _create_job
from src.db.symbol_management import HistoryJobCapacityError
from src.history_status import HistoryWorkerUnavailable

NOW = datetime(2026, 9, 6, 8, tzinfo=UTC)


@pytest.mark.parametrize('timeframe,source,source_tf,target', [
    ('T16000', 'ticks', None, 'custom'), ('H2', 'candles', 'H1', 'custom'),
    ('M10', 'candles', 'M5', 'custom'), ('H1', 'candles', 'H1', 'candles'),
])
@pytest.mark.asyncio
async def test_plan_reuses_exact_source_and_is_read_only(timeframe, source, source_tf, target):
    with patch('src.api.services.history_plan.first_source_time', AsyncMock(return_value=NOW-timedelta(days=2))) as edge, \
         patch('src.api.services.history_plan.get_retention_days', AsyncMock(return_value=365)):
        plan = await chart_history_plan(ChartHistoryRequest(symbol='USTEC', timeframe=timeframe, required_bars=1500, loaded_bars=658))
    assert plan['source_type'] == source and plan['source_timeframe'] == source_tf
    assert plan['target_type'] == target
    assert edge.await_count == 1
    assert plan['mode'] == 'fill_missing' and plan['estimated'] is True
    assert datetime.fromisoformat(plan['to']) - datetime.fromisoformat(plan['from']) <= timedelta(days=plan['max_window_days'])


def test_tick_window_extends_before_existing_history_not_into_live_tail():
    first = NOW-timedelta(days=14)
    start, end, limited = bounded_window(ChartHistoryRequest(symbol='USTEC', timeframe='T16000', required_bars=1500, loaded_bars=658), first, NOW, 365)
    assert end == first and start == first-timedelta(days=7) and not limited


def test_no_tick_data_uses_one_week_not_fake_tick_to_time_arithmetic():
    start, end, _ = bounded_window(ChartHistoryRequest(symbol='USTEC', timeframe='T16000', required_bars=1500), None, NOW, 365)
    assert end == NOW and start == NOW-timedelta(days=7)


def test_retention_exhaustion_does_not_change_retention():
    body = ChartHistoryRequest(symbol='USTEC', timeframe='T16000', required_bars=1500)
    assert bounded_window(body, NOW-timedelta(days=365), NOW, 365) is None


def test_long_period_plan_capped_and_reports_limit():
    start, end, limited = bounded_window(ChartHistoryRequest(symbol='USTEC', timeframe='D2', required_bars=15000), None, NOW, 365)
    assert end-start == timedelta(days=365) and limited


@pytest.mark.parametrize('timeframe', ['D999999', 'W999999', 'H999999'])
def test_large_untrusted_period_cannot_overflow_datetime(timeframe):
    start, end, limited = bounded_window(ChartHistoryRequest(symbol='USTEC', timeframe=timeframe, required_bars=15000), NOW-timedelta(days=700), NOW, 365)
    assert end-start == timedelta(days=365) and limited


def test_ancient_anchor_is_rejected_before_arithmetic():
    with pytest.raises(ValidationError):
        ChartHistoryRequest(symbol='USTEC', timeframe='H2', required_bars=1500, anchor='0001-01-01T00:00:00Z')


def test_naive_historical_anchor_is_normalized():
    anchor = NOW-timedelta(days=20)
    start, end, _ = bounded_window(ChartHistoryRequest(symbol='USTEC', timeframe='H2', required_bars=700, anchor=anchor.replace(tzinfo=None)), None, NOW, 365)
    assert end == anchor and start < end


@pytest.mark.parametrize('changes', [{'required_bars': 15001}, {'required_bars': 0}, {'timeframe': 'T0'}, {'timeframe': 'garbage'}, {'loaded_bars': -1}])
def test_input_budget_is_validated(changes):
    with pytest.raises(ValidationError):
        ChartHistoryRequest(**({'symbol': 'USTEC', 'timeframe': 'T16000', 'required_bars': 1500} | changes))


@pytest.mark.asyncio
async def test_worker_unavailable_prevents_planning_and_job_creation():
    requester = AsyncMock()
    requester._require_history_worker.side_effect = HistoryWorkerUnavailable()
    with patch('src.api.routes.symbol_management._available_symbol', return_value=('USTEC', '')), \
         patch('src.api.app.get_backfill_requester', return_value=requester), \
         patch('src.api.routes.symbol_management.chart_history_plan', AsyncMock()) as plan, \
         pytest.raises(HistoryWorkerUnavailable):
        await start_chart_history(ChartHistoryStart(symbol='USTEC', timeframe='T16000', required_bars=1500))
    plan.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_returns_bounded_queue_error_with_retry_hint():
    with patch('src.api.routes.symbol_management._available_symbol', return_value=('USTEC', '')), \
         patch('src.api.routes.symbol_management.sm.create_job', AsyncMock(side_effect=HistoryJobCapacityError())), \
         pytest.raises(HTTPException) as exc:
        await _create_job(BackfillJobCreate(symbol='USTEC', target_type='candles', timeframe='H1', **{'from':NOW-timedelta(days=2), 'to':NOW-timedelta(days=1)}))
    assert exc.value.status_code == 429 and exc.value.headers['Retry-After'] == '120'


@pytest.mark.asyncio
async def test_custom_ticks_cannot_bypass_retention():
    with patch('src.api.routes.symbol_management._available_symbol', return_value=('USTEC', '')), \
         patch('src.api.routes.symbol_management.sm.get_retention_days', AsyncMock(return_value=7)), \
         patch('src.api.routes.symbol_management.sm.create_job', AsyncMock()) as create, \
         pytest.raises(HTTPException) as exc:
        await _create_job(BackfillJobCreate(symbol='USTEC', target_type='custom', timeframe='T16000', **{'from':datetime.now(UTC)-timedelta(days=30), 'to':datetime.now(UTC)-timedelta(days=1)}))
    assert exc.value.status_code == 409
    create.assert_not_awaited()

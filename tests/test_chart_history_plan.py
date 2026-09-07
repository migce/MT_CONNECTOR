from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.api.services.history_plan import ChartHistoryRequest, bounded_window, chart_history_plan, no_extension_attempt
from src.api.routes.symbol_management import ChartHistoryStart, start_chart_history, BackfillJobCreate, _create_job
from src.db.symbol_management import HistoryJobCapacityError
from src.history_status import HistoryWorkerUnavailable

NOW = datetime(2026, 9, 6, 8, tzinfo=UTC)


@pytest.fixture(autouse=True)
def stored_tail():
    with patch('src.api.services.history_plan.latest_time_bars', AsyncMock(return_value=[])) as tail:
        yield tail


@pytest.mark.asyncio
@pytest.mark.parametrize('loaded', [0, 540, 1500])
async def test_existing_depth_is_authoritative_not_browser_count(stored_tail, loaded):
    stored_tail.return_value = [{'time': NOW-timedelta(minutes=3*i)} for i in reversed(range(1500))]
    with patch('src.api.services.history_plan.first_source_time', AsyncMock()) as edge, \
         patch('src.api.services.history_plan.recent_history_attempts', AsyncMock()) as attempts:
        plan = await chart_history_plan(ChartHistoryRequest(symbol='EURUSD', timeframe='M3', required_bars=1500, loaded_bars=loaded))
    assert plan['availability']['status'] == 'already_available'
    assert plan['availability']['stored_bars'] == 1500
    assert plan['availability']['first_source_at'] is None
    edge.assert_not_awaited(); attempts.assert_not_awaited()
    assert stored_tail.call_args.args[-1] is None


@pytest.mark.asyncio
async def test_historical_proof_uses_exact_anchor_and_server_shortage(stored_tail):
    anchor = NOW-timedelta(days=80)
    stored_tail.return_value = [{'time': anchor-timedelta(hours=6*i)} for i in reversed(range(242))]
    first = stored_tail.return_value[0]['time']
    with patch('src.api.services.history_plan.first_source_time', AsyncMock(return_value=first)), \
         patch('src.api.services.history_plan.recent_history_attempts', AsyncMock(return_value=[])):
        plan = await chart_history_plan(ChartHistoryRequest(symbol='ES500.U6', timeframe='H6', required_bars=268, loaded_bars=0, anchor=anchor))
    assert stored_tail.call_args.args[-1] == anchor
    assert plan['availability']['stored_bars'] == 242
    assert datetime.fromisoformat(plan['from']) < first
    assert datetime.fromisoformat(plan['to']) <= first+timedelta(hours=6)
    assert datetime.fromisoformat(plan['to'])-datetime.fromisoformat(plan['from']) < timedelta(days=30)


@pytest.mark.asyncio
async def test_existing_history_rejects_before_job_write():
    with patch('src.api.routes.symbol_management._available_symbol', return_value=('EURUSD', '')), \
         patch('src.api.app.get_backfill_requester', return_value=AsyncMock()), \
         patch('src.api.routes.symbol_management.chart_history_plan', AsyncMock(return_value={'availability': {'status':'already_available'}})), \
         patch('src.api.routes.symbol_management._create_job', AsyncMock()) as create, pytest.raises(HTTPException) as exc:
        await start_chart_history(ChartHistoryStart(symbol='EURUSD', timeframe='M3', required_bars=1500))
    assert exc.value.detail['code'] == 'history_already_available'
    create.assert_not_awaited()


@pytest.mark.parametrize('timeframe,source,source_tf,target', [
    ('T16000', 'ticks', None, 'custom'), ('H2', 'candles', 'H1', 'custom'),
    ('M10', 'candles', 'M5', 'custom'), ('H1', 'candles', 'H1', 'candles'),
])
@pytest.mark.asyncio
async def test_plan_reuses_exact_source_and_is_read_only(timeframe, source, source_tf, target):
    with patch('src.api.services.history_plan.first_source_time', AsyncMock(return_value=NOW-timedelta(days=2))) as edge, \
         patch('src.api.services.history_plan.get_retention_days', AsyncMock(return_value=365)), \
         patch('src.api.services.history_plan.recent_history_attempts', AsyncMock(return_value=[])):
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


def empty_attempt(**changes):
    return dict(source_type='candles', source_timeframe='H1', mode='fill_missing',
                status='partial', range_from=NOW-timedelta(days=180), range_to=NOW,
                covered_to=NOW-timedelta(days=1), rows_written=0,
                started_at=NOW-timedelta(minutes=11), finished_at=NOW-timedelta(minutes=10)) | changes


def detect(attempts, start=None):
    return no_extension_attempt(attempts, 'H1', NOW-timedelta(days=80), start or NOW-timedelta(days=140), NOW)


def test_recent_empty_extension_reports_observation_not_permanent_contract_floor():
    assert detect([empty_attempt()]) is not None


@pytest.mark.parametrize('changes', [
    {'status': 'failed'}, {'status': 'cancelled'}, {'status': 'running'},
    {'source_type': 'ticks'}, {'source_timeframe': 'H4'}, {'mode': 'refresh'},
    {'rows_written': 1}, {'covered_to': None}, {'started_at': None},
    {'covered_to': NOW-timedelta(days=100)}, {'range_from': NOW-timedelta(days=60)},
    {'range_to': NOW-timedelta(days=100)}, {'finished_at': None},
    {'finished_at': NOW-timedelta(hours=6)}, {'finished_at': NOW+timedelta(minutes=1)},
])
def test_no_false_boundary_from_outage_wrong_source_old_or_uncovered_attempt(changes):
    assert detect([empty_attempt(**changes)]) is None


def test_a_larger_older_request_is_not_blocked_by_a_smaller_previous_attempt():
    assert detect([empty_attempt()], start=NOW-timedelta(days=200)) is None


def test_newer_extension_invalidates_old_negative_evidence():
    assert detect([empty_attempt(rows_written=5), empty_attempt()]) is None


@pytest.mark.asyncio
async def test_plan_attaches_observed_date_and_finite_retry_not_contract_inception():
    now = datetime.now(UTC)
    attempt = empty_attempt(range_from=now-timedelta(days=364), range_to=now,
                            covered_to=now-timedelta(days=1), finished_at=now-timedelta(minutes=1))
    with patch('src.api.services.history_plan.first_source_time', AsyncMock(return_value=now-timedelta(days=80))), \
         patch('src.api.services.history_plan.recent_history_attempts', AsyncMock(return_value=[attempt])):
        plan = await chart_history_plan(ChartHistoryRequest(symbol='ES500.U6', timeframe='H6', required_bars=268, loaded_bars=242))
    info = plan['availability']
    assert info['status'] == 'no_additional_history'
    assert info['contract_start_confirmed'] is False
    assert info['first_source_at'] == (now-timedelta(days=80)).isoformat()
    assert datetime.fromisoformat(info['retry_after']) == attempt['finished_at']+timedelta(hours=6)


@pytest.mark.asyncio
async def test_no_extension_rejection_creates_and_enqueues_nothing():
    requester = AsyncMock()
    plan = {'availability': {'status': 'no_additional_history'}}
    with patch('src.api.routes.symbol_management._available_symbol', return_value=('ES500.U6', '')), \
         patch('src.api.app.get_backfill_requester', return_value=requester), \
         patch('src.api.routes.symbol_management.chart_history_plan', AsyncMock(return_value=plan)), \
         patch('src.api.routes.symbol_management._create_job', AsyncMock()) as create, \
         pytest.raises(HTTPException) as exc:
        await start_chart_history(ChartHistoryStart(symbol='ES500.U6', timeframe='H6', required_bars=268, loaded_bars=242))
    assert exc.value.status_code == 409
    assert exc.value.detail == {'code': 'history_no_additional_data', 'plan': plan}
    create.assert_not_awaited(); requester.enqueue_job.assert_not_awaited()

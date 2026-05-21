from datetime import datetime, timedelta, timezone
import asyncio

import trade_engine
import intraday_momentum_engine as ime
import intraday_exit_engine as iee
import broker_sync_loop
import database


def test_partial_fill_detection_and_rejected_status():
    partial = trade_engine.detect_partial_fills({'status':'Submitted','quantity':100,'filled_quantity':25,'remaining_quantity':75})
    assert partial['status'] == 'PARTIAL'
    rejected = trade_engine.detect_partial_fills({'status':'Inactive','quantity':10,'filled_quantity':0,'remaining_quantity':10})
    assert rejected['status'] == 'REJECTED'


def test_stale_order_detection():
    stale_ts = (datetime.now(timezone.utc)-timedelta(minutes=10)).isoformat()
    out = trade_engine.reconcile_open_orders([{'status':'Submitted','symbol':'AAPL','quantity':1,'filled_quantity':0,'remaining_quantity':1,'updated_at':stale_ts}])
    assert len(out['stale_orders']) == 1


def test_vwap_extension_and_market_confirmation_rejection():
    row = {
        'intraday_bars': {'1m':[1],'5m':[1],'15m':[1]},
        'price':110,'vwap':100,'ema9':99,'ema20':100,'relative_volume':0.8,'dollar_volume':100000,
        'opening_range_high':111,'setup':'none'
    }
    payload = ime.detect_intraday_entry_setup(row)
    assert payload['entry_allowed'] is False


def test_break_even_and_atr_like_exit_behaviour():
    out = iee.evaluate_exit({'symbol':'AAPL'}, {'pnl_pct':2.5})
    assert out['triggered'] is True


def test_broker_sync_repeated_failure_circuit_breaker(tmp_path, monkeypatch):
    monkeypatch.setattr(database, 'DB_PATH', str(tmp_path/'db.sqlite'))
    asyncio.run(database.init_db())
    class Bad:
        async def run_broker_sync_once(self):
            return {'ok':False,'errors':['down']}
    monkeypatch.setattr(broker_sync_loop, 'broker_sync', Bad())
    loop = broker_sync_loop.BrokerSyncLoop(interval_seconds=10, max_failures=2)
    asyncio.run(loop.sync_once())
    asyncio.run(loop.sync_once())
    state = asyncio.run(database.get_app_state('auto_trading_enabled'))
    assert state == 'false'

from __future__ import annotations

import auto_trader
import strategy_mode


def _run(coro):
    import asyncio
    return asyncio.run(coro)


def _setup(monkeypatch, *, allow_open=True):
    events = []
    opened_rows = []

    async def fake_get_app_state(*_args, **_kwargs):
        return "true"

    async def fake_state():
        return {"tripped": False}

    async def fake_startup_ok():
        return True

    async def fake_watchdog():
        return {"trading_blocked": False, "blocking_reasons": []}

    async def fake_mode():
        return strategy_mode.StrategyMode.INTRADAY_MOMENTUM

    async def fake_positions():
        return []

    async def fake_realized():
        return 0.0

    async def fake_event(payload):
        events.append(payload)

    async def fake_record(*_args, **_kwargs):
        return None

    async def fake_open(*, row, **_kwargs):
        opened_rows.append(row)
        return True

    monkeypatch.setattr(auto_trader.database, "get_app_state", fake_get_app_state)
    monkeypatch.setattr("circuit_breaker.get_circuit_breaker_state", fake_state)
    monkeypatch.setattr("startup_recovery.startup_recovery_passed", fake_startup_ok)
    monkeypatch.setattr("watchdog.get_watchdog_status", fake_watchdog)
    monkeypatch.setattr(auto_trader.strategy_mode, "get_strategy_mode", fake_mode)
    monkeypatch.setattr(auto_trader.database, "get_open_positions", fake_positions)
    monkeypatch.setattr(auto_trader.database, "get_realized_pnl", fake_realized)
    monkeypatch.setattr(auto_trader.database, "safe_record_trade_journal_event", fake_event)
    monkeypatch.setattr(auto_trader.database, "record_trade_decision", fake_record)
    monkeypatch.setattr(auto_trader.database, "record_rejected_setup", fake_record)
    monkeypatch.setattr(auto_trader, "auto_open_position", fake_open)
    monkeypatch.setattr(auto_trader, "get_market_hours_status", lambda: {"allowed": True})
    monkeypatch.setattr(auto_trader, "get_market_regime", lambda: {"regime": "BULL", "allow_new_buys": allow_open, "min_score_override": 60, "position_size_factor": 1.0})
    monkeypatch.setattr(auto_trader.intraday_momentum_engine, "detect_intraday_entry_setup", lambda row: row)

    return events, opened_rows


def test_intraday_aggressive_entry_allowed_is_candidate(monkeypatch):
    events, opened_rows = _setup(monkeypatch)
    _run(auto_trader.process_auto_trading([{"symbol": "SSYS", "aggressive_entry_allowed": True, "aggressive_score": 96}]))
    assert opened_rows
    assert any(e.get("event_type") == "AUTO_TRADER_INTRADAY_CANDIDATE_DETECTED" for e in events)


def test_intraday_intraday_entry_allowed_is_candidate(monkeypatch):
    events, opened_rows = _setup(monkeypatch)
    _run(auto_trader.process_auto_trading([{"symbol": "VRSK", "intraday_entry_allowed": True, "intraday_aggressive_score": 76}]))
    assert opened_rows
    assert any(e.get("event_type") == "AUTO_TRADER_INTRADAY_CANDIDATE_DETECTED" for e in events)


def test_intraday_legacy_entry_allowed_still_works(monkeypatch):
    _events, opened_rows = _setup(monkeypatch)
    _run(auto_trader.process_auto_trading([{"symbol": "ACMR", "entry_allowed": True, "intraday_momentum_score": 70}]))
    assert opened_rows


def test_intraday_all_false_rejected_with_reason(monkeypatch):
    events, opened_rows = _setup(monkeypatch)
    _run(auto_trader.process_auto_trading([{"symbol": "NOPE", "aggressive_entry_allowed": False, "intraday_entry_allowed": False, "entry_allowed": False, "signal": "NEUTRAL"}]))
    assert not opened_rows
    skipped = [e for e in events if e.get("event_type") == "AUTO_TRADER_INTRADAY_CANDIDATE_SKIPPED"]
    assert skipped
    assert skipped[0]["reason"] == "no_aggressive_or_intraday_entry_allowed"


def test_intraday_score_prefers_aggressive_score(monkeypatch):
    _events, opened_rows = _setup(monkeypatch)
    _run(auto_trader.process_auto_trading([{
        "symbol": "SCORE",
        "aggressive_entry_allowed": True,
        "aggressive_score": 85,
        "intraday_aggressive_score": 60,
        "intraday_momentum_score": 40,
    }]))
    assert opened_rows[0]["aggressive_score"] == 85


def test_valid_intraday_candidate_reaches_buy_order_submitted_path(monkeypatch):
    events, opened_rows = _setup(monkeypatch)
    _run(auto_trader.process_auto_trading([{"symbol": "BUYME", "aggressive_entry_allowed": True, "aggressive_score": 90}]))
    assert opened_rows
    assert any(e.get("event_type") == "BUY_CANDIDATE_ACCEPTED" for e in events)


def test_safety_gate_blocks_when_can_open_new_trades_false(monkeypatch):
    events, opened_rows = _setup(monkeypatch, allow_open=False)
    _run(auto_trader.process_auto_trading([{"symbol": "BLOCK", "aggressive_entry_allowed": True, "aggressive_score": 90}]))
    assert not opened_rows
    assert any(e.get("event_type") == "BUY_CANDIDATE_REJECTED" and "Market regime blocks new buys" in e.get("reason", "") for e in events)

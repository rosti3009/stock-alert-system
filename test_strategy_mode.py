from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import config
import database
import position_manager
import strategy_mode
from ibkr_asyncio_compat import ensure_event_loop


def run_async(coro):
    try:
        return asyncio.run(coro)
    finally:
        ensure_event_loop()


def test_switching_to_intraday_changes_buy_threshold():
    rules = strategy_mode.active_rules(strategy_mode.StrategyMode.INTRADAY_TECHNICAL)
    assert rules["min_score_to_buy"] == config.INTRADAY_MIN_SCORE_TO_BUY == 85
    assert rules["min_score_to_buy"] > strategy_mode.active_rules(strategy_mode.StrategyMode.SWING_DEFAULT)["min_score_to_buy"]


def test_switching_to_intraday_changes_max_positions():
    rules = strategy_mode.active_rules(strategy_mode.StrategyMode.INTRADAY_TECHNICAL)
    assert rules["max_open_positions"] == config.INTRADAY_MAX_OPEN_POSITIONS == 3
    assert strategy_mode.active_rules(strategy_mode.StrategyMode.SWING_DEFAULT)["max_open_positions"] == config.MAX_OPEN_POSITIONS


def test_intraday_blocks_buy_if_intraday_data_missing():
    decision = strategy_mode.validate_intraday_buy({
        "symbol": "AAPL",
        "signal": "BUY",
        "price": 100,
        "intraday_technical_score": 99,
        "relative_volume": 3,
        "volume": 1_000_000,
        "avg_volume": 1_000_000,
        "dollar_volume": 100_000_000,
        "bid": 99.99,
        "ask": 100.01,
        "setup": "breakout momentum",
        "trend": "Strong Bullish",
    })

    assert decision["allowed"] is False
    assert any("Intraday BUY blocked" in reason for reason in decision["reasons"])


def test_intraday_mode_does_not_bypass_execution_safety_gates():
    decision = strategy_mode.validate_intraday_buy({
        "symbol": "THIN",
        "signal": "BUY",
        "price": 10,
        "intraday_bars_available": True,
        "intraday_technical_score": 95,
        "relative_volume": 2,
        "volume": 1_000,
        "avg_volume": 1_000,
        "dollar_volume": 10_000_000,
        "bid": 9.00,
        "ask": 10.00,
        "setup": "breakout momentum",
        "trend": "Strong Bullish",
    })

    assert decision["allowed"] is False
    assert decision["execution_quality"]["blocks_buy"] is True
    assert any("Execution quality" in reason or "Spread too wide" in reason for reason in decision["reasons"])


def test_intraday_sell_logic_uses_intraday_exits():
    result = position_manager.evaluate_position(
        {"symbol": "AAPL", "buy_price": 100, "quantity": 2, "stop_loss": 98.5},
        {"symbol": "AAPL", "price": 98.4, "signal": "NEUTRAL"},
        mode=strategy_mode.StrategyMode.INTRADAY_TECHNICAL.value,
    )

    assert result["exit_engine"] == "intraday_exit"
    assert result["action"] == "INTRADAY_STOP_LOSS_HIT"
    assert result["status"] == "CLOSED"


def test_force_exit_before_close_is_active():
    status = strategy_mode.force_exit_before_close_status(
        datetime(2026, 5, 15, 19, 50, tzinfo=timezone.utc)  # 15:50 ET
    )

    assert status["enabled"] is True
    assert status["active"] is True
    assert status["minutes_before_close"] == 15


def test_switching_mode_does_not_reset_or_delete_positions(tmp_path):
    original_db_path = database.DB_PATH
    database.DB_PATH = str(tmp_path / "strategy_mode.db")
    try:
        run_async(database.init_db())
        run_async(database.add_position({"symbol": "AAPL", "buy_price": 100, "quantity": 1, "current_price": 100}))

        before = run_async(database.get_open_positions())
        payload = run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_TECHNICAL))
        after = run_async(database.get_open_positions())

        assert len(before) == 1
        assert len(after) == 1
        assert after[0]["symbol"] == "AAPL"
        assert payload["switch_warning"]
    finally:
        database.DB_PATH = original_db_path


def test_switching_back_to_swing_restores_swing_behavior(tmp_path):
    original_db_path = database.DB_PATH
    database.DB_PATH = str(tmp_path / "strategy_mode_back.db")
    try:
        run_async(database.init_db())
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_TECHNICAL))
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.SWING_DEFAULT))

        mode = run_async(strategy_mode.get_strategy_mode())
        rules = strategy_mode.active_rules(mode)

        assert mode == strategy_mode.StrategyMode.SWING_DEFAULT
        assert rules["min_score_to_buy"] == 80
        assert rules["max_open_positions"] == config.MAX_OPEN_POSITIONS
    finally:
        database.DB_PATH = original_db_path

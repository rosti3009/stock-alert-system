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
    rules = strategy_mode.active_rules(strategy_mode.StrategyMode.INTRADAY_MOMENTUM)
    assert rules["min_score_to_buy"] == 78
    assert rules["min_score_to_buy"] < strategy_mode.active_rules(strategy_mode.StrategyMode.SWING_DEFAULT)["min_score_to_buy"]


def test_switching_to_intraday_changes_max_positions():
    rules = strategy_mode.active_rules(strategy_mode.StrategyMode.INTRADAY_MOMENTUM)
    assert rules["max_open_positions"] == 8
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
        mode=strategy_mode.StrategyMode.INTRADAY_MOMENTUM.value,
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
        payload = run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_MOMENTUM))
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
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_MOMENTUM))
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.SWING_DEFAULT))

        mode = run_async(strategy_mode.get_strategy_mode())
        rules = strategy_mode.active_rules(mode)

        assert mode == strategy_mode.StrategyMode.SWING_DEFAULT
        assert rules["min_score_to_buy"] == 80
        assert rules["max_open_positions"] == config.MAX_OPEN_POSITIONS
    finally:
        database.DB_PATH = original_db_path

def test_aggressive_learning_profile_increases_capital_and_max_positions():
    original = {
        "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
        "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
        "PAPER_TRAINING_PROFILE": config.PAPER_TRAINING_PROFILE,
    }
    try:
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.PAPER_TRAINING_PROFILE = "AGGRESSIVE_LEARNING"

        rules = strategy_mode.intraday_rules()
        profile = config.active_paper_training_profile_rules()

        assert config.effective_virtual_trading_capital() == 500000.0
        assert profile["profile"] == "AGGRESSIVE_LEARNING"
        assert rules["max_open_positions"] == 8
        assert rules["position_size_factor"] == 0.5
        assert rules["min_score_to_buy"] == 78
        assert rules["min_relative_volume"] == 1.2
        assert rules["min_dollar_volume"] == 3000000.0
        assert rules["max_daily_trades"] == 15
        assert rules["max_consecutive_losses"] == 4
        assert rules["max_daily_loss_percent"] == 3.0
    finally:
        for key, value in original.items():
            setattr(config, key, value)


def test_live_mode_cannot_use_aggressive_paper_profile():
    original = {
        "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
        "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
        "PAPER_TRAINING_PROFILE": config.PAPER_TRAINING_PROFILE,
    }
    try:
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = True
        config.PAPER_TRAINING_PROFILE = "AGGRESSIVE_LEARNING"

        profile = config.active_paper_training_profile_rules()
        rules = strategy_mode.intraday_rules()

        assert profile["profile"] == "CONSERVATIVE"
        assert profile["requested_profile"] == "AGGRESSIVE_LEARNING"
        assert profile["live_profile_blocked"] is True
        assert config.effective_virtual_trading_capital() == config.VIRTUAL_TRADING_CAPITAL_USD
        assert rules["max_open_positions"] == config.INTRADAY_MAX_OPEN_POSITIONS
    finally:
        for key, value in original.items():
            setattr(config, key, value)


def test_strategy_payload_exposes_effective_training_profile():
    original = {
        "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
        "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
        "PAPER_TRAINING_PROFILE": config.PAPER_TRAINING_PROFILE,
    }
    try:
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.PAPER_TRAINING_PROFILE = "AGGRESSIVE_LEARNING"

        payload = strategy_mode.strategy_mode_payload(strategy_mode.StrategyMode.INTRADAY_MOMENTUM)

        assert payload["active_training_profile"] == "AGGRESSIVE_LEARNING"
        assert payload["profile_rules"]["paper_capital"] == 500000.0
        assert payload["effective_max_positions"] == 8
        assert payload["effective_score_threshold"] == 78
        assert payload["effective_risk_factor"] == 0.5
        assert payload["effective_max_daily_trades"] == 15
        assert "market_hours_guard" in payload["profile_rules"]["hard_protections_kept"]
        assert payload["force_exit_before_close"]["enabled"] is True
    finally:
        for key, value in original.items():
            setattr(config, key, value)

def test_intraday_momentum_score_not_weekly_primary():
    decision = strategy_mode.validate_intraday_buy({
        "symbol": "AAA", "intraday_bars": {"1m":[1],"5m":[1],"15m":[1]}, "price": 10, "vwap": 9.9,
        "relative_volume": 2.2, "dollar_volume": 10_000_000, "setup": "breakout momentum", "trend": "Strong Bullish", "weekly_score": 99,
    })
    assert decision["score"] <= 100


def test_high_weekly_missing_intraday_bars_rejected():
    decision = strategy_mode.validate_intraday_buy({"symbol":"AAA","weekly_score":99,"price":10})
    assert decision["allowed"] is False
    assert any("Intraday bars unavailable" in r for r in decision["reasons"])

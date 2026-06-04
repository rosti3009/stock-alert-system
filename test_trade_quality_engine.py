from __future__ import annotations

from datetime import datetime, timezone

import position_manager
import trade_quality_engine as tq


def base_candidate(**overrides):
    row = {
        "symbol": "TEST",
        "price": 50,
        "entry_price": 50,
        "bid": 49.95,
        "ask": 50.05,
        "avg_volume": 200_000,
        "volume": 400_000,
        "relative_volume": 2.0,
        "dollar_volume": 10_000_000,
        "vwap": 49,
        "ema9": 49.5,
        "ema20": 49.0,
        "ma20": 47,
        "ma50": 45,
        "momentum_5d": 5,
        "momentum_20d": 12,
        "intraday_momentum_score": 88,
        "rr_ratio": 2.4,
        "atr_percent": 3,
        "quote_timestamp": datetime.now(timezone.utc).isoformat(),
        "minutes_to_close": 120,
    }
    row.update(overrides)
    return row


def test_intraday_classification():
    result = tq.classify_candidate(base_candidate(strategy_type="INTRADAY"), market_hours={"allowed": True, "minutes_to_close": 120})
    assert result["strategy_type"] == "INTRADAY"
    assert result["trade_quality_score"] >= 75
    assert result["quality_grade"] in {"A", "B"}


def test_swing_classification():
    result = tq.classify_candidate(base_candidate(strategy_type="SWING", relative_volume=1.1), market_regime={"regime": "RISK_ON", "allow_new_buys": True})
    assert result["strategy_type"] == "SWING"
    assert result["trade_quality_score"] >= 70
    assert result["suggested_holding_period"].startswith("up to")


def test_rejected_low_liquidity_candidate():
    result = tq.classify_candidate(base_candidate(dollar_volume=100_000, avg_volume=2_000, volume=2_500, relative_volume=0.2))
    assert result["quality_grade"] == "REJECT"
    assert any("Dollar volume" in reason or "Relative volume" in reason for reason in result["rejection_reasons"])


def test_rejected_wide_spread_candidate():
    result = tq.classify_candidate(base_candidate(bid=49, ask=51, strategy_type="INTRADAY"), market_hours={"allowed": True, "minutes_to_close": 120})
    assert result["quality_grade"] == "REJECT"
    assert any("Spread" in reason for reason in result["rejection_reasons"])


def test_trade_quality_score_returns_full_component_breakdown():
    result = tq.classify_candidate(base_candidate())
    assert set(result["quality_components"]) == set(tq.QUALITY_COMPONENTS)
    assert all(0 <= score <= 100 for score in result["quality_components"].values())


def test_swing_position_does_not_receive_intraday_exit_action_in_intraday_mode():
    update = position_manager.evaluate_position(
        {"symbol": "SWNG", "strategy_type": "SWING", "buy_price": 100, "quantity": 2, "stop_loss": 92, "take_profit_1": 108, "take_profit_2": 116, "created_at": datetime.now(timezone.utc).isoformat()},
        {"price": 109, "signal": "HOLD"},
        mode="INTRADAY_TECHNICAL",
    )
    assert not str(update["action"]).startswith("INTRADAY_")


def test_intraday_position_does_not_receive_swing_exit_action_in_swing_mode():
    update = position_manager.evaluate_position(
        {"symbol": "DAY", "strategy_type": "INTRADAY", "buy_price": 100, "quantity": 2, "stop_loss": 98.5, "take_profit_1": 101.5, "take_profit_2": 103, "buy_date": "2020-01-01T00:00:00+00:00"},
        {"price": 100.2, "signal": "HOLD"},
        mode="SWING_DEFAULT",
    )
    assert update["action"] != "SWING_MAX_HOLD_DAYS"
    assert update.get("exit_engine") == "intraday_exit"


def test_intraday_exit_engine_ignores_swing_positions_directly():
    import intraday_exit_engine as iee

    update = iee.evaluate_exit(
        {"symbol": "SWNG", "strategy_type": "SWING"},
        {"pnl_pct": 5.0, "vwap_lost": True},
    )

    assert update["triggered"] is False
    assert update["reason"] == "not_intraday_position"

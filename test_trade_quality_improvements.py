from __future__ import annotations

from datetime import datetime, timedelta, timezone

import config
import database
import intraday_momentum_engine
import position_manager
import strategy_mode
from ranking_engine import RANKING_COMPONENTS, rank_candidates


def candidate(symbol: str, score: int, strategy_type: str = "INTRADAY") -> dict:
    return {
        "symbol": symbol,
        "signal": "BUY",
        "strategy_type": strategy_type,
        "price": 100,
        "score": score,
        "weekly_score": score,
        "intraday_momentum_score": score,
        "relative_volume": 2.5,
        "dollar_volume": 12_000_000,
        "spread_percent": 0.2,
        "slippage_estimate": 0.1,
        "trend": "Strong Bullish",
        "setup": "breakout momentum continuation",
        "vwap": 99,
        "ema9": 99.5,
        "ema20": 98,
        "ma20": 95,
        "ma50": 90,
        "rr_ratio": 2.5,
        "market_regime": "BULLISH",
        "momentum_5d": 4,
        "momentum_20d": 8,
    }


def test_intraday_thresholds_changed_and_safety_limits_kept():
    assert config.PAPER_TRAINING_PROFILES["INTRADAY_AGGRESSIVE"]["intraday"]["min_score_to_buy"] == 55
    assert config.PAPER_TRAINING_PROFILES["INTRADAY_AGGRESSIVE"]["intraday"]["min_relative_volume"] == 1.5
    assert intraday_momentum_engine.BUY_THRESHOLD == 55
    rules = strategy_mode.intraday_rules()
    assert rules["min_score_to_buy"] == 55
    assert rules["min_relative_volume"] == 1.5
    assert rules["max_slippage_estimate"] <= 1.5
    assert rules["max_spread_percent"] <= 2.5
    assert rules["allow_overnight"] is False
    assert config.IBKR_PAPER_TRADING is True
    assert config.IBKR_ENABLE_REAL_TRADING is False


def test_swing_exit_engine_never_emits_intraday_actions():
    swing = {"symbol": "SWNG", "strategy_type": "SWING", "buy_price": 100, "quantity": 10, "stop_loss": 90, "take_profit_1": 101, "take_profit_2": 102, "created_at": datetime.now(timezone.utc).isoformat()}
    update = position_manager.evaluate_position(swing, {"price": 101.5, "signal": "HOLD"}, mode="INTRADAY_MOMENTUM")
    assert update["action"] != "INTRADAY_MOVE_STOP_TO_BREAKEVEN"
    assert update["action"] != "INTRADAY_TAKE_PROFIT_FAST"
    assert not str(update["action"]).startswith("INTRADAY_")


def test_intraday_exit_engine_can_emit_intraday_actions(monkeypatch):
    monkeypatch.setattr(strategy_mode, "force_exit_before_close_status", lambda *a, **k: {"active": False})
    day = {"symbol": "DAY", "strategy_type": "INTRADAY", "buy_price": 100, "quantity": 10, "stop_loss": 98, "take_profit_1": 101.5, "take_profit_2": 104, "created_at": datetime.now(timezone.utc).isoformat()}
    update = position_manager.evaluate_position(day, {"price": 101.6, "signal": "HOLD"}, mode="SWING_DEFAULT")
    assert update["action"].startswith("INTRADAY_")
    assert update.get("exit_engine") == "intraday_exit"


def test_missing_strategy_type_defaults_to_swing_even_in_intraday_mode(monkeypatch):
    monkeypatch.setattr(strategy_mode, "force_exit_before_close_status", lambda *a, **k: {"active": True})
    update = position_manager.evaluate_position({"symbol": "NULL", "buy_price": 100, "quantity": 1, "stop_loss": 98, "take_profit_1": 101}, {"price": 101.5, "signal": "HOLD"}, mode="INTRADAY_MOMENTUM")
    assert not str(update["action"]).startswith("INTRADAY_")


def test_ranking_selects_top_five_and_rejects_lower_ranked_with_components():
    rows = [candidate(f"D{i}", 95 - i, "INTRADAY") for i in range(7)]
    result = rank_candidates(rows, "INTRADAY", top_n=5)
    assert len(result["selected"]) == 5
    assert len(result["rejected"]) == 2
    assert all(r["rejected_by_ranking"] for r in result["rejected"])
    assert set(result["selected"][0]["ranking_components"]) == set(RANKING_COMPONENTS)


def test_ranking_selects_max_five_swing():
    rows = [candidate(f"S{i}", 92 - i, "SWING") for i in range(8)]
    result = rank_candidates(rows, "SWING", top_n=5)
    assert len(result["selected"]) == 5
    assert all(r["strategy_type"] == "SWING" for r in result["selected"])


def test_profit_factor_zero_losses_safe(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "pf.db"))
    monkeypatch.setattr(database, "DB_PATH", str(tmp_path / "pf.db"))
    import asyncio
    async def run():
        await database.init_db()
        await database.add_position({"symbol": "WIN", "buy_price": 100, "quantity": 1, "profit_amount": 10, "strategy_type": "SWING"})
        await database.update_position("WIN", {"status": "CLOSED", "profit_amount": 10, "closed_at": datetime.now(timezone.utc).isoformat()})
        payload = await database.get_performance_by_strategy()
        assert set(["SWING", "INTRADAY", "TOTAL"]).issubset(payload)
        assert payload["SWING"]["profit_factor"] >= 0
    asyncio.run(run())

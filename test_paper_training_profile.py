from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

import config
import database
import main


def test_trading_status_exposes_effective_aggressive_profile(tmp_path, monkeypatch):
    original = {
        "DB_PATH": database.DB_PATH,
        "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
        "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
        "PAPER_TRAINING_PROFILE": config.PAPER_TRAINING_PROFILE,
        "TRADING_MODE": config.TRADING_MODE,
        "AUTO_SEND_ORDERS": config.AUTO_SEND_ORDERS,
    }

    async def watchdog_ok():
        return {
            "trading_blocked": False,
            "blocking_reasons": [],
            "circuit_breaker": {"tripped": False},
            "tws_connected": True,
            "shared_ib_connected": True,
            "heartbeat": "ok",
            "market_data_feed_active": True,
            "stale_data": {"blocked": False},
        }

    async def tracker_ok():
        return {"running": True}

    try:
        database.DB_PATH = str(tmp_path / "paper_training_status.db")
        asyncio.run(database.init_db())
        asyncio.run(database.set_app_state("strategy_mode", "INTRADAY_MOMENTUM"))
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.PAPER_TRAINING_PROFILE = "AGGRESSIVE_LEARNING"
        config.TRADING_MODE = "PAPER"
        config.AUTO_SEND_ORDERS = True
        monkeypatch.setattr(main, "get_market_regime", lambda: {"regime": "Bullish", "allow_new_buys": True, "position_size_factor": 1.0})
        monkeypatch.setattr(main.watchdog, "get_watchdog_status", watchdog_ok)
        monkeypatch.setattr(main.live_position_tracker, "get_tracker_status", tracker_ok)
        monkeypatch.setattr(main, "get_market_hours_status", lambda: {"enabled": True, "allowed": True, "reason": "US regular market is open"})

        payload = TestClient(main.app).get("/api/trading-status").json()

        assert payload["active_training_profile"] == "AGGRESSIVE_LEARNING"
        assert payload["account_balance"] == 500000.0
        assert payload["virtual_trading_capital"] == 500000.0
        assert payload["profile_rules"]["paper_capital"] == 500000.0
        assert payload["effective_max_positions"] == 8
        assert payload["effective_score_threshold"] == 78
        assert payload["effective_risk_factor"] == 0.5
        assert payload["effective_max_daily_trades"] == 15
    finally:
        for key, value in original.items():
            if key == "DB_PATH":
                database.DB_PATH = value
            else:
                setattr(config, key, value)

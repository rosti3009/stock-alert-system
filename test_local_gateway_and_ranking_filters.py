from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import auto_trader
import config
import database
import main
from ranking_engine import STRATEGY_INTRADAY, STRATEGY_SWING


def _run(coro):
    return asyncio.run(coro)


def _candidate(symbol: str, strategy: str, **overrides):
    row = {
        "symbol": symbol,
        "strategy_type": strategy,
        "signal": "BUY",
        "price": 25,
        "volume": 10_000_000,
        "avg_volume": 10_000_000,
        "entry_price": 25,
        "stop_loss": 23,
        "score": 90,
        "weekly_score": 90,
        "ranking_score": 90,
        "ranking_grade": "A",
        "rejected_by_ranking": False,
        "ranking_checked_at": datetime.now(timezone.utc).isoformat(),
    }
    row.update(overrides)
    return row


def test_skipped_candidate_cannot_appear_in_top_intraday(monkeypatch):
    async def fake_ranked(*_args, **_kwargs):
        return [_candidate("SKIP", STRATEGY_INTRADAY, signal="SKIPPED"), _candidate("BUY", STRATEGY_INTRADAY)]

    async def fake_latest(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main.database, "get_ranking_candidates", fake_ranked)
    monkeypatch.setattr(main.database, "get_latest_candidates", fake_latest)
    payload = TestClient(main.app).get("/api/ranking/top-intraday").json()
    assert [row["symbol"] for row in payload["candidates"]] == ["BUY"]


def test_price_below_minimum_cannot_appear_in_top_intraday(monkeypatch):
    async def fake_ranked(*_args, **_kwargs):
        return [_candidate("LOWP", STRATEGY_INTRADAY, price=config.MIN_PRICE - 0.01), _candidate("BUY", STRATEGY_INTRADAY)]

    async def fake_latest(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main.database, "get_ranking_candidates", fake_ranked)
    monkeypatch.setattr(main.database, "get_latest_candidates", fake_latest)
    payload = TestClient(main.app).get("/api/ranking/top-intraday").json()
    assert [row["symbol"] for row in payload["candidates"]] == ["BUY"]


def test_volume_below_minimum_cannot_appear_in_top_swing(monkeypatch):
    async def fake_ranked(*_args, **_kwargs):
        return [_candidate("LOWV", STRATEGY_SWING, volume=config.MIN_AVG_VOLUME - 1), _candidate("BUY", STRATEGY_SWING)]

    async def fake_latest(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main.database, "get_ranking_candidates", fake_ranked)
    monkeypatch.setattr(main.database, "get_latest_candidates", fake_latest)
    payload = TestClient(main.app).get("/api/ranking/top-swing").json()
    assert [row["symbol"] for row in payload["candidates"]] == ["BUY"]


def test_stale_broker_snapshot_blocks_auto_trading(monkeypatch):
    events = []
    opened = []

    async def stale_snapshot():
        return {"connected": 1, "synced_at": (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat(), "source": "LOCAL_GATEWAY_PUSH"}

    async def enabled(*_args, **_kwargs):
        return "true"

    async def settings():
        return {"auto_trader_enabled": True, "intraday_min_score": 50, "swing_min_score": 60}

    async def ok_state():
        return {"tripped": False}

    async def startup_ok():
        return True

    async def watchdog_ok():
        return {"trading_blocked": False, "blocking_reasons": []}

    async def record_event(payload):
        events.append(payload)

    async def fake_open(**kwargs):
        opened.append(kwargs)
        return True

    monkeypatch.setattr(auto_trader.database, "get_app_state", enabled)
    monkeypatch.setattr(auto_trader.database, "get_strategy_settings", settings)
    monkeypatch.setattr("circuit_breaker.get_circuit_breaker_state", ok_state)
    monkeypatch.setattr("startup_recovery.startup_recovery_passed", startup_ok)
    monkeypatch.setattr("watchdog.get_watchdog_status", watchdog_ok)
    monkeypatch.setattr(auto_trader.database, "get_latest_broker_sync_snapshot", stale_snapshot)
    monkeypatch.setattr(auto_trader.database, "safe_record_trade_journal_event", record_event)
    monkeypatch.setattr(auto_trader, "auto_open_position", fake_open)

    _run(auto_trader.process_auto_trading([{"symbol": "STALE", "signal": "BUY", "score": 99}]))

    assert not opened
    assert any(event.get("event_type") == "AUTO_TRADING_BLOCKED_BY_BROKER_SNAPSHOT" for event in events)


def test_pushed_broker_snapshot_becomes_source_of_truth(tmp_path, monkeypatch):
    original_db = database.DB_PATH
    original_main_db = main.database.DB_PATH
    original_token = config.BROKER_PUSH_TOKEN
    database.DB_PATH = str(tmp_path / "broker_push.db")
    main.database.DB_PATH = database.DB_PATH
    config.BROKER_PUSH_TOKEN = "secret-test-token"
    try:
        snapshot = {
            "ok": True,
            "connected": True,
            "account": "DU123",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "equity": {"net_liquidation": 12345, "total_cash": 2345, "available_funds": 2000, "buying_power": 4000},
            "positions": [{"symbol": "AAPL", "quantity": 2, "avg_cost": 100, "market_price": 110, "market_value": 220, "unrealized_pnl": 20}],
            "open_orders": [],
            "executions": [],
            "errors": [],
        }
        client = TestClient(main.app)
        pushed = client.post("/api/broker/push-snapshot", json=snapshot, headers={"X-Broker-Push-Token": "secret-test-token"}).json()
        assert pushed["ok"] is True
        truth = client.get("/api/broker/source-of-truth").json()
        assert truth["source"] == "LOCAL_GATEWAY_PUSH"
        assert truth["connected"] is True
        assert truth["metrics"]["broker_positions"] == 1
        assert truth["positions"][0]["symbol"] == "AAPL"
    finally:
        database.DB_PATH = original_db
        main.database.DB_PATH = original_main_db
        config.BROKER_PUSH_TOKEN = original_token


def test_debug_intraday_reports_filter_counts_and_reasons(monkeypatch):
    rows = [
        _candidate("GOOD", STRATEGY_INTRADAY, relative_volume=2.0, avg_volume=1_000_000, dollar_volume=10_000_000, ranking_score=90),
        _candidate("LOWRV", STRATEGY_INTRADAY, relative_volume=0.5, avg_volume=1_000_000, dollar_volume=10_000_000, ranking_score=90),
        _candidate("LOWDV", STRATEGY_INTRADAY, relative_volume=2.0, avg_volume=1_000_000, dollar_volume=100_000, ranking_score=90),
        _candidate("LOWSCORE", STRATEGY_INTRADAY, relative_volume=2.0, avg_volume=1_000_000, dollar_volume=10_000_000, ranking_score=40),
    ]

    async def fake_latest(*_args, **_kwargs):
        return rows

    async def fake_ranked(*_args, **_kwargs):
        return []

    async def fake_top(*_args, **_kwargs):
        return [rows[0]]

    monkeypatch.setattr(main.database, "get_latest_candidates", fake_latest)
    monkeypatch.setattr(main.database, "get_ranking_candidates", fake_ranked)
    monkeypatch.setattr(main, "_get_actionable_ranking_top", fake_top)
    payload = TestClient(main.app).get("/api/ranking/debug-intraday").json()

    assert payload["ok"] is True
    assert payload["scanned"] == 4
    assert payload["passed_volume"] == 3
    assert payload["passed_relative_volume"] == 2
    assert payload["passed_score"] == 1
    assert payload["passed_risk"] == 1
    assert payload["final_candidates"] == 1
    assert payload["top_rejections"]["low_relative_volume"] == 1
    assert payload["top_rejections"]["low_dollar_volume"] == 1
    assert payload["top_rejections"]["score_too_low"] == 1
    assert payload["thresholds"]["min_score"] == config.INTRADAY_MIN_SCORE_TO_BUY


def test_intraday_enrichment_generates_actionable_entry_stop_and_target():
    from ranking_engine import enrich_actionable_candidate

    row = _candidate(
        "ACT",
        STRATEGY_INTRADAY,
        signal="",
        entry_price=None,
        stop_loss=None,
        current_price=20,
        atr=0.4,
        relative_volume=1.2,
        avg_volume=500_000,
        dollar_volume=3_000_000,
        ranking_score=70,
    )

    enriched = enrich_actionable_candidate(row, STRATEGY_INTRADAY)

    assert enriched["buy_signal"] is True
    assert enriched["signal"] == "BUY"
    assert enriched["entry_price"] == 20
    assert enriched["stop_loss"] < enriched["entry_price"]
    assert enriched["take_profit"] > enriched["entry_price"]
    assert enriched["strategy_type"] == STRATEGY_INTRADAY


def test_top_intraday_uses_fallback_entry_and_stop(monkeypatch):
    async def fake_ranked(*_args, **_kwargs):
        return []

    async def fake_latest(*_args, **_kwargs):
        return [_candidate(
            "FALL",
            STRATEGY_INTRADAY,
            signal="",
            entry_price=None,
            stop_loss=None,
            current_price=30,
            atr_percent=2,
            relative_volume=1.1,
            avg_volume=600_000,
            dollar_volume=4_000_000,
            ranking_score=75,
        )]

    monkeypatch.setattr(main.database, "get_ranking_candidates", fake_ranked)
    monkeypatch.setattr(main.database, "get_latest_candidates", fake_latest)
    payload = TestClient(main.app).get("/api/ranking/top-intraday").json()

    assert [row["symbol"] for row in payload["candidates"]] == ["FALL"]
    assert payload["candidates"][0]["buy_signal"] is True
    assert payload["candidates"][0]["entry_price"] == 30
    assert payload["candidates"][0]["stop_loss"] > 0


def test_swing_top_is_independent_of_intraday_relative_volume(monkeypatch):
    async def fake_ranked(strategy_type, *_args, **_kwargs):
        if strategy_type == STRATEGY_SWING:
            return [_candidate(
                "SWNG",
                STRATEGY_SWING,
                relative_volume=0.1,
                avg_volume=700_000,
                dollar_volume=6_000_000,
                ranking_score=80,
            )]
        return []

    async def fake_latest(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main.database, "get_ranking_candidates", fake_ranked)
    monkeypatch.setattr(main.database, "get_latest_candidates", fake_latest)
    payload = TestClient(main.app).get("/api/ranking/top-swing").json()

    assert [row["symbol"] for row in payload["candidates"]] == ["SWNG"]
    assert payload["candidates"][0]["strategy_type"] == STRATEGY_SWING


def test_debug_risk_exposes_exact_reasons(monkeypatch):
    async def fake_top(*_args, **_kwargs):
        return [_candidate("RISK", STRATEGY_INTRADAY, entry_price=10, stop_loss=9, take_profit=11.5)]

    async def fake_positions():
        return []

    async def fake_portfolio_risk():
        return {"total_portfolio_exposure_percent": 0, "total_open_risk_percent": 0}

    monkeypatch.setattr(main, "_get_actionable_ranking_top", fake_top)
    monkeypatch.setattr(main.database, "get_open_positions", fake_positions)
    monkeypatch.setattr(main.portfolio_risk_engine, "get_portfolio_risk", fake_portfolio_risk)
    payload = TestClient(main.app).get("/api/ranking/debug-risk").json()

    row = payload["candidates"][0]
    assert {"symbol", "strategy_type", "entry_price", "stop_loss", "take_profit", "calculated_shares", "position_value", "risk_dollars", "risk_percent", "risk_reasons", "allowed"} <= set(row)
    assert row["symbol"] == "RISK"


def test_defensive_regime_limits_intraday_new_buys_to_top_one_to_three():
    assert auto_trader.intraday_buy_limit_for_regime("DEFENSIVE", 5) == 1
    assert auto_trader.intraday_buy_limit_for_regime("ELEVATED", 5) == 3
    assert auto_trader.intraday_buy_limit_for_regime("RISK_ON", 5) == 5

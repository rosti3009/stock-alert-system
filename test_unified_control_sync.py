from __future__ import annotations

import pytest

import database
from ranking_engine import STRATEGY_INTRADAY, STRATEGY_SWING, rank_candidates


@pytest.mark.anyio("asyncio")
async def test_strategy_settings_save_load_and_preset(tmp_path, monkeypatch):
    db_path = tmp_path / "settings.sqlite"
    monkeypatch.setattr(database, "DB_PATH", str(db_path))
    await database.init_db()

    saved = await database.save_strategy_settings({"max_open_positions": 0, "scanner_mode": "intraday", "enable_mean_reversion": True})
    assert saved["max_open_positions"] == 0
    assert saved["scanner_mode"] == "intraday"
    assert saved["enable_mean_reversion"] is True

    loaded = await database.get_strategy_settings()
    assert loaded["max_open_positions"] == 0
    assert loaded["enable_mean_reversion"] is True

    conservative = await database.apply_strategy_preset("conservative")
    assert conservative["risk_profile"] == "conservative"
    assert conservative["max_position_size_percent"] == 8


@pytest.mark.anyio("asyncio")
async def test_broker_source_of_truth_reconciles_positions(tmp_path, monkeypatch):
    db_path = tmp_path / "broker.sqlite"
    monkeypatch.setattr(database, "DB_PATH", str(db_path))
    await database.init_db()
    await database.add_position({"symbol": "OLD", "buy_price": 10, "quantity": 1}, enforce_max_open_positions=False)

    snapshot = {
        "ok": True,
        "connected": True,
        "synced_at": "2026-06-02T00:00:00+00:00",
        "positions": [{"symbol": "NEW", "quantity": 3, "avg_cost": 20, "market_price": 22, "market_value": 66, "unrealized_pnl": 6}],
    }
    result = await database.reconcile_broker_source_of_truth(snapshot)
    assert result["recovered"] == ["NEW"]
    assert result["missing_from_broker"] == ["OLD"]

    new_pos = await database.get_position("NEW")
    old_pos = await database.get_position("OLD")
    assert new_pos["source"] == "BROKER_SYNC"
    assert new_pos["position_truth_source"] == "IBKR"
    assert new_pos["broker_quantity"] == 3
    assert old_pos["status"] == "MISSING_FROM_BROKER"
    assert old_pos["sync_status"] == "MISSING_FROM_BROKER"


@pytest.mark.anyio("asyncio")
async def test_max_open_positions_zero_means_no_count_limit(tmp_path, monkeypatch):
    db_path = tmp_path / "limits.sqlite"
    monkeypatch.setattr(database, "DB_PATH", str(db_path))
    await database.init_db()
    await database.add_position({"symbol": "AAA", "buy_price": 10, "quantity": 1}, max_open_positions=0, enforce_max_open_positions=True)
    await database.add_position({"symbol": "BBB", "buy_price": 11, "quantity": 1}, max_open_positions=0, enforce_max_open_positions=True)
    assert await database.count_open_positions() == 2


def test_ranking_returns_top_five_intraday_and_swing():
    rows = [{"symbol": f"I{i}", "score": 80 + i, "signal": "BUY", "relative_volume": 3, "dollar_volume": 20_000_000} for i in range(8)]
    intraday = rank_candidates(rows, STRATEGY_INTRADAY, top_n=5, minimum_score=0)
    swing = rank_candidates(rows, STRATEGY_SWING, top_n=5, minimum_score=0)
    assert len(intraday["selected"]) == 5
    assert len(swing["selected"]) == 5
    assert all(item["strategy_type"] == STRATEGY_INTRADAY for item in intraday["selected"])
    assert all(item["strategy_type"] == STRATEGY_SWING for item in swing["selected"])

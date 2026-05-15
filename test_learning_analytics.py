from __future__ import annotations

import asyncio
import os
import tempfile
from contextlib import contextmanager
from unittest.mock import patch

from fastapi.testclient import TestClient

import auto_trader
import database
import main


@contextmanager
def temp_database():
    original_db_path = database.DB_PATH
    fd, path = tempfile.mkstemp(prefix="learning_analytics_", suffix=".db")
    os.close(fd)
    database.DB_PATH = path
    try:
        yield path
    finally:
        database.DB_PATH = original_db_path
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_rejected_setups_are_recorded():
    async def run():
        with temp_database():
            await database.init_db()
            await database.record_rejected_setup({
                "symbol": "aapl",
                "strategy_mode": "INTRADAY_TECHNICAL",
                "rejection_reason": "Score too low (72 < 80)",
                "score": 72,
                "volume": 2000,
                "avg_volume": 1000,
                "vwap_status": "ABOVE_VWAP",
                "momentum_score": 72,
                "spread_percent": 0.08,
                "slippage_estimate": 0.01,
                "market_regime": "Bullish",
                "sector": "Technology",
            })
            return await database.get_rejected_setups()

    rows = asyncio.run(run())
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["failed_filter"] == "score"
    assert rows[0]["rvol"] == 2
    assert rows[0]["time_of_day"]


def test_accepted_setups_are_recorded():
    async def run():
        with temp_database():
            await database.init_db()
            await database.record_trade_decision({
                "symbol": "msft",
                "setup_type": "BREAKOUT",
                "entry_reason": "BUY candidate passed auto-trading filters",
                "score": 91,
                "volume": 3000,
                "avg_volume": 1000,
                "vwap_status": "ABOVE_VWAP",
                "breakout_status": "CONFIRMED",
                "momentum_score": 91,
                "market_regime": "Bullish",
                "sector": "Technology",
                "entry_price": 420.25,
            })
            async with database.aiosqlite.connect(database.DB_PATH) as db:
                db.row_factory = database.aiosqlite.Row
                async with db.execute("SELECT * FROM trade_decisions") as cursor:
                    return await cursor.fetchall()

    rows = asyncio.run(run())
    assert len(rows) == 1
    row = dict(rows[0])
    assert row["symbol"] == "MSFT"
    assert row["setup_type"] == "BREAKOUT"
    assert row["entry_price"] == 420.25


def test_exits_create_outcomes_and_setup_performance_aggregates():
    async def run():
        with temp_database():
            await database.init_db()
            await database.record_trade_decision({
                "symbol": "nvda",
                "setup_type": "BREAKOUT",
                "entry_time": "2026-05-15T14:30:00+00:00",
                "entry_price": 100,
                "rvol": 2.4,
                "market_regime": "Bullish",
                "sector": "Technology",
            })
            await database.record_trade_outcome({
                "symbol": "nvda",
                "exit_time": "2026-05-15T15:00:00+00:00",
                "current_price": 103,
                "exit_reason": "TAKE_PROFIT_1",
                "profit_amount": 30,
                "profit_percent": 3,
            })
            await database.record_trade_decision({
                "symbol": "tsla",
                "setup_type": "REVERSAL",
                "entry_time": "2026-05-15T16:00:00+00:00",
                "entry_price": 200,
            })
            await database.record_trade_outcome({
                "symbol": "tsla",
                "exit_time": "2026-05-15T16:10:00+00:00",
                "current_price": 198,
                "exit_reason": "STOP_LOSS_HIT",
                "profit_amount": -20,
                "profit_percent": -1,
            })

            outcomes = await database.get_trade_outcomes()
            performance = await database.refresh_setup_performance()
            summary = await database.get_learning_summary()
            return outcomes, performance, summary

    outcomes, performance, summary = asyncio.run(run())
    assert len(outcomes) == 2
    breakout = next(row for row in performance if row["setup_type"] == "BREAKOUT")
    assert breakout["total_trades"] == 1
    assert breakout["wins"] == 1
    assert breakout["win_rate"] == 100
    assert summary["informational_only"] is True
    assert summary["breakout_vs_reversal_performance"]
    assert summary["most_common_loss_reasons"][0]["exit_reason"] == "STOP_LOSS_HIT"


def test_analytics_endpoints_return_expected_fields():
    with temp_database():
        asyncio.run(database.init_db())
        asyncio.run(database.record_rejected_setup({"symbol": "AAPL", "rejection_reason": "Score too low", "score": 60}))
        asyncio.run(database.record_trade_decision({"symbol": "AAPL", "setup_type": "BREAKOUT", "entry_price": 10}))
        asyncio.run(database.record_trade_outcome({"symbol": "AAPL", "exit_reason": "TAKE_PROFIT_1", "profit_amount": 5, "profit_percent": 5}))

        client = TestClient(main.app)
        rejections = client.get("/api/analytics/rejections").json()
        setup_perf = client.get("/api/analytics/setup-performance").json()
        outcomes = client.get("/api/analytics/outcomes").json()
        summary = client.get("/api/analytics/learning-summary").json()

    assert {"symbol", "rejection_reason", "failed_filter", "rvol", "time_of_day"} <= set(rejections[0])
    assert {"setup_type", "win_rate", "avg_profit_amount"} <= set(setup_perf[0])
    assert {"symbol", "exit_reason", "profit_amount", "hold_minutes"} <= set(outcomes[0])
    assert {"win_rate_by_setup_type", "best_rvol_ranges", "market_regime_performance", "most_common_loss_reasons"} <= set(summary)


def test_learning_layer_does_not_affect_order_execution():
    async def run():
        with temp_database():
            await database.init_db()
            order_calls = []

            async def fake_open_position(**kwargs):
                order_calls.append(kwargs)
                return False

            with patch.object(auto_trader, "auto_open_position", side_effect=fake_open_position):
                await auto_trader.process_auto_trading([{
                    "symbol": "AAPL",
                    "signal": "BUY",
                    "score": 99,
                    "weekly_score": 99,
                    "price": 100,
                    "volume": 2000,
                    "avg_volume": 1000,
                    "trend": "Bullish",
                }])

            decisions = await database.get_trade_journal()
            return order_calls, decisions

    order_calls, decisions = asyncio.run(run())
    assert len(order_calls) <= 1
    assert all(row["event_type"] != "ORDER_PLACED_BY_ANALYTICS" for row in decisions)

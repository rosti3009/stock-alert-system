from __future__ import annotations

import asyncio
import gc
import json
import os
import tempfile
import time
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone

import config
import database
import position_exit_priority_engine as engine


class PositionExitPriorityEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        self.original_capital = config.VIRTUAL_TRADING_CAPITAL_USD
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.VIRTUAL_TRADING_CAPITAL_USD = 5000.0
        asyncio.run(database.init_db())
        asyncio.run(database.set_app_state(
            "market_regime_engine_latest",
            json.dumps({"regime": "DEFENSIVE", "risk_level": "ELEVATED"}),
        ))

    def tearDown(self):
        config.DB_PATH = self.original_db_path
        database.DB_PATH = self.original_database_db_path
        config.VIRTUAL_TRADING_CAPITAL_USD = self.original_capital
        gc.collect()
        for attempt in range(5):
            try:
                os.unlink(self.tmp.name)
                break
            except FileNotFoundError:
                break
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.1)

    def test_evaluates_all_requested_factors_and_ranks_exit_candidate(self):
        stale_date = (datetime.now(timezone.utc) - timedelta(days=55)).isoformat()
        positions = [
            {
                "symbol": "AAPL",
                "quantity": 15,
                "buy_price": 100,
                "current_price": 80,
                "profit_amount": -300,
                "profit_percent": -20,
                "stop_loss": 60,
                "status": "OPEN",
                "buy_date": stale_date,
            },
            {
                "symbol": "JPM",
                "quantity": 2,
                "buy_price": 100,
                "current_price": 105,
                "profit_amount": 10,
                "profit_percent": 5,
                "stop_loss": 95,
                "status": "OPEN",
                "buy_date": stale_date,
            },
        ]
        rows = {
            "AAPL": {
                "symbol": "AAPL",
                "price": 80,
                "ma20": 90,
                "ma50": 95,
                "ma200": 105,
                "trend": "Bearish",
                "momentum_5d": -8,
                "momentum_20d": -18,
                "momentum_60d": -25,
                "volume": 100000,
                "avg_volume": 600000,
                "volume_ratio": 0.17,
                "atr_percent": 11,
                "bid": 78,
                "ask": 82,
                "average_spread_percent": 0.5,
                "gap_percent": -6,
            },
            "JPM": {
                "symbol": "JPM",
                "price": 105,
                "ma20": 101,
                "ma50": 99,
                "trend": "Bullish",
                "momentum_5d": 2,
                "momentum_20d": 4,
                "volume": 900000,
                "avg_volume": 800000,
                "volume_ratio": 1.12,
                "atr_percent": 2,
            },
        }

        snapshot = engine.evaluate_exit_priorities(
            positions,
            rows,
            account_equity=5000.0,
            market_regime={"regime": "DEFENSIVE", "risk_level": "ELEVATED"},
            checked_at="2026-05-12T00:00:00+00:00",
        )

        self.assertTrue(snapshot["read_only"])
        self.assertTrue(snapshot["recommendation_only"])
        self.assertTrue(snapshot["no_order_actions"])
        self.assertEqual(snapshot["worst_positions"][0]["symbol"], "AAPL")
        self.assertEqual(snapshot["worst_positions"][0]["priority_state"], "EXIT_CANDIDATE")
        self.assertGreaterEqual(snapshot["worst_positions"][0]["exit_priority_score"], 80)
        self.assertGreater(snapshot["capital_trapped"], 0)
        self.assertEqual(snapshot["priority_counts"]["EXIT_CANDIDATE"], 1)

        component_names = {item["name"] for item in snapshot["worst_positions"][0]["components"]}
        self.assertEqual(component_names, {
            "unrealized_pnl",
            "relative_weakness_vs_market",
            "momentum_deterioration",
            "execution_quality_deterioration",
            "liquidity_deterioration",
            "spread_deterioration",
            "volume_collapse",
            "sector_overconcentration",
            "position_size_vs_virtual_capital",
            "atr_volatility_expansion",
            "drawdown_contribution",
            "market_regime_compatibility",
            "time_held_stale_position",
            "gap_overnight_risk",
            "portfolio_risk_contribution",
            "correlation_clustering",
        })
        self.assertIn("MOMENTUM_DETERIORATION", snapshot["worst_positions"][0]["risk_flags"])
        self.assertIn("AAPL", [item["symbol"] for item in snapshot["weakest_momentum_positions"]])
        self.assertIn("AAPL", [item["symbol"] for item in snapshot["highest_risk_positions"]])

    def test_keep_state_for_healthy_low_risk_position(self):
        snapshot = engine.evaluate_exit_priorities(
            [{"symbol": "JPM", "quantity": 2, "buy_price": 100, "current_price": 110, "profit_percent": 10, "profit_amount": 20, "stop_loss": 100, "status": "OPEN"}],
            {"JPM": {"symbol": "JPM", "price": 110, "ma20": 105, "ma50": 100, "trend": "Bullish", "momentum_5d": 2, "momentum_20d": 6, "volume": 1000000, "avg_volume": 900000, "volume_ratio": 1.1, "atr_percent": 2}},
            account_equity=5000.0,
            market_regime={"regime": "BULL", "risk_level": "LOW"},
        )

        self.assertEqual(snapshot["positions"][0]["priority_state"], "KEEP")
        self.assertLess(snapshot["positions"][0]["exit_priority_score"], 25)
        self.assertEqual(snapshot["capital_trapped"], 0)

    def test_async_api_snapshot_records_review_and_recovery_journal_events(self):
        stale_date = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        asyncio.run(database.add_position({
            "symbol": "AAPL",
            "buy_price": 100,
            "current_price": 80,
            "quantity": 15,
            "profit_amount": -300,
            "profit_percent": -20,
            "stop_loss": 60,
            "buy_date": stale_date,
        }))
        rows = {
            "AAPL": {
                "symbol": "AAPL",
                "price": 80,
                "ma20": 90,
                "ma50": 95,
                "trend": "Bearish",
                "momentum_5d": -8,
                "momentum_20d": -18,
                "volume": 100000,
                "avg_volume": 600000,
                "volume_ratio": 0.17,
                "atr_percent": 11,
                "bid": 78,
                "ask": 82,
            }
        }

        first = asyncio.run(engine.get_position_exit_priority(rows))
        self.assertEqual(first["positions"][0]["priority_state"], "EXIT_CANDIDATE")

        asyncio.run(database.update_position("AAPL", {"current_price": 115, "profit_amount": 225, "profit_percent": 15, "stop_loss": 105}))
        recovered = asyncio.run(engine.get_position_exit_priority({
            "AAPL": {
                "symbol": "AAPL",
                "price": 115,
                "ma20": 110,
                "ma50": 105,
                "trend": "Bullish",
                "momentum_5d": 3,
                "momentum_20d": 8,
                "volume": 1000000,
                "avg_volume": 900000,
                "volume_ratio": 1.1,
                "atr_percent": 2,
            }
        }))
        self.assertIn(recovered["positions"][0]["priority_state"], {"KEEP", "REVIEW"})

        rows = asyncio.run(database.get_trade_journal(limit=20))
        event_types = {row["event_type"] for row in rows}
        self.assertIn("POSITION_EXIT_CANDIDATE", event_types)
        self.assertIn("POSITION_RECOVERED", event_types)

        with closing(__import__("sqlite3").connect(self.tmp.name)) as db:
            order_count = db.execute("SELECT COUNT(*) FROM open_orders").fetchone()[0]
        self.assertEqual(order_count, 0)


if __name__ == "__main__":
    unittest.main()

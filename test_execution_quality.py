from __future__ import annotations

import asyncio
import gc
import os
import tempfile
import time
import unittest
asyncio.set_event_loop(asyncio.new_event_loop())
from unittest.mock import patch

import config
import database
import execution_quality
import auto_trader


class ExecutionQualityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        self.original_thresholds = {
            "MAX_SPREAD_PERCENT": config.MAX_SPREAD_PERCENT,
            "MAX_SPREAD_DOLLARS": config.MAX_SPREAD_DOLLARS,
            "MIN_RELATIVE_VOLUME": config.MIN_RELATIVE_VOLUME,
            "MIN_AVERAGE_VOLUME": config.MIN_AVERAGE_VOLUME,
            "MAX_SLIPPAGE_ESTIMATE": config.MAX_SLIPPAGE_ESTIMATE,
            "MAX_INTRADAY_VOLATILITY": config.MAX_INTRADAY_VOLATILITY,
            "MAX_CANDLE_EXPANSION_PERCENT": config.MAX_CANDLE_EXPANSION_PERCENT,
            "AUTO_SEND_ORDERS": config.AUTO_SEND_ORDERS,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.MAX_SPREAD_PERCENT = 1.0
        config.MAX_SPREAD_DOLLARS = 0.25
        config.MIN_RELATIVE_VOLUME = 0.75
        config.MIN_AVERAGE_VOLUME = 500000.0
        config.MAX_SLIPPAGE_ESTIMATE = 1.0
        config.MAX_INTRADAY_VOLATILITY = 6.0
        config.MAX_CANDLE_EXPANSION_PERCENT = 250.0
        asyncio.run(database.init_db())

    def tearDown(self):
        config.DB_PATH = self.original_db_path
        database.DB_PATH = self.original_database_db_path
        for key, value in self.original_thresholds.items():
            setattr(config, key, value)
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

    def test_safe_execution_quality_allows_buy(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "AAPL", "avg_volume": 2_000_000, "volume_ratio": 1.2, "atr_percent": 1.5},
            quote={"symbol": "AAPL", "bid": 100.00, "ask": 100.04, "last": 100.02},
            limit_price=100.0,
        )

        self.assertEqual(result["state"], "EXECUTION_SAFE")
        self.assertTrue(result["allowed"])
        self.assertFalse(result["blocks_buy"])
        self.assertAlmostEqual(result["metrics"]["spread_percent"], 0.04, places=2)

    def test_blocks_buy_for_dangerous_spread(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "WIDE", "avg_volume": 2_000_000, "volume_ratio": 1.2},
            quote={"symbol": "WIDE", "bid": 10.0, "ask": 10.4, "last": 10.2},
            limit_price=10.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertTrue(result["blocks_buy"])
        self.assertIn("dangerous_spread", result["block_categories"])

    def test_blocks_buy_for_low_liquidity(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "THIN", "avg_volume": 100_000, "volume_ratio": 0.3, "price": 20.0},
            limit_price=20.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertIn("low_liquidity", result["block_categories"])
        self.assertIn("Low average volume", result["blocked_buy_reason"])

    def test_blocks_buy_for_halt_risk_quote(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "HALT", "avg_volume": 2_000_000, "volume_ratio": 1.1},
            quote={"symbol": "HALT", "bid": 0, "ask": 0, "last": 15.0},
            limit_price=15.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertIn("halt_risk", result["block_categories"])

    def test_volatility_danger_does_not_block_buy(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "FAST", "avg_volume": 2_000_000, "volume_ratio": 1.2, "atr_percent": 8.0, "price": 50.0},
            limit_price=50.0,
        )

        self.assertEqual(result["state"], "EXECUTION_DANGER")
        self.assertTrue(result["allowed"])
        self.assertFalse(result["blocks_buy"])

    def test_summary_returns_worst_state(self):
        safe = execution_quality.evaluate_execution_quality(
            row={"symbol": "SAFE", "avg_volume": 2_000_000, "volume_ratio": 1.2, "price": 100},
            limit_price=100,
        )
        blocked = execution_quality.evaluate_execution_quality(
            row={"symbol": "THIN", "avg_volume": 10_000, "volume_ratio": 0.2, "price": 5},
            limit_price=5,
        )
        summary = execution_quality.summarize_execution_quality([safe, blocked])

        self.assertEqual(summary["state"], "EXECUTION_BLOCK_BUY")
        self.assertEqual(summary["liquidity_status"], "LOW")

    def test_auto_open_position_journals_execution_block_without_submitting_order(self):
        config.AUTO_SEND_ORDERS = True
        row = {
            "symbol": "THIN",
            "signal": "BUY",
            "price": 20.0,
            "entry_price": 20.0,
            "stop_loss": 18.0,
            "avg_volume": 100_000,
            "volume_ratio": 0.2,
        }

        with patch.object(auto_trader.portfolio_risk_engine, "require_new_buy_allowed") as risk_check, \
             patch.object(auto_trader, "execute_limit_buy_sync") as execute_buy:
            opened = asyncio.run(auto_trader.auto_open_position(
                row=row,
                open_positions=[],
                account_equity=10_000,
                market={"regime": "TEST"},
            ))

        self.assertFalse(opened)
        risk_check.assert_not_called()
        execute_buy.assert_not_called()
        rows = asyncio.run(database.get_trade_journal(limit=10, symbol="THIN"))
        self.assertTrue(any(row["event_type"] == "EXECUTION_BLOCK_BUY" for row in rows))

    def test_long_execution_reason_list_is_summarized_and_raw_preserved(self):
        evaluations = []
        for index in range(10):
            evaluations.append({
                "symbol": f"TH{index}",
                "state": "EXECUTION_BLOCK_BUY",
                "blocked_buy_reason": "Low average volume 10000 below minimum 500000",
                "block_categories": ["low_liquidity"],
                "metrics": {},
                "warnings": [],
            })
        for index in range(3):
            evaluations.append({
                "symbol": f"WD{index}",
                "state": "EXECUTION_BLOCK_BUY",
                "blocked_buy_reason": "Dangerous spread 4.00% exceeds max 1.00%",
                "block_categories": ["dangerous_spread"],
                "metrics": {},
                "warnings": [],
            })

        summary = execution_quality.summarize_execution_quality(evaluations)

        self.assertLessEqual(len(summary["blocked_buy_reason"]), 250)
        self.assertIn("Low liquidity: 10 symbols", summary["blocked_buy_reason"])
        self.assertIn("Spread risk: 3 symbols", summary["blocked_buy_reason"])
        self.assertEqual(len(summary["raw_blocked_buy_reasons"]), 13)
        self.assertEqual(summary["full_details"]["blocked_buy_reasons"], summary["raw_blocked_buy_reasons"])

    def test_duplicate_execution_reasons_are_grouped(self):
        evaluations = [
            {"symbol": "AAA", "state": "EXECUTION_BLOCK_BUY", "blocked_buy_reason": "Low relative volume 0.20x", "block_categories": ["low_liquidity"], "metrics": {}, "warnings": []},
            {"symbol": "BBB", "state": "EXECUTION_BLOCK_BUY", "blocked_buy_reason": "Low relative volume 0.20x", "block_categories": ["low_liquidity"], "metrics": {}, "warnings": []},
        ]

        summary = execution_quality.summarize_execution_quality(evaluations)

        self.assertIn("Low liquidity: 2 symbols", summary["blocked_buy_reason"])
        self.assertEqual(summary["blocked_buy_reason_summary"]["top_categories"][0]["symbol_count"], 2)


if __name__ == "__main__":
    unittest.main()

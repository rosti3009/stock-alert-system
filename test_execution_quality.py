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
            "MIN_DOLLAR_VOLUME": config.MIN_DOLLAR_VOLUME,
            "MAX_SLIPPAGE_ESTIMATE": config.MAX_SLIPPAGE_ESTIMATE,
            "MAX_INTRADAY_VOLATILITY": config.MAX_INTRADAY_VOLATILITY,
            "MAX_CANDLE_EXPANSION_PERCENT": config.MAX_CANDLE_EXPANSION_PERCENT,
            "AUTO_SEND_ORDERS": config.AUTO_SEND_ORDERS,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.MAX_SPREAD_PERCENT = 3.0
        config.MAX_SPREAD_DOLLARS = 0.50
        config.MIN_AVERAGE_VOLUME = 500000.0
        config.MIN_DOLLAR_VOLUME = 5_000_000.0
        config.MIN_RELATIVE_VOLUME = 1.0
        config.MAX_SLIPPAGE_ESTIMATE = 2.0
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


    def test_low_price_momentum_stock_uses_wider_slippage_tier(self):
        with self.assertLogs(execution_quality.log, level="INFO") as captured:
            result = execution_quality.evaluate_execution_quality(
                row={
                    "symbol": "MOMO",
                    "avg_volume": 2_000_000,
                    "volume_ratio": 2.4,
                    "estimated_slippage_percent": 2.4,
                },
                quote={"symbol": "MOMO", "bid": 7.99, "ask": 8.01, "last": 8.00},
                limit_price=8.0,
            )

        self.assertEqual(result["state"], "EXECUTION_SAFE")
        self.assertTrue(result["allowed"])
        self.assertEqual(result["metrics"]["slippage_price_tier"], "under_10")
        self.assertEqual(result["metrics"]["max_slippage_estimate"], 2.5)
        self.assertEqual(result["metrics"]["execution_decision"], "allow")
        self.assertIn("estimated_slippage=2.4", captured.output[0])
        self.assertIn("threshold=2.5", captured.output[0])
        self.assertIn("price_tier=under_10", captured.output[0])
        self.assertIn("decision=allow", captured.output[0])

    def test_mid_price_stock_blocks_above_mid_slippage_tier(self):
        result = execution_quality.evaluate_execution_quality(
            row={
                "symbol": "MID",
                "avg_volume": 2_000_000,
                "volume_ratio": 1.4,
                "estimated_slippage_percent": 1.6,
            },
            quote={"symbol": "MID", "bid": 19.99, "ask": 20.01, "last": 20.00},
            limit_price=20.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertFalse(result["allowed"])
        self.assertEqual(result["metrics"]["slippage_price_tier"], "10_to_50")
        self.assertEqual(result["metrics"]["max_slippage_estimate"], 1.5)
        self.assertEqual(result["metrics"]["execution_decision"], "block")
        self.assertIn("extreme_slippage", result["block_categories"])

    def test_high_price_stock_blocks_above_high_slippage_tier(self):
        result = execution_quality.evaluate_execution_quality(
            row={
                "symbol": "HIGH",
                "avg_volume": 2_000_000,
                "volume_ratio": 1.3,
                "estimated_slippage_percent": 1.01,
            },
            quote={"symbol": "HIGH", "bid": 99.99, "ask": 100.01, "last": 100.00},
            limit_price=100.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertEqual(result["metrics"]["slippage_price_tier"], "above_50")
        self.assertEqual(result["metrics"]["max_slippage_estimate"], 1.0)
        self.assertIn("extreme_slippage", result["block_categories"])

    def test_slippage_threshold_boundary_allows_equal_and_blocks_above(self):
        at_threshold = execution_quality.evaluate_execution_quality(
            row={
                "symbol": "BNDY",
                "avg_volume": 2_000_000,
                "volume_ratio": 1.3,
                "estimated_slippage_percent": 1.5,
            },
            quote={"symbol": "BNDY", "bid": 24.99, "ask": 25.01, "last": 25.00},
            limit_price=25.0,
        )
        above_threshold = execution_quality.evaluate_execution_quality(
            row={
                "symbol": "BNDY",
                "avg_volume": 2_000_000,
                "volume_ratio": 1.3,
                "estimated_slippage_percent": 1.5001,
            },
            quote={"symbol": "BNDY", "bid": 24.99, "ask": 25.01, "last": 25.00},
            limit_price=25.0,
        )

        self.assertEqual(at_threshold["state"], "EXECUTION_SAFE")
        self.assertTrue(at_threshold["allowed"])
        self.assertEqual(at_threshold["metrics"]["max_slippage_estimate"], 1.5)
        self.assertEqual(above_threshold["state"], "EXECUTION_BLOCK_BUY")
        self.assertFalse(above_threshold["allowed"])
        self.assertIn("extreme_slippage", above_threshold["block_categories"])

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
        self.assertIn("Low dollar volume", result["blocked_buy_reason"])
        self.assertIn("Low relative volume", result["blocked_buy_reason"])
        self.assertEqual(result["metrics"]["dollar_volume"], 2_000_000.0)
        self.assertIn("Low average volume", "; ".join(result["metrics"]["liquidity_block_reasons"]))

    def test_missing_average_volume_blocks_buy(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "MISS", "volume_ratio": 1.5, "price": 20.0},
            limit_price=20.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertFalse(result["allowed"])
        self.assertIn("low_liquidity", result["block_categories"])
        self.assertIn("Missing average volume", result["blocked_buy_reason"])
        self.assertIsNone(result["metrics"]["average_volume"])
        self.assertIsNone(result["metrics"]["dollar_volume"])
        self.assertEqual(result["liquidity"]["decision"], "block")
        self.assertIn("Missing average volume", result["liquidity"]["block_reasons"])

    def test_avg_volume_alias_and_dollar_volume_fallback_work(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "ALIAS", "avg_volume": 800_000, "price": 12.0, "relative_volume": 1.8},
            limit_price=12.0,
        )
        self.assertEqual(result["state"], "EXECUTION_SAFE")
        self.assertTrue(result["allowed"])
        self.assertEqual(result["metrics"]["average_volume"], 800_000.0)
        self.assertEqual(result["metrics"]["dollar_volume"], 9_600_000.0)
        self.assertIn("EXECUTION_VOLUME_FALLBACK_USED", result["journal_events"])
        self.assertIn("EXECUTION_DOLLAR_VOLUME_COMPUTED", result["journal_events"])

    def test_missing_price_still_blocks_even_with_avg_volume_alias(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "NOPRICE", "avg_volume": 800_000, "relative_volume": 1.8},
            limit_price=0.0,
        )
        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertIn("Missing dollar volume", result["blocked_buy_reason"])

    def test_blocks_buy_when_dollar_volume_below_threshold(self):
        result = execution_quality.evaluate_execution_quality(
            row={"symbol": "CHEAP", "average_volume": 600_000, "relative_volume": 1.2, "current_price": 5.0},
            limit_price=5.0,
        )

        self.assertEqual(result["state"], "EXECUTION_BLOCK_BUY")
        self.assertIn("Low dollar volume", result["blocked_buy_reason"])
        self.assertEqual(result["metrics"]["average_volume"], 600_000.0)
        self.assertEqual(result["metrics"]["current_price"], 5.0)
        self.assertEqual(result["metrics"]["dollar_volume"], 3_000_000.0)
        self.assertEqual(result["liquidity"]["dollar_volume"], 3_000_000.0)

    def test_relative_volume_only_blocks_when_available(self):
        missing_relative = execution_quality.evaluate_execution_quality(
            row={"symbol": "NOREL", "average_volume": 600_000, "current_price": 10.0},
            limit_price=10.0,
        )
        low_relative = execution_quality.evaluate_execution_quality(
            row={"symbol": "LOWREL", "average_volume": 600_000, "relative_volume": 0.99, "current_price": 10.0},
            limit_price=10.0,
        )

        self.assertEqual(missing_relative["state"], "EXECUTION_SAFE")
        self.assertTrue(missing_relative["allowed"])
        self.assertEqual(low_relative["state"], "EXECUTION_BLOCK_BUY")
        self.assertIn("Low relative volume", low_relative["blocked_buy_reason"])

    def test_liquidity_block_reason_is_logged(self):
        with self.assertLogs(execution_quality.log, level="INFO") as captured:
            execution_quality.evaluate_execution_quality(
                row={"symbol": "LOG", "average_volume": 100_000, "relative_volume": 0.5, "current_price": 10.0},
                limit_price=10.0,
            )

        liquidity_logs = [line for line in captured.output if "liquidity decision" in line]
        self.assertTrue(liquidity_logs)
        self.assertIn("decision=block", liquidity_logs[0])
        self.assertIn("Low average volume", liquidity_logs[0])

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

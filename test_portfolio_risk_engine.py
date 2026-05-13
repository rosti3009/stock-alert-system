from __future__ import annotations

from contextlib import closing
import asyncio
import gc
import os
import sqlite3
import tempfile
import time
import unittest

import config
import database
import portfolio_risk_engine


class PortfolioRiskEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        self.original_thresholds = {
            "MAX_TOTAL_EXPOSURE_PERCENT": config.MAX_TOTAL_EXPOSURE_PERCENT,
            "MAX_SYMBOL_EXPOSURE_PERCENT": config.MAX_SYMBOL_EXPOSURE_PERCENT,
            "MAX_SECTOR_EXPOSURE_PERCENT": config.MAX_SECTOR_EXPOSURE_PERCENT,
            "MAX_DAILY_DRAWDOWN_PERCENT": config.MAX_DAILY_DRAWDOWN_PERCENT,
            "MAX_ACCOUNT_UTILIZATION_PERCENT": config.MAX_ACCOUNT_UTILIZATION_PERCENT,
            "ACCOUNT_BALANCE": config.ACCOUNT_BALANCE,
            "VIRTUAL_TRADING_CAPITAL_USD": config.VIRTUAL_TRADING_CAPITAL_USD,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.ACCOUNT_BALANCE = 10000.0
        config.VIRTUAL_TRADING_CAPITAL_USD = 5000.0
        config.MAX_TOTAL_EXPOSURE_PERCENT = 80.0
        config.MAX_SYMBOL_EXPOSURE_PERCENT = 25.0
        config.MAX_SECTOR_EXPOSURE_PERCENT = 45.0
        config.MAX_DAILY_DRAWDOWN_PERCENT = 5.0
        config.MAX_ACCOUNT_UTILIZATION_PERCENT = 90.0
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

    def test_safe_snapshot_calculates_exposures(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[
                {"symbol": "AAPL", "quantity": 5, "buy_price": 100, "current_price": 110, "stop_loss": 95},
                {"symbol": "JPM", "quantity": 2.5, "buy_price": 100, "current_price": 100, "stop_loss": 90},
            ],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
            daily_realized_pnl=100,
            checked_at="2026-05-12T00:00:00+00:00",
        )

        self.assertEqual(snapshot["risk_state"], "SAFE")
        self.assertEqual(snapshot["new_buy_risk_status"], "ALLOWED")
        self.assertEqual(snapshot["account_equity"], 5000.0)
        self.assertEqual(snapshot["effective_equity"], 5000.0)
        self.assertEqual(snapshot["virtual_trading_capital"], 5000.0)
        self.assertAlmostEqual(snapshot["broker_net_liquidation"], 10000.0)
        self.assertAlmostEqual(snapshot["total_portfolio_exposure_percent"], 16.0)
        self.assertAlmostEqual(snapshot["largest_position_percent"], 11.0)
        self.assertEqual(snapshot["largest_position"]["symbol"], "AAPL")
        self.assertEqual(snapshot["total_open_risk"], 100.0)
        self.assertEqual(snapshot["daily_realized_pnl_percent"], 2.0)

    def test_blocks_new_buys_for_symbol_concentration(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[{"symbol": "AAPL", "quantity": 15, "buy_price": 100, "current_price": 100, "stop_loss": 90}],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
        )

        self.assertEqual(snapshot["risk_state"], "BLOCK_NEW_BUYS")
        self.assertTrue(snapshot["blocks_new_buys"])
        self.assertTrue(any(alert["metric"] == "largest_position_percent" for alert in snapshot["alerts"]))

    def test_blocks_new_buys_for_daily_drawdown(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
            daily_realized_pnl=-600,
        )

        self.assertEqual(snapshot["risk_state"], "BLOCK_NEW_BUYS")
        self.assertTrue(snapshot["blocks_new_buys"])
        self.assertAlmostEqual(snapshot["daily_drawdown_percent"], 12.0)

    def test_sector_concentration_warns_before_blocking(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[
                {"symbol": "AAPL", "quantity": 12, "buy_price": 100, "current_price": 100, "stop_loss": 90},
                {"symbol": "MSFT", "quantity": 9, "buy_price": 100, "current_price": 100, "stop_loss": 90},
            ],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
        )

        self.assertEqual(snapshot["risk_state"], "WARNING")
        self.assertFalse(snapshot["blocks_new_buys"])
        self.assertTrue(any(alert["metric"] == "sector_exposure_percent" for alert in snapshot["alerts"]))

    def test_virtual_capital_overrides_large_broker_account_for_exposure(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[{"symbol": "AAPL", "quantity": 10, "buy_price": 100, "current_price": 100, "stop_loss": 95}],
            account_summary=[
                {"tag": "NetLiquidation", "value": "999000"},
                {"tag": "TotalCashValue", "value": "998000"},
                {"tag": "BuyingPower", "value": "1998000"},
            ],
            daily_realized_pnl=-250,
        )

        self.assertEqual(snapshot["effective_equity"], 5000.0)
        self.assertEqual(snapshot["virtual_trading_capital"], 5000.0)
        self.assertEqual(snapshot["broker_net_liquidation"], 999000.0)
        self.assertEqual(snapshot["broker_cash"], 998000.0)
        self.assertEqual(snapshot["broker_buying_power"], 1998000.0)
        self.assertAlmostEqual(snapshot["total_portfolio_exposure_percent"], 20.0)
        self.assertAlmostEqual(snapshot["account_utilization_percent"], 20.0)
        self.assertAlmostEqual(snapshot["daily_drawdown_percent"], 5.0)

    def test_refresh_journals_block_and_recovery_events(self):
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.execute(
                """
                INSERT INTO account_summary (tag, value, currency, account, updated_at)
                VALUES ('NetLiquidation', '10000', 'USD', 'DU123', '2026-05-12T00:00:00+00:00')
                """
            )
            db.commit()

        asyncio.run(database.add_position({"symbol": "AAPL", "buy_price": 100, "current_price": 100, "quantity": 15, "stop_loss": 90}))
        blocked = asyncio.run(portfolio_risk_engine.refresh_portfolio_risk())
        self.assertTrue(blocked["blocks_new_buys"])

        asyncio.run(database.close_position("AAPL", "test close"))
        recovered = asyncio.run(portfolio_risk_engine.refresh_portfolio_risk())
        self.assertEqual(recovered["risk_state"], "SAFE")

        rows = asyncio.run(database.get_trade_journal(limit=20))
        event_types = {row["event_type"] for row in rows}
        self.assertIn("RISK_BLOCK_BUY", event_types)
        self.assertIn("RISK_RECOVERED", event_types)

    def test_portfolio_risk_alert_summary_is_short_and_preserves_raw_details(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[{"symbol": "AAPL", "quantity": 25, "buy_price": 100, "current_price": 100, "stop_loss": 90}],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
            daily_realized_pnl=-600,
        )

        self.assertLessEqual(len(snapshot["block_reason_summary"]["text"]), 250)
        self.assertTrue(snapshot["raw_block_reasons"])
        self.assertEqual(snapshot["full_details"]["block_reasons"], snapshot["raw_block_reasons"])
        self.assertTrue(snapshot["block_reason_summary"]["top_categories"])

    def test_portfolio_duplicate_alert_reasons_are_grouped(self):
        summary = portfolio_risk_engine.summarize_reason_list([
            "Daily realized drawdown exceeds the configured limit.",
            "Daily realized drawdown exceeds the configured limit.",
            "Account utilization exceeds the configured danger limit.",
        ])

        self.assertIn("Drawdown: 2 symbols", summary["text"])
        self.assertIn("Capital: 1 symbol", summary["text"])
        self.assertLessEqual(len(summary["text"]), 250)

    def test_unknown_low_confidence_sector_does_not_dominate_warnings(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[{"symbol": "ZZZZ", "quantity": 50, "buy_price": 100, "current_price": 100, "stop_loss": 90}],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
        )

        self.assertFalse(any(alert["metric"] == "sector_exposure_percent" for alert in snapshot["alerts"]))
        self.assertEqual(snapshot["unknown_sector_percentage"], 100.0)
        self.assertEqual(snapshot["diversification_quality"], "LOW")

    def test_portfolio_risk_exposes_sector_quality_metrics(self):
        snapshot = portfolio_risk_engine.evaluate_risk_snapshot(
            positions=[
                {"symbol": "NVDA", "quantity": 5, "buy_price": 100, "current_price": 100, "stop_loss": 90},
                {"symbol": "LLY", "quantity": 5, "buy_price": 100, "current_price": 100, "stop_loss": 90},
            ],
            account_summary=[{"tag": "NetLiquidation", "value": "10000"}],
        )

        self.assertEqual(snapshot["known_sector_percentage"], 100.0)
        self.assertEqual(snapshot["unknown_sector_percentage"], 0.0)
        self.assertIn(snapshot["diversification_quality"], {"HIGH", "MODERATE"})
        self.assertTrue(snapshot["top_sectors"])
        self.assertIn("classification_source", snapshot["largest_position"])
        self.assertIn("normalized_sector", snapshot["largest_position"])


if __name__ == "__main__":
    unittest.main()

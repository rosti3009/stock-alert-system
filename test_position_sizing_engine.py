import unittest

import config
from position_sizing_engine import (
    PositionSizingInput,
    PositionSizingState,
    evaluate_position_sizing,
    summarize_position_sizing,
)


class PositionSizingEngineTests(unittest.TestCase):
    def setUp(self):
        self.base_row = {
            "symbol": "AAPL",
            "price": 100.0,
            "stop_loss": 92.0,
            "atr": 2.0,
            "avg_volume": 2_000_000,
            "volume": 2_000_000,
            "relative_volume": 1.0,
            "bid": 99.95,
            "ask": 100.05,
        }
        self.original_virtual_capital = config.VIRTUAL_TRADING_CAPITAL_USD
        config.VIRTUAL_TRADING_CAPITAL_USD = 5000.0
        self.base_context = {
            "open_positions": [],
            "account_equity": 10_000.0,
            "market_regime": {"regime": "BULL", "position_size_factor": 1.0},
            "execution_quality": {"state": "EXECUTION_SAFE", "blocks_buy": False, "metrics": {"spread_percent": 0.1}},
            "portfolio_risk": {
                "total_portfolio_exposure_percent": 0.0,
                "total_open_risk_percent": 0.0,
                "daily_drawdown_percent": 0.0,
                "unrealized_drawdown_percent": 0.0,
                "exposure_by_sector": [],
                "exposure_by_symbol": [],
            },
        }

    def tearDown(self):
        config.VIRTUAL_TRADING_CAPITAL_USD = self.original_virtual_capital

    def build(self, row=None, **overrides):
        payload = dict(self.base_context)
        payload.update(overrides)
        return evaluate_position_sizing(PositionSizingInput(row=row or self.base_row, **payload))

    def test_full_size_recommendation(self):
        result = self.build()
        self.assertEqual(result["state"], PositionSizingState.FULL_SIZE.value)
        self.assertFalse(result["blocks_buy"])
        self.assertGreater(result["recommended_position_size_usd"], 0)
        self.assertEqual(result["volatility_adjustment"], 1.0)
        self.assertEqual(result["liquidity_adjustment"], 1.0)

    def test_virtual_capital_overrides_large_broker_account_for_sizing(self):
        result = self.build(account_equity=999_000.0)

        self.assertEqual(result["account_equity"], 5000.0)
        self.assertEqual(result["effective_equity"], 5000.0)
        self.assertEqual(result["virtual_trading_capital"], 5000.0)
        self.assertEqual(result["broker_account_equity"], 999000.0)
        self.assertEqual(result["max_risk_per_trade"], 50.0)
        self.assertLessEqual(result["recommended_position_size_usd"], 1000.0)
        self.assertEqual(result["risk_calculation_basis"], "virtual_trading_capital")

    def test_high_volatility_reduces_size(self):
        row = {**self.base_row, "atr_percent": config.MAX_INTRADAY_VOLATILITY + 1, "atr": 0}
        result = self.build(row=row)
        self.assertEqual(result["state"], PositionSizingState.REDUCED_SIZE.value)
        self.assertFalse(result["blocks_buy"])
        self.assertLess(result["volatility_adjustment"], 1.0)

    def test_dangerous_volatility_blocks_buy_sizing(self):
        row = {**self.base_row, "atr_percent": (config.MAX_INTRADAY_VOLATILITY * 2) + 1, "atr": 0}
        result = self.build(row=row)
        self.assertEqual(result["state"], PositionSizingState.BLOCK_NEW_POSITION.value)
        self.assertTrue(result["blocks_buy"])
        self.assertTrue(any("Dangerous volatility" in reason for reason in result["block_reasons"]))

    def test_insufficient_liquidity_blocks_buy_sizing(self):
        row = {**self.base_row, "avg_volume": config.MIN_AVERAGE_VOLUME * 0.25}
        result = self.build(row=row)
        self.assertEqual(result["state"], PositionSizingState.BLOCK_NEW_POSITION.value)
        self.assertTrue(result["blocks_buy"])
        self.assertTrue(any("Insufficient liquidity" in reason for reason in result["block_reasons"]))

    def test_crash_protection_blocks_buy_sizing(self):
        result = self.build(market_regime={"regime": "CRASH_PROTECTION", "position_size_factor": 0.0})
        self.assertEqual(result["state"], PositionSizingState.BLOCK_NEW_POSITION.value)
        self.assertTrue(result["blocks_buy"])
        self.assertIn("Crash protection regime", result["block_reasons"])

    def test_extreme_concentration_blocks_buy_sizing(self):
        portfolio_risk = {
            **self.base_context["portfolio_risk"],
            "exposure_by_sector": [{"sector": "Technology", "exposure_percent": config.MAX_SECTOR_EXPOSURE_PERCENT - 1}],
            "exposure_by_symbol": [],
        }
        result = self.build(portfolio_risk=portfolio_risk)
        self.assertEqual(result["state"], PositionSizingState.BLOCK_NEW_POSITION.value)
        self.assertTrue(any("Extreme concentration" in reason for reason in result["block_reasons"]))

    def test_summary_uses_worst_state(self):
        safe = self.build()
        blocked = self.build(market_regime={"regime": "CRASH_PROTECTION", "position_size_factor": 0.0})
        summary = summarize_position_sizing([safe, blocked])
        self.assertEqual(summary["state"], PositionSizingState.BLOCK_NEW_POSITION.value)
        self.assertTrue(summary["blocks_buy"])


if __name__ == "__main__":
    unittest.main()

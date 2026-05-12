from __future__ import annotations

import asyncio
import gc
import os
import tempfile
import time
import unittest
from unittest.mock import patch

import config
import database
import market_regime_engine


def raw_series(symbol: str, start: float, step: float, bars: int = 240) -> dict:
    closes = [start + (i * step) for i in range(bars)]
    opens = [value * 0.998 for value in closes]
    highs = [value * 1.01 for value in closes]
    lows = [value * 0.99 for value in closes]
    volumes = [1_000_000 + i for i in range(bars)]
    return {
        "symbol": symbol,
        "current_price": closes[-1],
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "volumes": volumes,
    }


def candidate(symbol: str, momentum: float, trend: str = "Bullish") -> dict:
    return {
        "symbol": symbol,
        "price": 100,
        "ma20": 95 if momentum >= 0 else 105,
        "momentum_5d": momentum,
        "trend": trend,
        "volume_ratio": 1.1,
    }


class MarketRegimeEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        self.original_thresholds = {
            "REGIME_VIX_WARNING": config.REGIME_VIX_WARNING,
            "REGIME_VIX_DANGER": config.REGIME_VIX_DANGER,
            "REGIME_DRAWDOWN_WARNING": config.REGIME_DRAWDOWN_WARNING,
            "REGIME_DRAWDOWN_BLOCK": config.REGIME_DRAWDOWN_BLOCK,
            "REGIME_BREADTH_WARNING": config.REGIME_BREADTH_WARNING,
            "REGIME_BREADTH_DANGER": config.REGIME_BREADTH_DANGER,
            "ACCOUNT_BALANCE": config.ACCOUNT_BALANCE,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.ACCOUNT_BALANCE = 10_000.0
        config.REGIME_VIX_WARNING = 25.0
        config.REGIME_VIX_DANGER = 35.0
        config.REGIME_DRAWDOWN_WARNING = 5.0
        config.REGIME_DRAWDOWN_BLOCK = 10.0
        config.REGIME_BREADTH_WARNING = 45.0
        config.REGIME_BREADTH_DANGER = 35.0
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

    def test_strong_bull_recommends_aggressive_entries_without_threshold_override(self):
        raw_by_symbol = {
            "SPY": raw_series("SPY", 100, 1.0),
            "QQQ": raw_series("QQQ", 100, 1.2),
            "VIX": raw_series("VIX", 18, 0.0),
        }
        indicators = {symbol: market_regime_engine.compute_indicators(raw) for symbol, raw in raw_by_symbol.items()}
        candidates = [candidate(f"SYM{i}", 2.0) for i in range(8)]

        snapshot = market_regime_engine.evaluate_market_regime(
            indicators_by_symbol=indicators,
            raw_by_symbol=raw_by_symbol,
            candidates=candidates,
        )

        self.assertEqual(snapshot["regime"], "STRONG_BULL")
        self.assertTrue(snapshot["allow_new_buys"])
        self.assertTrue(snapshot["allow_aggressive_entries"])
        self.assertEqual(snapshot["recommended_max_exposure"], 100.0)
        self.assertEqual(snapshot["min_score_override"], 80)

    def test_defensive_market_is_advisory_and_does_not_block_buys(self):
        raw_by_symbol = {
            "SPY": raw_series("SPY", 120, -0.05),
            "QQQ": raw_series("QQQ", 100, 0.15),
            "VIX": raw_series("VIX", 26, 0.0),
        }
        indicators = {symbol: market_regime_engine.compute_indicators(raw) for symbol, raw in raw_by_symbol.items()}
        candidates = [candidate("A", -1.0, "Bearish"), candidate("B", 1.0, "Bullish")]

        snapshot = market_regime_engine.evaluate_market_regime(
            indicators_by_symbol=indicators,
            raw_by_symbol=raw_by_symbol,
            candidates=candidates,
        )

        self.assertEqual(snapshot["regime"], "DEFENSIVE")
        self.assertEqual(snapshot["risk_level"], "ELEVATED")
        self.assertTrue(snapshot["allow_new_buys"])
        self.assertFalse(snapshot["allow_aggressive_entries"])
        self.assertFalse(snapshot["buy_blocked"])

    def test_extreme_vix_blocks_new_buys(self):
        raw_by_symbol = {
            "SPY": raw_series("SPY", 100, 0.5),
            "QQQ": raw_series("QQQ", 100, 0.5),
            "VIX": raw_series("VIX", 45, 0.0),
        }
        indicators = {symbol: market_regime_engine.compute_indicators(raw) for symbol, raw in raw_by_symbol.items()}

        snapshot = market_regime_engine.evaluate_market_regime(
            indicators_by_symbol=indicators,
            raw_by_symbol=raw_by_symbol,
            candidates=[candidate("A", 2.0), candidate("B", 1.0)],
        )

        self.assertEqual(snapshot["regime"], "RISK_OFF")
        self.assertFalse(snapshot["allow_new_buys"])
        self.assertTrue(snapshot["buy_blocked"])
        self.assertIn("Extreme VIX", snapshot["buy_block_reasons"])

    def test_refresh_records_history_and_market_events(self):
        bullish_raw = {
            "SPY": raw_series("SPY", 100, 1.0),
            "QQQ": raw_series("QQQ", 100, 1.0),
            "VIX": raw_series("VIX", 18, 0.0),
        }
        crash_raw = {
            "SPY": raw_series("SPY", 200, -0.7),
            "QQQ": raw_series("QQQ", 200, -0.8),
            "VIX": raw_series("VIX", 60, 0.0),
        }

        def payload(raw_map):
            indicators = {symbol: market_regime_engine.compute_indicators(raw) for symbol, raw in raw_map.items()}
            return raw_map, indicators

        with patch("market_regime_engine._fetch_market_indicators", return_value=payload(bullish_raw)):
            first = asyncio.run(market_regime_engine.refresh_market_regime(
                candidates=[candidate(f"B{i}", 2.0) for i in range(5)],
                positions=[],
            ))
        self.assertEqual(first["regime"], "STRONG_BULL")

        bad_candidates = [candidate(f"D{i}", -3.0, "Bearish") for i in range(10)]
        with patch("market_regime_engine._fetch_market_indicators", return_value=payload(crash_raw)):
            second = asyncio.run(market_regime_engine.refresh_market_regime(
                candidates=bad_candidates,
                positions=[],
            ))

        self.assertEqual(second["regime"], "CRASH_PROTECTION")
        self.assertFalse(second["allow_new_buys"])

        history = asyncio.run(market_regime_engine.get_market_regime_history(limit=10))
        self.assertEqual(len(history), 2)

        rows = asyncio.run(database.get_trade_journal(limit=20))
        event_types = {row["event_type"] for row in rows}
        self.assertIn("MARKET_REGIME_CHANGED", event_types)
        self.assertIn("MARKET_RISK_WARNING", event_types)
        self.assertIn("MARKET_BUY_BLOCKED", event_types)


if __name__ == "__main__":
    unittest.main()

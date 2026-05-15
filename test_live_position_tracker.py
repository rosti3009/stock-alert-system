from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import config
import database
import live_position_tracker
import main
import strategy_mode


class LivePositionTrackerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "tracker.db")
        self.patches = [
            patch.object(config, "DB_PATH", self.db_path),
            patch.object(database, "DB_PATH", self.db_path),
            patch.object(config, "POSITION_TRACK_INTERVAL_SECONDS", 15, create=True),
            patch.object(config, "SWING_POSITION_TRACK_INTERVAL_SECONDS", 60, create=True),
            patch.object(live_position_tracker.watchdog, "refresh_market_data_timestamp", new=self._noop_refresh),
            patch.object(live_position_tracker, "fetch_intraday_bars", side_effect=lambda symbol, timeframe="5m": [{"close": 101.0, "timeframe": timeframe}]),
        ]
        for p in self.patches:
            p.start()
        asyncio.run(database.init_db())

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()

    async def _noop_refresh(self, *args, **kwargs):
        return None

    def add_position(self, symbol: str = "AAPL") -> None:
        asyncio.run(database.add_position({
            "symbol": symbol,
            "buy_price": 100,
            "quantity": 2,
            "current_price": 100,
            "reason": "test position",
        }))

    def test_open_position_enters_live_tracker(self):
        self.add_position("AAPL")

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD", "rsi": 55, "bid": 100.9, "ask": 101.1, "vwap": 100.5}

        updated = asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual([p["symbol"] for p in updated], ["AAPL"])
        self.assertEqual(status["tracked_symbols"], ["AAPL"])
        self.assertEqual(status["positions"][0]["source"], "live_position_tracker")
        self.assertTrue(status["positions"][0]["bid_ask_refreshed"])
        self.assertTrue(status["positions"][0]["vwap_refreshed"])
        self.assertTrue(status["positions"][0]["bars_1m_refreshed"])
        self.assertTrue(status["positions"][0]["pnl_refreshed"])

    def test_closed_position_is_removed_from_live_tracker(self):
        self.add_position("AAPL")

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD"}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        asyncio.run(database.close_position("AAPL", "test close"))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual(status["tracked_symbols"], [])
        self.assertEqual(status["tracked_count"], 0)
        self.assertEqual(status["open_position_count"], 0)

    def test_intraday_positions_refresh_continuously_without_swing_throttle(self):
        self.add_position("AAPL")
        asyncio.run(database.set_app_state(strategy_mode.STRATEGY_MODE_KEY, strategy_mode.StrategyMode.INTRADAY_TECHNICAL.value))
        calls: list[str] = []

        async def fake_scan(symbol: str) -> dict:
            calls.append(symbol)
            return {"symbol": symbol, "price": 101 + len(calls), "signal": "HOLD", "setup": "momentum breakout", "vwap": 100}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))

        self.assertEqual(calls, ["AAPL", "AAPL"])

    def test_scanner_rotation_no_longer_controls_active_positions(self):
        self.add_position("AAPL")
        scanned: list[str] = []

        async def fake_scan(symbol: str) -> dict:
            scanned.append(symbol)
            return {"symbol": symbol, "price": 200, "signal": "HOLD", "score": 50, "weekly_score": 50, "reasons": [], "weekly_reasons": []}

        with patch.object(main.session_manager, "get_cached_session_status", return_value={"scan_allowed": True}), \
             patch.object(main, "get_scan_symbols", new=lambda: self._async_value(["MSFT"])), \
             patch.object(main, "scan_symbol", new=fake_scan), \
             patch.object(main, "process_auto_trading", new=lambda rows: self._async_value(None)), \
             patch.object(main, "rebuild_top_weekly", return_value=None), \
             patch.object(main.watchdog, "refresh_market_data_timestamp", new=lambda *args, **kwargs: self._async_value(None)):
            asyncio.run(main.run_full_scan())

        self.assertEqual(scanned, ["MSFT"])

    async def _async_value(self, value):
        return value

    def test_watchdog_blocks_when_live_position_tracking_is_stale(self):
        import json
        from datetime import datetime, timedelta, timezone
        import watchdog

        self.add_position("AAPL")
        fresh = datetime.now(timezone.utc).isoformat()
        stale = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        asyncio.run(database.set_app_state("tws_mirror_last_success_at", fresh))
        asyncio.run(database.set_app_state("execution_sync_last_success_at", fresh))
        asyncio.run(database.set_app_state(
            live_position_tracker.LIVE_POSITION_TRACKER_STATE_KEY,
            json.dumps({
                "source": "live_position_tracker",
                "last_refresh_at": stale,
                "tracked_count": 1,
                "tracked_symbols": ["AAPL"],
                "positions": [{"symbol": "AAPL", "last_refresh_at": stale}],
                "healthy": True,
            }),
        ))

        with patch.object(config, "WATCHDOG_POSITION_TRACKING_STALE_SECONDS", 60, create=True), \
             patch.object(watchdog, "is_ib_connected", return_value=True), \
             patch.object(watchdog, "send_watchdog_alert", return_value=True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["trading_blocked"])
        self.assertTrue(status["stale_data"]["live_position_tracking"])
        self.assertTrue(any("Live position tracking stale" in reason for reason in status["blocking_reasons"]))


if __name__ == "__main__":
    unittest.main()

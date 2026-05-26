from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import aiosqlite

asyncio.set_event_loop(asyncio.new_event_loop())

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

    def insert_position(
        self,
        symbol: str,
        *,
        status: str = "OPEN",
        source: str | None = None,
        action: str = "HOLD",
        recovery_source_position_id: int | None = None,
    ) -> None:
        async def _insert() -> None:
            now = database.now_iso()
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO positions (
                        symbol, buy_price, quantity, buy_date, current_price,
                        profit_amount, profit_percent, stop_loss, take_profit_1, take_profit_2,
                        status, action, reason, notes, source, created_at, updated_at, closed_at,
                        recovery_source_position_id
                    )
                    VALUES (?, 100, 2, ?, 100, NULL, NULL, 92, 108, 116, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
                    """,
                    (
                        symbol,
                        now,
                        status,
                        action,
                        "test position",
                        source,
                        now,
                        now,
                        recovery_source_position_id,
                    ),
                )
                await db.commit()

        asyncio.run(_insert())

    def test_open_position_enters_live_tracker(self):
        self.add_position("AAPL")

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD", "rsi": 55, "bid": 100.9, "ask": 101.1, "vwap": 100.5}

        updated = asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual([p["symbol"] for p in updated], ["AAPL"])
        self.assertEqual(status["tracked_symbols"], ["AAPL"])
        self.assertEqual(status["positions"][0]["source"], "live_position_tracker")
        self.assertTrue(status["positions"][0]["live_tracking"])
        self.assertEqual(status["positions"][0]["live_tracking_source"], "live_position_tracker")
        self.assertEqual(status["positions"][0]["live_tracking_last_refresh_at"], status["positions"][0]["last_refresh_at"])
        self.assertTrue(status["positions"][0]["bid_ask_refreshed"])
        self.assertTrue(status["positions"][0]["vwap_refreshed"])
        self.assertTrue(status["positions"][0]["bars_1m_refreshed"])
        self.assertTrue(status["positions"][0]["pnl_refreshed"])

    def test_get_open_positions_includes_all_open_sources(self):
        self.insert_position("AUTO", source="AUTO", action="CUSTOM_ACTION")
        self.insert_position("ASTL", source="TWS_RECONCILIATION_RECOVERY", recovery_source_position_id=1)
        self.insert_position("BASE", source="TWS_BASELINE_ADOPTED")
        self.insert_position("DONE", status="CLOSED", source="TWS_RECONCILIATION_RECOVERY")

        open_positions = asyncio.run(database.get_open_positions())

        self.assertEqual([p["symbol"] for p in open_positions], ["AUTO", "ASTL", "BASE"])
        self.assertEqual([p["source"] for p in open_positions], ["AUTO", "TWS_RECONCILIATION_RECOVERY", "TWS_BASELINE_ADOPTED"])
        self.assertEqual(open_positions[0]["action"], "CUSTOM_ACTION")
        self.assertEqual(open_positions[1]["recovery_source_position_id"], 1)

    def test_recovered_open_position_enters_live_tracker(self):
        self.insert_position(
            "ASTL",
            source="TWS_RECONCILIATION_RECOVERY",
            recovery_source_position_id=1,
        )

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD", "bid": 100.9, "ask": 101.1}

        updated = asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual([p["symbol"] for p in updated], ["ASTL"])
        self.assertEqual(status["open_position_count"], 1)
        self.assertEqual(status["tracked_count"], 1)
        self.assertEqual(status["tracked_symbols"], ["ASTL"])
        self.assertTrue(status["positions"][0]["live_tracking"])
        self.assertEqual(status["positions"][0]["position_source"], "TWS_RECONCILIATION_RECOVERY")

    def test_adopted_open_position_enters_live_tracker(self):
        self.insert_position("MSFT", source="TWS_BASELINE_ADOPTED")

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD"}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual(status["open_position_count"], 1)
        self.assertEqual(status["tracked_count"], 1)
        self.assertEqual(status["tracked_symbols"], ["MSFT"])
        self.assertEqual(status["positions"][0]["position_source"], "TWS_BASELINE_ADOPTED")

    def test_closed_recovered_and_adopted_rows_are_excluded(self):
        self.insert_position("ASTL", status="CLOSED", source="TWS_RECONCILIATION_RECOVERY")
        self.insert_position("MSFT", status="CLOSED", source="TWS_BASELINE_ADOPTED")

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD"}

        open_positions = asyncio.run(database.get_open_positions())
        updated = asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual(open_positions, [])
        self.assertEqual(updated, [])
        self.assertEqual(status["open_position_count"], 0)
        self.assertEqual(status["tracked_count"], 0)
        self.assertEqual(status["tracked_symbols"], [])

    def test_recovered_tracker_refresh_updates_timestamps(self):
        self.insert_position("ASTL", source="TWS_RECONCILIATION_RECOVERY", recovery_source_position_id=1)
        calls: list[str] = []

        async def fake_scan(symbol: str) -> dict:
            calls.append(symbol)
            return {"symbol": symbol, "price": 101 + len(calls), "signal": "HOLD"}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        first = asyncio.run(live_position_tracker.get_tracker_status())
        first_refresh = first["positions"][0]["last_refresh_at"]

        with patch.object(live_position_tracker, "now_iso", side_effect=["2099-01-01T00:00:00+00:00", "2099-01-01T00:00:01+00:00"]):
            asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        second = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual(calls, ["ASTL", "ASTL"])
        self.assertEqual(second["tracked_symbols"], ["ASTL"])
        self.assertNotEqual(second["positions"][0]["last_refresh_at"], first_refresh)
        self.assertEqual(second["positions"][0]["last_refresh_at"], "2099-01-01T00:00:00+00:00")
        self.assertEqual(second["positions"][0]["live_tracking_last_refresh_at"], "2099-01-01T00:00:00+00:00")

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

    def test_sell_signal_does_not_close_without_broker_confirmation(self):
        self.add_position("TSCO")

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 95, "signal": "SELL"}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        row = asyncio.run(database.get_position("TSCO"))
        self.assertEqual(row["status"], "CLOSE_REQUESTED")
        self.assertEqual(row["action"], "SELL_SIGNAL")

    def test_broker_open_reopens_closed_and_restores_tracker_symbol(self):
        self.insert_position("TSCO", status="CLOSED", action="INTRADAY_SELL_SIGNAL")
        asyncio.run(database.save_broker_sync_snapshot({
            "synced_at": database.now_iso(),
            "ok": True,
            "connected": True,
            "positions": [{"symbol": "TSCO", "position": 3}],
            "open_orders": [],
            "executions": [],
            "errors": [],
            "equity": {},
        }))

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "price": 101, "signal": "HOLD"}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        row = asyncio.run(database.get_position("TSCO"))
        status = asyncio.run(live_position_tracker.get_tracker_status())
        self.assertEqual(row["status"], "OPEN")
        self.assertEqual(row["action"], "POSITION_REOPENED_FROM_BROKER_TRUTH")
        self.assertIn("TSCO", status["tracked_symbols"])

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

    def test_refresh_rebuilds_tracker_symbols_from_broker_snapshot_when_empty(self):
        self.add_position("TSCO")
        asyncio.run(database.save_broker_sync_snapshot({
            "synced_at": database.now_iso(),
            "ok": True,
            "connected": True,
            "account": "DU123",
            "positions": [{"symbol": "TSCO", "position": 10}],
            "executions": [],
            "open_orders": [],
            "errors": [],
        }))

        async def fake_scan(symbol: str) -> dict:
            return {"symbol": symbol, "signal": "ERROR", "error": "temporary scan failure"}

        asyncio.run(live_position_tracker.refresh_live_tracked_positions(fake_scan))
        status = asyncio.run(live_position_tracker.get_tracker_status())

        self.assertEqual(status["open_position_count"], 1)
        self.assertEqual(status["tracked_count"], 1)
        self.assertEqual(status["tracked_symbols"], ["TSCO"])
        self.assertIsNotNone(status["last_refresh_at"])


if __name__ == "__main__":
    unittest.main()

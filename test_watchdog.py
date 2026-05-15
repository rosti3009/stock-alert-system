from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import aiosqlite

import config
import database
import watchdog
from trading_safety import require_watchdog_order_allowed


def iso_delta(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


async def _init_watchdog_db(db_path: str) -> None:
    await database.init_db()
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tws_heartbeat (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                connected INTEGER DEFAULT 0,
                account TEXT,
                last_sync_at TEXT,
                error TEXT
            )
            """
        )
        await db.commit()


async def seed_watchdog_inputs(*, connected: bool, mirror_at: str, execution_at: str, market_at: str | None = None):
    await database.set_app_state("tws_mirror_last_success_at", mirror_at)
    await database.set_app_state("execution_sync_last_success_at", execution_at)
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO tws_heartbeat (id, connected, account, last_sync_at, error)
            VALUES (1, ?, 'DU123', ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                connected = excluded.connected,
                account = excluded.account,
                last_sync_at = excluded.last_sync_at,
                error = excluded.error
            """,
            (1 if connected else 0, mirror_at, None if connected else "socket closed"),
        )
        if market_at is not None:
            await db.execute(
                """
                INSERT INTO daily_candidates (symbol, price, signal, created_at)
                VALUES ('AAPL', 100, 'BUY', ?)
                """,
                (market_at,),
            )
        await db.commit()


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "watchdog.db")
        self.patches = [
            patch.object(config, "DB_PATH", self.db_path),
            patch.object(database, "DB_PATH", self.db_path),
            patch.object(config, "WATCHDOG_TWS_MIRROR_STALE_SECONDS", 120, create=True),
            patch.object(config, "WATCHDOG_EXECUTION_SYNC_STALE_SECONDS", 120, create=True),
            patch.object(config, "WATCHDOG_MARKET_DATA_STALE_SECONDS", 120, create=True),
            patch.object(config, "WATCHDOG_DISCONNECT_CIRCUIT_SECONDS", 180, create=True),
            patch.object(config, "WATCHDOG_RECONNECT_BACKOFF_SECONDS", 30, create=True),
            patch.object(config, "WATCHDOG_ALERT_COOLDOWN_SECONDS", 300, create=True),
        ]
        for p in self.patches:
            p.start()
        asyncio.run(_init_watchdog_db(self.db_path))

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()

    def test_disconnected_tws_blocks_trading(self):
        sent_alerts = []
        fresh = iso_delta(0)
        asyncio.run(seed_watchdog_inputs(connected=False, mirror_at=fresh, execution_at=fresh, market_at=fresh))

        with patch.object(watchdog, "is_ib_connected", return_value=False), \
             patch.object(watchdog, "_attempt_reconnect_sync", return_value={"ok": False, "result": "failed", "error": "offline"}), \
             patch.object(watchdog, "send_watchdog_alert", side_effect=lambda message: sent_alerts.append(message) or True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["trading_blocked"])
        self.assertIn("TWS/API disconnected", status["blocking_reasons"])
        with self.assertRaisesRegex(RuntimeError, "blocked by watchdog"):
            require_watchdog_order_allowed("BUY")
        self.assertTrue(any("disconnect" in alert.lower() for alert in sent_alerts))

    def test_stale_market_data_blocks_trading(self):
        sent_alerts = []
        fresh = iso_delta(0)
        stale = iso_delta(-600)
        asyncio.run(seed_watchdog_inputs(connected=True, mirror_at=fresh, execution_at=fresh, market_at=stale))

        with patch.object(watchdog, "is_ib_connected", return_value=True), \
             patch.object(watchdog, "send_watchdog_alert", side_effect=lambda message: sent_alerts.append(message) or True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["trading_blocked"])
        self.assertTrue(status["stale_data"]["market_data"])
        self.assertTrue(any("Market data stale" in reason for reason in status["blocking_reasons"]))
        self.assertTrue(any("Market data stale" in alert for alert in sent_alerts))

    def test_reconnect_success_clears_blocked_state(self):
        connected = {"value": False}

        def reconnect_success():
            connected["value"] = True
            return {"ok": True, "result": "connected", "error": None}

        fresh = iso_delta(0)
        asyncio.run(seed_watchdog_inputs(connected=False, mirror_at=fresh, execution_at=fresh, market_at=fresh))
        asyncio.run(database.set_app_state(watchdog.WATCHDOG_STATUS_KEY, json.dumps({"tws_connected": False})))

        with patch.object(watchdog, "is_ib_connected", side_effect=lambda: connected["value"]), \
             patch.object(watchdog, "_attempt_reconnect_sync", side_effect=reconnect_success), \
             patch.object(watchdog, "send_watchdog_alert", return_value=True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["tws_connected"])
        self.assertFalse(status["trading_blocked"])
        self.assertEqual(status["blocking_reasons"], [])

    def test_duplicate_telegram_alerts_are_suppressed(self):
        sent_alerts = []
        fresh = iso_delta(0)
        stale = iso_delta(-600)
        asyncio.run(seed_watchdog_inputs(connected=True, mirror_at=fresh, execution_at=fresh, market_at=stale))

        with patch.object(watchdog, "is_ib_connected", return_value=True), \
             patch.object(watchdog, "send_watchdog_alert", side_effect=lambda message: sent_alerts.append(message) or True):
            asyncio.run(watchdog.run_watchdog_once())
            asyncio.run(watchdog.run_watchdog_once())

        stale_alerts = [alert for alert in sent_alerts if "Market data stale" in alert]
        self.assertEqual(len(stale_alerts), 1)

    def test_watchdog_status_endpoint_works(self):
        async def fake_status():
            return {"healthy": True, "trading_blocked": False, "last_heartbeat_at": "2026-05-15T00:00:00+00:00"}

        import main

        with patch.object(main.watchdog, "get_watchdog_status", side_effect=fake_status):
            response = asyncio.run(main.api_watchdog_status())

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode())
        self.assertTrue(payload["healthy"])
        self.assertFalse(payload["trading_blocked"])


class CircuitBreakerRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "recovery.db")
        self.patches = [
            patch.object(config, "DB_PATH", self.db_path),
            patch.object(database, "DB_PATH", self.db_path),
            patch.object(config, "CIRCUIT_BREAKER_MAX_IBKR_ERRORS", 3, create=True),
            patch.object(config, "WATCHDOG_TWS_MIRROR_STALE_SECONDS", 120, create=True),
            patch.object(config, "WATCHDOG_EXECUTION_SYNC_STALE_SECONDS", 120, create=True),
            patch.object(config, "WATCHDOG_MARKET_DATA_STALE_SECONDS", 120, create=True),
            patch.object(config, "WATCHDOG_DISCONNECT_CIRCUIT_SECONDS", 180, create=True),
            patch.object(config, "WATCHDOG_RECONNECT_BACKOFF_SECONDS", 30, create=True),
            patch.object(config, "WATCHDOG_ALERT_COOLDOWN_SECONDS", 300, create=True),
        ]
        for p in self.patches:
            p.start()
        asyncio.run(_init_watchdog_db(self.db_path))

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()

    def test_stale_ibkr_breaker_auto_recovers_when_health_is_fresh(self):
        import circuit_breaker
        import main

        async def fake_startup_status():
            return {"ok": True, "state": "PASSED"}

        async def fake_startup_passed():
            return True

        async def fake_reconciliation():
            return {"ok": True, "issues_count": 0, "issues": []}

        async def fake_watchdog_status():
            return {
                "tws_connected": True,
                "stale_data": {"tws_mirror": False, "execution_sync": False, "market_data": False},
                "trading_blocked": False,
                "blocking_reasons": [],
            }

        async def run_case():
            await circuit_breaker.record_ibkr_error("socket reset", source="tws_mirror.run_tws_mirror_once")
            await circuit_breaker.record_ibkr_error("socket reset", source="tws_mirror.run_tws_mirror_once")
            tripped = await circuit_breaker.record_ibkr_error("socket reset", source="tws_mirror.run_tws_mirror_once")
            self.assertTrue(tripped["tripped"])
            with patch.object(main.startup_recovery, "get_startup_recovery_status", side_effect=fake_startup_status), \
                 patch.object(main.startup_recovery, "startup_recovery_passed", side_effect=fake_startup_passed), \
                 patch.object(main.reconciliation_lifecycle, "get_reconciliation_status", side_effect=fake_reconciliation), \
                 patch.object(main.watchdog, "get_watchdog_status", side_effect=fake_watchdog_status):
                result = await main._evaluate_auto_trading_enable_safety()
            self.assertFalse(result["circuit_breaker"].get("tripped"))
            self.assertNotIn("Circuit breaker tripped", ";".join(result["blocked_reasons"]))
            self.assertEqual(await database.get_app_state(circuit_breaker.IBKR_ERROR_COUNT_KEY), None)
            self.assertIsNotNone(await circuit_breaker.get_last_auto_recovery())

        asyncio.run(run_case())

    def test_watchdog_healthy_state_clears_watchdog_circuit_breaker(self):
        import circuit_breaker

        fresh = iso_delta(0)
        asyncio.run(seed_watchdog_inputs(connected=True, mirror_at=fresh, execution_at=fresh, market_at=fresh))
        asyncio.run(circuit_breaker.trip_circuit_breaker("Prolonged TWS/API disconnect (999s)", source="watchdog"))

        with patch.object(watchdog, "is_ib_connected", return_value=True), \
             patch.object(watchdog, "send_watchdog_alert", return_value=True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["healthy"])
        self.assertTrue(status["circuit_breaker_auto_recovered"])
        self.assertFalse(asyncio.run(circuit_breaker.get_circuit_breaker_state()).get("tripped"))

    def test_successful_reconnect_clears_watchdog_circuit_breaker(self):
        import circuit_breaker

        connected = {"value": False}

        def reconnect_success():
            connected["value"] = True
            return {"ok": True, "result": "connected", "error": None}

        fresh = iso_delta(0)
        asyncio.run(seed_watchdog_inputs(connected=False, mirror_at=fresh, execution_at=fresh, market_at=fresh))
        asyncio.run(database.set_app_state(watchdog.WATCHDOG_STATUS_KEY, json.dumps({"tws_connected": False})))
        asyncio.run(circuit_breaker.trip_circuit_breaker("Prolonged TWS/API disconnect (999s)", source="watchdog"))

        with patch.object(watchdog, "is_ib_connected", side_effect=lambda: connected["value"]), \
             patch.object(watchdog, "_attempt_reconnect_sync", side_effect=reconnect_success), \
             patch.object(watchdog, "send_watchdog_alert", return_value=True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["tws_connected"])
        self.assertFalse(status["trading_blocked"])
        self.assertTrue(status["circuit_breaker_auto_recovered"])
        self.assertFalse(asyncio.run(circuit_breaker.get_circuit_breaker_state()).get("tripped"))

    def test_active_scanner_refreshes_market_data_timestamp(self):
        import main

        closes = [100 + (i * 0.1) for i in range(230)]
        raw = {
            "symbol": "AAPL",
            "current_price": closes[-1],
            "opens": closes,
            "highs": [c + 1 for c in closes],
            "lows": [c - 1 for c in closes],
            "closes": closes,
            "volumes": [1_000_000 + i for i in range(230)],
        }

        with patch.object(main, "fetch_stock_data", return_value=raw), \
             patch.object(main, "maybe_send_alert", return_value=None):
            result = asyncio.run(main.scan_symbol("AAPL"))

        self.assertEqual(result["symbol"], "AAPL")
        self.assertIsNotNone(asyncio.run(database.get_app_state(watchdog.LAST_MARKET_DATA_AT_KEY)))
        source = json.loads(asyncio.run(database.get_app_state(watchdog.LAST_MARKET_DATA_REFRESH_SOURCE_KEY)))
        self.assertEqual(source["source"], "scanner_bars")
        self.assertEqual(source["symbol"], "AAPL")

    def test_stale_market_data_still_blocks_trading_after_refresh_ages_out(self):
        fresh = iso_delta(0)
        stale = iso_delta(-600)
        asyncio.run(seed_watchdog_inputs(connected=True, mirror_at=fresh, execution_at=fresh, market_at=None))
        asyncio.run(database.set_app_state(watchdog.LAST_MARKET_DATA_AT_KEY, stale))
        asyncio.run(database.set_app_state(watchdog.LAST_MARKET_DATA_REFRESH_SOURCE_KEY, json.dumps({"source": "scanner_bars", "refreshed_at": stale})))

        with patch.object(watchdog, "is_ib_connected", return_value=True), \
             patch.object(watchdog, "send_watchdog_alert", return_value=True):
            status = asyncio.run(watchdog.run_watchdog_once())

        self.assertTrue(status["trading_blocked"])
        self.assertTrue(status["stale_data"]["market_data"])
        self.assertFalse(status["market_data_feed_active"])
        self.assertTrue(any("Market data stale" in reason for reason in status["blocking_reasons"]))


if __name__ == "__main__":
    unittest.main()

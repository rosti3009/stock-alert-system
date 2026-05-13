from __future__ import annotations

import asyncio
import gc
import os
import tempfile
import time
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

import config
import database
import execution_sync
import startup_recovery
from circuit_breaker import (
    IBKR_ERROR_COUNT_KEY,
    IBKR_LAST_ERROR_KEY,
    IBKR_THRESHOLD_STATE_KEY,
    get_circuit_breaker_state,
    record_ibkr_error,
    reset_circuit_breaker,
    validate_buying_power,
)
from trading_safety import require_paper_auto_trading_allowed


class StartupRecoveryCircuitBreakerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_config_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        self.original_auto_send = config.AUTO_SEND_ORDERS
        self.original_paper = config.IBKR_PAPER_TRADING
        self.original_live = config.IBKR_ENABLE_REAL_TRADING
        self.original_port = config.IBKR_PORT
        self.original_mode = config.TRADING_MODE
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.AUTO_SEND_ORDERS = True
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.IBKR_PORT = 7497
        config.TRADING_MODE = "PAPER"
        asyncio.run(database.init_db())

    def tearDown(self):
        config.DB_PATH = self.original_config_db_path
        database.DB_PATH = self.original_database_db_path
        config.AUTO_SEND_ORDERS = self.original_auto_send
        config.IBKR_PAPER_TRADING = self.original_paper
        config.IBKR_ENABLE_REAL_TRADING = self.original_live
        config.IBKR_PORT = self.original_port
        config.TRADING_MODE = self.original_mode
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

    def test_circuit_breaker_trips_for_invalid_buying_power_and_blocks_trading(self):
        async def scenario():
            state = await validate_buying_power(-1, source="test")
            self.assertTrue(state["tripped"], state)
            self.assertIn("Invalid buying power", state["reason"])
            with self.assertRaisesRegex(RuntimeError, "circuit breaker tripped"):
                require_paper_auto_trading_allowed("unit test order")

            reset = await reset_circuit_breaker()
            self.assertFalse(reset["tripped"], reset)

        asyncio.run(scenario())

    def test_repeated_ibkr_errors_trip_circuit_breaker(self):
        async def scenario():
            await record_ibkr_error("first", source="test")
            await record_ibkr_error("second", source="test")
            state = await record_ibkr_error("third", source="test")
            self.assertTrue(state["tripped"], state)
            self.assertIn("Repeated IBKR errors", state["reason"])

        asyncio.run(scenario())

    def test_api_reset_clears_repeated_ibkr_error_state_and_allows_startup_recovery(self):
        async def fake_tws():
            return {"connected": True, "positions": [], "orders": [], "account": "DU1", "error": None}

        async def fake_account():
            return {
                "connected": True,
                "account": "DU1",
                "account_summary": [
                    {"tag": "BuyingPower", "value": "10000", "currency": "USD", "account": "DU1"},
                    {"tag": "NetLiquidation", "value": "5000", "currency": "USD", "account": "DU1"},
                ],
                "equity": {"buying_power": 10000, "net_liquidation": 5000},
                "error": None,
            }

        async def ok(*args, **kwargs):
            return {"ok": True, "issues": [], "issues_count": 0}

        async def seed_repeated_error_state():
            await record_ibkr_error("first", source="test")
            await record_ibkr_error("second", source="test")
            state = await record_ibkr_error("third", source="test")
            self.assertTrue(state["tripped"], state)
            self.assertEqual(await database.get_app_state(IBKR_ERROR_COUNT_KEY), "3")
            self.assertEqual(await database.get_app_state(IBKR_LAST_ERROR_KEY), "third")
            threshold_state = await database.get_app_state(IBKR_THRESHOLD_STATE_KEY)
            self.assertIsNotNone(threshold_state)
            self.assertIn('"threshold_reached": true', threshold_state)

        asyncio.run(seed_repeated_error_state())

        from main import app

        response = TestClient(app).post(
            "/api/circuit-breaker/reset",
            json={"reason": "Unit test reset"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["tripped"], response.json())

        async def verify_reset_and_startup_recovery():
            self.assertIsNone(await database.get_app_state(IBKR_ERROR_COUNT_KEY))
            self.assertIsNone(await database.get_app_state(IBKR_LAST_ERROR_KEY))
            self.assertIsNone(await database.get_app_state(IBKR_THRESHOLD_STATE_KEY))

            with patch("startup_recovery.run_tws_mirror_once", fake_tws), \
                patch("startup_recovery.account_sync.run_account_sync_once", fake_account), \
                patch("startup_recovery.execution_sync.sync_executions", ok), \
                patch("startup_recovery.adopt_tws_positions_as_baseline", ok), \
                patch("startup_recovery.close_db_positions_flat_in_tws", ok), \
                patch("startup_recovery.run_reconciliation_once", ok):
                status = await startup_recovery.run_startup_recovery()

            self.assertTrue(status["ok"], status)
            self.assertEqual(status["state"], "PASSED", status)
            self.assertFalse((await get_circuit_breaker_state())["tripped"])
            self.assertIsNone(await database.get_app_state(IBKR_ERROR_COUNT_KEY))

        asyncio.run(verify_reset_and_startup_recovery())

    def _execution(self, exec_id="E1"):
        return SimpleNamespace(
            execId=exec_id,
            side="BOT",
            shares=5,
            price=100.25,
            orderId=1,
            permId=10,
            acctNumber="DU1",
            exchange="NYSE",
            time="2026-05-13T00:00:00+00:00",
        )

    def test_normalize_raw_execution_without_execution_attribute(self):
        row = execution_sync.normalize_execution_item(self._execution())

        self.assertEqual(row["exec_id"], "E1")
        self.assertEqual(row["side"], "BOT")
        self.assertEqual(row["quantity"], 5.0)
        self.assertEqual(row["price"], 100.25)
        self.assertEqual(row["commission"], None)
        self.assertEqual(row["realized_pnl"], None)

    def test_normalize_fill_like_execution_with_commission_report(self):
        fill = SimpleNamespace(
            execution=self._execution("E2"),
            contract=SimpleNamespace(symbol="aapl"),
            commissionReport=SimpleNamespace(commission=1.23, realizedPNL=4.56),
        )

        row = execution_sync.normalize_execution_item(fill)

        self.assertEqual(row["exec_id"], "E2")
        self.assertEqual(row["symbol"], "AAPL")
        self.assertEqual(row["commission"], 1.23)
        self.assertEqual(row["realized_pnl"], 4.56)

    def test_normalize_execution_items_skips_malformed_rows(self):
        rows = execution_sync.normalize_execution_items([
            SimpleNamespace(not_an_execution=True),
            self._execution("E3"),
        ])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["exec_id"], "E3")

    def test_execution_sync_dedupes_execution_ids_and_preserves_commission(self):
        rows = [
            {
                "exec_id": "E1",
                "symbol": "AAPL",
                "side": "BOT",
                "quantity": 5,
                "price": 100.25,
                "order_id": 1,
                "perm_id": 10,
                "account": "DU1",
                "exchange": "NYSE",
                "time": "2026-05-13T00:00:00+00:00",
                "commission": 1.23,
                "realized_pnl": 0.0,
                "raw_json": "{}",
            },
            {
                "exec_id": "E1",
                "symbol": "AAPL",
                "side": "BOT",
                "quantity": 5,
                "price": 100.25,
                "order_id": 1,
                "perm_id": 10,
                "account": "DU1",
                "exchange": "NYSE",
                "time": "2026-05-13T00:00:00+00:00",
                "commission": 1.23,
                "realized_pnl": 0.0,
                "raw_json": "{}",
            },
            {
                "exec_id": "E2",
                "symbol": "AAPL",
                "side": "BOT",
                "quantity": 2,
                "price": 101.0,
                "order_id": 1,
                "perm_id": 10,
                "account": "DU1",
                "exchange": "NYSE",
                "time": "2026-05-13T00:01:00+00:00",
                "commission": 0.75,
                "realized_pnl": 0.0,
                "raw_json": "{}",
            },
        ]

        async def scenario():
            with patch("execution_sync.fetch_executions_sync", return_value=rows):
                result = await execution_sync.sync_executions()
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["fetched_count"], 3, result)
            self.assertEqual(result["inserted_count"], 2, result)
            self.assertEqual(result["duplicate_count"], 1, result)
            executions = await execution_sync.get_executions(symbol="AAPL")
            self.assertEqual(len(executions), 2, executions)
            self.assertEqual(sum(row["quantity"] for row in executions), 7)
            self.assertEqual(executions[0]["commission"], 0.75)

        asyncio.run(scenario())


    def test_startup_recovery_account_sync_accepts_raw_execution_objects(self):
        class FakeIB:
            def __init__(self):
                self.connected = False

            def connect(self, *args, **kwargs):
                self.connected = True

            def isConnected(self):
                return self.connected

            def disconnect(self):
                self.connected = False

            def managedAccounts(self):
                return ["DU1"]

            def accountSummary(self):
                return [
                    SimpleNamespace(tag="BuyingPower", value="10000", currency="USD", account="DU1"),
                    SimpleNamespace(tag="NetLiquidation", value="5000", currency="USD", account="DU1"),
                ]

            def reqAllOpenOrders(self):
                return None

            def sleep(self, seconds):
                return None

            def openTrades(self):
                return []

            def executions(self):
                return [
                    self_execution,
                    SimpleNamespace(not_an_execution=True),
                ]

        self_execution = self._execution("RAW1")

        async def fake_tws():
            return {"connected": True, "positions": [], "orders": [], "account": "DU1", "error": None}

        async def ok(*args, **kwargs):
            return {"ok": True, "issues": [], "issues_count": 0}

        async def scenario():
            await reset_circuit_breaker()
            with patch("ib_insync.IB", FakeIB), \
                patch("startup_recovery.run_tws_mirror_once", fake_tws), \
                patch("startup_recovery.execution_sync.sync_executions", ok), \
                patch("startup_recovery.adopt_tws_positions_as_baseline", ok), \
                patch("startup_recovery.close_db_positions_flat_in_tws", ok), \
                patch("startup_recovery.run_reconciliation_once", ok):
                status = await startup_recovery.run_startup_recovery()

            self.assertTrue(status["ok"], status)
            self.assertEqual(status["state"], "PASSED", status)
            self.assertEqual(status["steps"][1]["name"], "sync_account_open_orders_executions", status)
            self.assertEqual(status["steps"][1]["result"]["execution_history"][0]["exec_id"], "RAW1")
            executions = await startup_recovery.account_sync.get_executions(symbol="")
            self.assertEqual(len(executions), 1, executions)
            self.assertEqual(executions[0]["exec_id"], "RAW1")

        asyncio.run(scenario())

    def test_startup_recovery_passes_before_auto_trading_can_run(self):
        async def fake_tws():
            return {"connected": True, "positions": [], "orders": [], "account": "DU1", "error": None}

        async def fake_account():
            return {
                "connected": True,
                "account": "DU1",
                "account_summary": [
                    {"tag": "BuyingPower", "value": "10000", "currency": "USD", "account": "DU1"},
                    {"tag": "NetLiquidation", "value": "5000", "currency": "USD", "account": "DU1"},
                ],
                "equity": {"buying_power": 10000, "net_liquidation": 5000},
                "error": None,
            }

        async def ok(*args, **kwargs):
            return {"ok": True, "issues": [], "issues_count": 0}

        async def scenario():
            await reset_circuit_breaker()
            with patch("startup_recovery.run_tws_mirror_once", fake_tws), \
                patch("startup_recovery.account_sync.run_account_sync_once", fake_account), \
                patch("startup_recovery.execution_sync.sync_executions", ok), \
                patch("startup_recovery.adopt_tws_positions_as_baseline", ok), \
                patch("startup_recovery.close_db_positions_flat_in_tws", ok), \
                patch("startup_recovery.run_reconciliation_once", ok):
                status = await startup_recovery.run_startup_recovery()

            self.assertTrue(status["ok"], status)
            self.assertEqual(status["state"], "PASSED", status)
            self.assertTrue(await startup_recovery.startup_recovery_passed())
            circuit = await get_circuit_breaker_state()
            self.assertFalse(circuit["tripped"], circuit)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

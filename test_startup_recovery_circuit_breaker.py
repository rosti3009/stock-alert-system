from __future__ import annotations

import asyncio
import gc
import os
import tempfile
import time
import unittest
from contextlib import closing
from unittest.mock import patch

import config
import database
import execution_sync
import startup_recovery
from circuit_breaker import (
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

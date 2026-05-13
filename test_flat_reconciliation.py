from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import config
import database
import reconciliation


class FakeClient:
    def __init__(self, positions=None):
        self.positions = positions or []
        self.connected = False
        self.ib = SimpleNamespace(orders=[])

    def connect(self):
        self.connected = True
        return True

    def is_connected(self):
        return self.connected

    def get_positions(self):
        return self.positions

    def disconnect(self):
        self.connected = False


def make_tws_position(symbol: str, quantity: float):
    return SimpleNamespace(
        contract=SimpleNamespace(symbol=symbol),
        position=quantity,
    )


def fetch_position(db_path: str, symbol: str) -> dict | None:
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT * FROM positions WHERE symbol = ?",
            (symbol.strip().upper(),),
        ).fetchone()
        return dict(row) if row else None


class FlatTwsReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original = {
            "config_DB_PATH": config.DB_PATH,
            "database_DB_PATH": database.DB_PATH,
            "TRADING_MODE": config.TRADING_MODE,
            "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
            "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
            "IBKR_PORT": config.IBKR_PORT,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.TRADING_MODE = "PAPER"
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.IBKR_PORT = 7497
        asyncio.run(database.init_db())

    def tearDown(self):
        config.DB_PATH = self.original["config_DB_PATH"]
        database.DB_PATH = self.original["database_DB_PATH"]
        config.TRADING_MODE = self.original["TRADING_MODE"]
        config.IBKR_PAPER_TRADING = self.original["IBKR_PAPER_TRADING"]
        config.IBKR_ENABLE_REAL_TRADING = self.original["IBKR_ENABLE_REAL_TRADING"]
        config.IBKR_PORT = self.original["IBKR_PORT"]
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def add_open_position(self, symbol: str, quantity: float = 2):
        return asyncio.run(database.add_position({
            "symbol": symbol,
            "buy_price": 100,
            "quantity": quantity,
        }))

    def test_dry_run_closes_nothing(self):
        self.add_open_position("AAPL", 2)
        client = FakeClient([])

        result = asyncio.run(reconciliation.close_db_positions_flat_in_tws(
            dry_run=True,
            ibkr_client=client,
        ))

        self.assertEqual(result["dry_run"], True)
        self.assertEqual(result["tws_positions_count"], 0)
        self.assertEqual(result["db_open_before"], 1)
        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["closed_symbols"], [])
        self.assertEqual(result["would_close_symbols"], ["AAPL"])
        self.assertEqual(result["remaining_issues"][0]["symbol"], "AAPL")

        position = fetch_position(self.tmp.name, "AAPL")
        self.assertEqual(position["status"], "OPEN")
        self.assertEqual(position["action"], "HOLD")
        journal = asyncio.run(database.get_trade_journal(limit=10, symbol="AAPL"))
        self.assertFalse(any(row["event_type"] == "POSITION_RECONCILED_CLOSED" for row in journal))
        self.assertEqual(client.ib.orders, [])

    def test_db_open_and_tws_flat_closes_locally(self):
        self.add_open_position("MSFT", 3)
        client = FakeClient([])

        result = asyncio.run(reconciliation.close_db_positions_flat_in_tws(
            ibkr_client=client,
        ))

        self.assertEqual(result["dry_run"], False)
        self.assertEqual(result["closed_count"], 1)
        self.assertEqual(result["closed_symbols"], ["MSFT"])
        self.assertEqual(result["remaining_issues"], [])

        position = fetch_position(self.tmp.name, "MSFT")
        self.assertEqual(position["status"], "CLOSED")
        self.assertEqual(position["action"], "RECONCILED_CLOSED")
        self.assertEqual(position["reason"], reconciliation.FLAT_RECONCILIATION_REASON)
        self.assertIsNotNone(position["closed_at"])
        self.assertIsNotNone(position["updated_at"])

        journal = asyncio.run(database.get_trade_journal(limit=10, symbol="MSFT"))
        self.assertTrue(
            any(row["event_type"] == "POSITION_RECONCILED_CLOSED" for row in journal)
        )
        self.assertEqual(client.ib.orders, [])

    def test_tws_open_position_is_not_closed_locally(self):
        self.add_open_position("NVDA", 4)
        client = FakeClient([make_tws_position("NVDA", 4)])

        result = asyncio.run(reconciliation.close_db_positions_flat_in_tws(
            ibkr_client=client,
        ))

        self.assertEqual(result["tws_positions_count"], 1)
        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["closed_symbols"], [])
        self.assertEqual(result["skipped_symbols"], ["NVDA"])
        self.assertEqual(result["remaining_issues"], [])

        position = fetch_position(self.tmp.name, "NVDA")
        self.assertEqual(position["status"], "OPEN")
        self.assertEqual(position["action"], "HOLD")
        self.assertEqual(client.ib.orders, [])

    def test_endpoint_runs_tws_reconciliation_in_worker_thread(self):
        import main

        self.add_open_position("AMD", 5)
        request_thread_id = threading.get_ident()

        class ThreadCheckingClient(FakeClient):
            def __init__(self):
                super().__init__([])
                self.connect_thread_id = None
                self.loop_available = False
                self.loop_running = True

            def connect(self):
                self.connect_thread_id = threading.get_ident()
                loop = asyncio.get_event_loop()
                self.loop_available = loop is not None
                self.loop_running = loop.is_running()
                return super().connect()

        client = ThreadCheckingClient()
        request = SimpleNamespace(query_params={"dry_run": "true"})

        with patch.object(reconciliation, "IBKRClient", return_value=client):
            response = asyncio.run(main.api_close_db_positions_flat_in_tws(
                request=request,
                dry_run=False,
            ))

        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["dry_run"], True)
        self.assertEqual(payload["closed_count"], 0)
        self.assertEqual(payload["would_close_symbols"], ["AMD"])
        self.assertIsNotNone(client.connect_thread_id)
        self.assertNotEqual(client.connect_thread_id, request_thread_id)
        self.assertTrue(client.loop_available)
        self.assertFalse(client.loop_running)
        self.assertEqual(client.ib.orders, [])

        position = fetch_position(self.tmp.name, "AMD")
        self.assertEqual(position["status"], "OPEN")

    def test_live_trading_blocks_endpoint(self):
        import main

        self.add_open_position("TSLA", 1)
        config.IBKR_ENABLE_REAL_TRADING = True
        request = SimpleNamespace(query_params={"dry_run": "true"})

        response = asyncio.run(main.api_close_db_positions_flat_in_tws(
            request=request,
            dry_run=False,
        ))
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(payload["status"], "blocked")
        self.assertIn("LIVE trading is enabled", payload["reason"])
        self.assertEqual(payload["closed_count"], 0)
        self.assertEqual(payload["dry_run"], True)

        position = fetch_position(self.tmp.name, "TSLA")
        self.assertEqual(position["status"], "OPEN")


if __name__ == "__main__":
    unittest.main()

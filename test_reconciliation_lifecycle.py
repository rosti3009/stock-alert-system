from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import unittest

import config
import database
import reconciliation


CREATE_TWS_POSITIONS = """
CREATE TABLE IF NOT EXISTS tws_positions (
    symbol TEXT PRIMARY KEY,
    quantity REAL,
    avg_cost REAL,
    market_price REAL,
    market_value REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    account TEXT,
    updated_at TEXT
)
"""


class ReconciliationLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_config_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        asyncio.run(database.init_db())
        asyncio.run(reconciliation.init_reconciliation_db())

    def tearDown(self):
        config.DB_PATH = self.original_config_db_path
        database.DB_PATH = self.original_database_db_path
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def add_db_position(self, symbol: str = "AAPL", quantity: float = 10) -> None:
        asyncio.run(database.add_position({
            "symbol": symbol,
            "buy_price": 100,
            "quantity": quantity,
            "reason": "lifecycle test",
        }))

    def upsert_tws_position(self, symbol: str = "AAPL", quantity: float = 10) -> None:
        with sqlite3.connect(self.tmp.name) as db:
            db.execute(CREATE_TWS_POSITIONS)
            db.execute(
                """
                INSERT INTO tws_positions (
                    symbol, quantity, avg_cost, market_price, market_value,
                    unrealized_pnl, realized_pnl, account, updated_at
                )
                VALUES (?, ?, 100, 100, ?, 0, 0, 'DU123', ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    quantity = excluded.quantity,
                    market_value = excluded.market_value,
                    updated_at = excluded.updated_at
                """,
                (symbol, quantity, quantity * 100, reconciliation.now_iso()),
            )
            db.commit()

    def issue_statuses(self) -> list[str]:
        with sqlite3.connect(self.tmp.name) as db:
            return [row[0] for row in db.execute(
                "SELECT status FROM reconciliation_issues ORDER BY id ASC"
            ).fetchall()]

    def test_issue_opens_resolves_and_history_is_kept(self):
        self.add_db_position(quantity=10)

        first = asyncio.run(reconciliation.run_reconciliation_once())
        self.assertFalse(first["ok"])
        self.assertEqual(first["open_count"], 1)
        self.assertEqual(first["issues_count"], 1)
        self.assertEqual(first["issues"][0]["status"], "OPEN")
        self.assertEqual(self.issue_statuses(), ["OPEN"])

        self.upsert_tws_position(quantity=10)

        second = asyncio.run(reconciliation.run_reconciliation_once())
        self.assertTrue(second["ok"])
        self.assertEqual(second["open_count"], 0)
        self.assertEqual(second["resolved_count"], 1)
        self.assertEqual(second["issues"], [])
        self.assertEqual(self.issue_statuses(), ["RESOLVED"])

        history = asyncio.run(reconciliation.get_reconciliation_history())
        self.assertEqual(history["count"], 1)
        self.assertEqual(history["issues"][0]["status"], "RESOLVED")
        self.assertIsNotNone(history["issues"][0]["resolved_at"])

    def test_recurring_issue_creates_new_open_row_without_deleting_history(self):
        self.add_db_position(quantity=10)
        asyncio.run(reconciliation.run_reconciliation_once())
        self.upsert_tws_position(quantity=10)
        asyncio.run(reconciliation.run_reconciliation_once())

        self.upsert_tws_position(quantity=5)
        recurring = asyncio.run(reconciliation.run_reconciliation_once())

        self.assertFalse(recurring["ok"])
        self.assertEqual(recurring["open_count"], 1)
        self.assertEqual(recurring["resolved_count"], 1)
        self.assertEqual(self.issue_statuses(), ["RESOLVED", "OPEN"])

        history = asyncio.run(reconciliation.get_reconciliation_history())
        self.assertEqual(history["count"], 2)
        self.assertEqual(history["counters"]["open"], 1)
        self.assertEqual(history["counters"]["resolved"], 1)
        self.assertEqual(history["counters"]["auto_fixed"], 0)


if __name__ == "__main__":
    unittest.main()

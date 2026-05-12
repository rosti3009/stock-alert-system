from __future__ import annotations

from contextlib import closing
import asyncio
import gc
import os
import time
import sqlite3
import tempfile
import unittest

import config
import database
import reconciliation
from reconciliation_lifecycle import get_reconciliation_history, get_reconciliation_status


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
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.execute(CREATE_TWS_POSITIONS)
            db.commit()

    def tearDown(self):
        config.DB_PATH = self.original_config_db_path
        database.DB_PATH = self.original_database_db_path
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

    def set_tws_position(self, symbol: str, quantity: float) -> None:
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.execute(
                """
                INSERT INTO tws_positions (symbol, quantity, avg_cost, market_price, updated_at)
                VALUES (?, ?, 100, 100, '2026-05-12T00:00:00+00:00')
                ON CONFLICT(symbol) DO UPDATE SET
                    quantity = excluded.quantity,
                    updated_at = excluded.updated_at
                """,
                (symbol, quantity),
            )
            db.commit()

    def fetch_issue_rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                """
                SELECT id, symbol, issue_type, status, created_at, resolved_at
                FROM reconciliation_issues
                ORDER BY id ASC
                """
            ).fetchall()

    def test_reconciliation_preserves_history_and_resolves_disappeared_issues(self):
        async def scenario():
            await database.add_position({
                "symbol": "AAPL",
                "buy_price": 100.0,
                "quantity": 10,
                "reason": "test position",
            })

            first = await reconciliation.run_reconciliation_once()
            self.assertFalse(first["ok"], first)
            self.assertEqual(first["issues_count"], 1, first)
            self.assertEqual(first["issues"][0]["status"], "OPEN", first)

            repeated = await reconciliation.run_reconciliation_once()
            self.assertEqual(repeated["issues_count"], 1, repeated)
            self.assertEqual(len(self.fetch_issue_rows()), 1)

            self.set_tws_position("AAPL", 10)
            resolved = await reconciliation.run_reconciliation_once()
            self.assertTrue(resolved["ok"], resolved)
            self.assertEqual(resolved["issues_count"], 0, resolved)

            rows = self.fetch_issue_rows()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "RESOLVED")
            self.assertIsNotNone(rows[0]["resolved_at"])

            status = await get_reconciliation_status()
            self.assertEqual(status["issues"], [])
            self.assertEqual(status["counters"]["open"], 0)
            self.assertEqual(status["counters"]["resolved"], 1)

            self.set_tws_position("AAPL", 0)
            recurring = await reconciliation.run_reconciliation_once()
            self.assertFalse(recurring["ok"], recurring)
            self.assertEqual(recurring["issues_count"], 1, recurring)

            history = await get_reconciliation_history()
            self.assertEqual(history["counters"]["open"], 1, history)
            self.assertEqual(history["counters"]["resolved"], 1, history)
            self.assertEqual(history["counters"]["total"], 2, history)
            self.assertEqual(len(self.fetch_issue_rows()), 2)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

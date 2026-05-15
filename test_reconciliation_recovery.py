from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

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


class ReconciliationRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.sqlite3")
        self.original_config_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        config.DB_PATH = self.db_path
        database.DB_PATH = self.db_path
        asyncio.run(database.init_db())
        asyncio.run(reconciliation.init_reconciliation_db())
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(CREATE_TWS_POSITIONS)
            db.commit()

    def tearDown(self):
        config.DB_PATH = self.original_config_db_path
        database.DB_PATH = self.original_database_db_path
        self.tmpdir.cleanup()

    def seed_closed_position_and_live_tws(self, quantity: float = 7.0) -> None:
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(
                """
                INSERT INTO positions (
                    symbol, buy_price, quantity, buy_date, current_price,
                    profit_amount, profit_percent, stop_loss, take_profit_1,
                    take_profit_2, status, action, reason, notes, source,
                    created_at, updated_at, closed_at
                )
                VALUES (
                    'MSFT', 100, 0, '2026-05-10T00:00:00+00:00', 98,
                    -14, -2, 92, 108, 116, 'CLOSED', 'CLOSED',
                    'Closed manually', 'original note', 'TEST',
                    '2026-05-10T00:00:00+00:00',
                    '2026-05-11T00:00:00+00:00',
                    '2026-05-11T00:00:00+00:00'
                )
                """
            )
            db.execute(
                """
                INSERT INTO tws_positions (
                    symbol, quantity, avg_cost, market_price, market_value,
                    unrealized_pnl, realized_pnl, account, updated_at
                )
                VALUES (
                    'MSFT', ?, 101, 104, 728, 21, 0, 'DU123',
                    '2026-05-12T00:00:00+00:00'
                )
                """,
                (quantity,),
            )
            db.execute(
                """
                INSERT INTO reconciliation_issues (
                    symbol, issue_type, severity, db_quantity, tws_quantity,
                    execution_quantity, details, status, created_at
                )
                VALUES (
                    'MSFT', 'TWS_OPEN_BUT_DB_FLAT', 'HIGH', 0, ?, 0,
                    'pre-existing mismatch', 'OPEN',
                    '2026-05-12T00:00:00+00:00'
                )
                """,
                (quantity,),
            )
            db.commit()

    def fetch_position_rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.db_path)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                "SELECT * FROM positions WHERE symbol = 'MSFT' ORDER BY id"
            ).fetchall()

    def fetch_reconciliation_rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.db_path)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                """
                SELECT * FROM reconciliation_issues
                WHERE symbol = 'MSFT'
                ORDER BY id
                """
            ).fetchall()

    def fetch_journal_rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.db_path)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                """
                SELECT * FROM trade_journal
                WHERE symbol = 'MSFT'
                ORDER BY id
                """
            ).fetchall()

    def test_closed_db_live_tws_recovers_open_position_and_resolves_issue(self):
        self.seed_closed_position_and_live_tws()

        async def scenario():
            with patch.object(
                reconciliation,
                "IBKRClient",
                side_effect=AssertionError("no orders or TWS calls"),
            ):
                result = await reconciliation.run_reconciliation_once()

            self.assertTrue(result["ok"], result)
            self.assertEqual(result["issues_count"], 0, result)
            self.assertEqual(result["recovered_positions_count"], 1, result)

            rows = self.fetch_position_rows()
            self.assertEqual(len(rows), 1)
            position = dict(rows[0])
            self.assertEqual(position["status"], "OPEN")
            self.assertIsNone(position["closed_at"])
            self.assertEqual(position["quantity"], 7.0)
            self.assertEqual(position["current_price"], 104.0)
            self.assertEqual(position["source"], reconciliation.RECOVERY_SOURCE)
            self.assertIn(reconciliation.RECOVERY_NOTE, position["notes"])
            self.assertEqual(position["recovery_source_position_id"], position["id"])

            open_positions = await database.get_open_positions()
            self.assertEqual([p["symbol"] for p in open_positions], ["MSFT"])
            invested_capital = sum(
                float(p.get("buy_price") or 0) * float(p.get("quantity") or 0)
                for p in open_positions
            )
            self.assertEqual(invested_capital, 700.0)

            issue_rows = self.fetch_reconciliation_rows()
            self.assertEqual(len(issue_rows), 1)
            self.assertEqual(issue_rows[0]["status"], "RESOLVED")
            self.assertIsNotNone(issue_rows[0]["resolved_at"])

            journal_rows = self.fetch_journal_rows()
            self.assertEqual(len(journal_rows), 1)
            self.assertEqual(
                journal_rows[0]["event_type"],
                "POSITION_RECONCILIATION_RECOVERED",
            )

            repeated = await reconciliation.run_reconciliation_once()
            self.assertTrue(repeated["ok"], repeated)
            self.assertEqual(repeated["recovered_positions_count"], 0, repeated)
            self.assertEqual(len(self.fetch_position_rows()), 1)

        asyncio.run(scenario())

    def test_non_positive_tws_quantity_does_not_reopen_closed_position(self):
        self.seed_closed_position_and_live_tws(quantity=0)

        async def scenario():
            result = await reconciliation.run_reconciliation_once()
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["recovered_positions_count"], 0, result)
            rows = self.fetch_position_rows()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "CLOSED")
            self.assertEqual(await database.get_open_positions(), [])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

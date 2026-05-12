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


def fetch_position(db_path: str, symbol: str) -> dict | None:
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT * FROM positions WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        return dict(row) if row else None


class AdoptTwsPositionsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original = {
            "config_DB_PATH": config.DB_PATH,
            "database_DB_PATH": database.DB_PATH,
            "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
            "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
            "IBKR_PORT": config.IBKR_PORT,
            "TRADING_MODE": config.TRADING_MODE,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.IBKR_PORT = 7497
        config.TRADING_MODE = "PAPER"
        asyncio.run(database.init_db())

    def tearDown(self):
        config.DB_PATH = self.original["config_DB_PATH"]
        database.DB_PATH = self.original["database_DB_PATH"]
        config.IBKR_PAPER_TRADING = self.original["IBKR_PAPER_TRADING"]
        config.IBKR_ENABLE_REAL_TRADING = self.original["IBKR_ENABLE_REAL_TRADING"]
        config.IBKR_PORT = self.original["IBKR_PORT"]
        config.TRADING_MODE = self.original["TRADING_MODE"]
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def insert_tws_position(
        self,
        symbol: str,
        quantity: float,
        avg_cost: float,
        market_price: float,
    ) -> None:
        with sqlite3.connect(self.tmp.name) as db:
            db.execute(CREATE_TWS_POSITIONS)
            db.execute(
                """
                INSERT INTO tws_positions (
                    symbol, quantity, avg_cost, market_price, market_value,
                    unrealized_pnl, realized_pnl, account, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 12.34, 0, 'DU123', ?)
                """,
                (
                    symbol,
                    quantity,
                    avg_cost,
                    market_price,
                    quantity * market_price,
                    reconciliation.now_iso(),
                ),
            )
            db.commit()

    def test_adopts_tws_position_missing_from_local_open_positions(self):
        self.insert_tws_position("AAPL", 5, 101.25, 103.5)

        result = asyncio.run(reconciliation.adopt_tws_positions_as_baseline())

        self.assertEqual(result["adopted_count"], 1)
        self.assertEqual(result["symbols_adopted"], ["AAPL"])
        self.assertEqual(result["remaining_reconciliation_issues_count"], 0)

        position = fetch_position(self.tmp.name, "AAPL")
        self.assertIsNotNone(position)
        self.assertEqual(position["status"], "OPEN")
        self.assertEqual(position["source"], "TWS_BASELINE_ADOPTED")
        self.assertEqual(position["reason"], "Adopted from existing TWS position")
        self.assertEqual(position["quantity"], 5)
        self.assertEqual(position["buy_price"], 101.25)
        self.assertEqual(position["current_price"], 103.5)

        journal = asyncio.run(database.get_trade_journal(limit=10, symbol="AAPL"))
        self.assertTrue(
            any(row["event_type"] == "POSITION_BASELINE_ADOPTED" for row in journal)
        )

    def test_skips_position_that_already_exists_open(self):
        self.insert_tws_position("MSFT", 3, 200, 205)
        asyncio.run(database.add_position({
            "symbol": "MSFT",
            "buy_price": 200,
            "quantity": 3,
        }))

        result = asyncio.run(reconciliation.adopt_tws_positions_as_baseline())

        self.assertEqual(result["adopted_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertEqual(result["symbols_skipped"], ["MSFT"])
        self.assertEqual(
            result["skipped_reasons"]["MSFT"],
            "Position already exists as OPEN in local DB",
        )

    def test_adoption_is_blocked_when_real_trading_enabled(self):
        self.insert_tws_position("NVDA", 1, 900, 905)
        config.IBKR_ENABLE_REAL_TRADING = True

        with self.assertRaisesRegex(RuntimeError, "LIVE trading is enabled"):
            asyncio.run(reconciliation.adopt_tws_positions_as_baseline())

        self.assertIsNone(fetch_position(self.tmp.name, "NVDA"))

    def test_adoption_is_blocked_when_not_paper_port(self):
        self.insert_tws_position("TSLA", 2, 250, 255)
        config.IBKR_PORT = 7496

        with self.assertRaisesRegex(RuntimeError, "not Paper port 7497"):
            asyncio.run(reconciliation.adopt_tws_positions_as_baseline())

        self.assertIsNone(fetch_position(self.tmp.name, "TSLA"))


if __name__ == "__main__":
    unittest.main()

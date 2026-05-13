from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import unittest

import account_sync
import config
import database
import main
import portfolio_risk_engine


class PaperSessionResetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original = {
            "database_DB_PATH": database.DB_PATH,
            "config_DB_PATH": config.DB_PATH,
            "TRADING_MODE": config.TRADING_MODE,
            "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
            "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
            "VIRTUAL_TRADING_CAPITAL_USD": config.VIRTUAL_TRADING_CAPITAL_USD,
            "database_VIRTUAL_TRADING_CAPITAL_USD": database.VIRTUAL_TRADING_CAPITAL_USD,
        }
        database.DB_PATH = self.tmp.name
        config.DB_PATH = self.tmp.name
        config.TRADING_MODE = "PAPER"
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.VIRTUAL_TRADING_CAPITAL_USD = 5000.0
        database.VIRTUAL_TRADING_CAPITAL_USD = 5000.0
        asyncio.run(database.init_db())
        asyncio.run(account_sync.init_account_sync_db())

    def tearDown(self):
        database.DB_PATH = self.original["database_DB_PATH"]
        config.DB_PATH = self.original["config_DB_PATH"]
        config.TRADING_MODE = self.original["TRADING_MODE"]
        config.IBKR_PAPER_TRADING = self.original["IBKR_PAPER_TRADING"]
        config.IBKR_ENABLE_REAL_TRADING = self.original["IBKR_ENABLE_REAL_TRADING"]
        config.VIRTUAL_TRADING_CAPITAL_USD = self.original["VIRTUAL_TRADING_CAPITAL_USD"]
        database.VIRTUAL_TRADING_CAPITAL_USD = self.original["database_VIRTUAL_TRADING_CAPITAL_USD"]
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def insert_closed_position(self, symbol: str, pnl: float) -> None:
        with sqlite3.connect(self.tmp.name) as db:
            db.execute(
                """
                INSERT INTO positions (
                    symbol, buy_price, quantity, current_price, profit_amount,
                    profit_percent, status, source, created_at, updated_at, closed_at
                )
                VALUES (?, 100, 1, ?, ?, ?, 'CLOSED', 'paper', ?, ?, ?)
                """,
                (
                    symbol,
                    100 + pnl,
                    pnl,
                    pnl,
                    "2026-05-13T12:00:00+00:00",
                    "2026-05-13T12:01:00+00:00",
                    "2026-05-13T12:02:00+00:00",
                ),
            )
            db.commit()

    def insert_execution_and_equity(self) -> None:
        with sqlite3.connect(self.tmp.name) as db:
            db.execute(
                """
                INSERT INTO execution_history (
                    exec_id, symbol, side, quantity, price, realized_pnl, time, created_at
                )
                VALUES ('E1', 'AAPL', 'SLD', 1, 110, -125, ?, ?)
                """,
                (database.now_iso(), database.now_iso()),
            )
            db.execute(
                """
                INSERT INTO equity_curve (
                    timestamp, account, net_liquidation, total_cash, buying_power,
                    unrealized_pnl, realized_pnl, source
                )
                VALUES (?, 'DU12345', 5250, 5250, 10000, 0, 250, 'account_sync')
                """,
                (database.now_iso(),),
            )
            db.commit()

    def test_reset_blocks_when_open_positions_exist(self):
        asyncio.run(database.add_position({"symbol": "MSFT", "buy_price": 100, "quantity": 1}))

        result = asyncio.run(database.reset_active_paper_session())

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["open_positions"], 1)
        self.assertIsNone(asyncio.run(database.get_active_paper_session(create_if_missing=False)))

    def test_reset_starts_new_session_and_preserves_history(self):
        self.insert_closed_position("AAPL", 300.0)
        self.insert_closed_position("MSFT", -50.0)
        self.insert_execution_and_equity()
        asyncio.run(database.safe_record_trade_journal_event({"event_type": "TEST", "source_module": "test"}))
        asyncio.run(database.set_app_state("portfolio_risk_state", "BLOCK_NEW_BUYS"))
        asyncio.run(database.set_app_state("portfolio_risk_latest", json.dumps({"alerts": ["old"]})))
        asyncio.run(database.set_app_state("scan_offset", "123"))

        result = asyncio.run(database.reset_active_paper_session())

        self.assertEqual(result["status"], "reset")
        self.assertEqual(result["orders_submitted"], 0)
        session = result["session"]
        self.assertEqual(session["realized_pnl_baseline"], 250.0)
        self.assertEqual(session["daily_realized_pnl_baseline"], -125.0)
        self.assertEqual(session["session_start_equity"], 5250.0)
        self.assertEqual(asyncio.run(database.get_realized_pnl()), 0.0)
        self.assertEqual(asyncio.run(portfolio_risk_engine.get_daily_realized_pnl()), 0.0)
        self.assertIsNone(asyncio.run(database.get_app_state("portfolio_risk_state")))
        self.assertIsNone(asyncio.run(database.get_app_state("portfolio_risk_latest")))
        self.assertIsNone(asyncio.run(database.get_app_state("scan_offset")))

        performance = asyncio.run(database.get_performance_summary())
        self.assertEqual(performance["total_trades"], 2)
        self.assertEqual(performance["total_pnl"], 250.0)
        journal = asyncio.run(database.get_trade_journal(limit=10))
        self.assertEqual({row["event_type"] for row in journal}, {"TEST", "PAPER_SESSION_RESET"})
        equity_curve = asyncio.run(account_sync.get_equity_curve())
        self.assertEqual(equity_curve[0]["source"], "paper_session_baseline")
        self.assertEqual(equity_curve[0]["net_liquidation"], 5250.0)
        self.assertEqual(len(asyncio.run(account_sync.get_equity_curve(session_only=False))), 1)

    def test_endpoint_is_paper_only(self):
        config.IBKR_PAPER_TRADING = False

        response = asyncio.run(main.api_paper_reset_session())
        payload = json.loads(response.body)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(payload["status"], "blocked")
        self.assertIn("IBKR_PAPER_TRADING is false", payload["reason"])
        self.assertEqual(payload["orders_submitted"], 0)


if __name__ == "__main__":
    unittest.main()

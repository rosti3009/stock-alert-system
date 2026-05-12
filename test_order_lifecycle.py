from __future__ import annotations

from contextlib import closing
import asyncio
import gc
import os
import sqlite3
import tempfile
import time
import unittest

import config
import database
import order_lifecycle


class OrderLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_config_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        asyncio.run(database.init_db())

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

    def fetch_journal_events(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                """
                SELECT event_type, symbol, decision, reason
                FROM trade_journal
                ORDER BY id ASC
                """
            ).fetchall()

    def test_records_previous_state_and_journals_important_transitions(self):
        async def scenario():
            await order_lifecycle.record_order_lifecycle_event({
                "symbol": "aapl",
                "side": "BUY",
                "quantity": 3,
                "price": 123.45,
                "order_id": 101,
                "perm_id": 9001,
                "client_id": 7,
                "source_module": "test_order_lifecycle",
                "state": order_lifecycle.OrderState.CREATED,
                "reason": "local create",
                "raw_payload": {"step": "created"},
            })
            await order_lifecycle.record_order_lifecycle_event({
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 3,
                "price": 123.45,
                "order_id": 101,
                "perm_id": 9001,
                "client_id": 7,
                "source_module": "test_order_lifecycle",
                "state": "SUBMITTED",
                "reason": "submitted to TWS",
                "raw_payload": {"step": "submitted"},
            })
            await order_lifecycle.record_order_lifecycle_event({
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 3,
                "price": 123.45,
                "order_id": 101,
                "perm_id": 9001,
                "client_id": 7,
                "source_module": "test_order_lifecycle",
                "state": "FILLED",
                "reason": "filled in TWS",
                "raw_payload": {"step": "filled"},
            })

            rows = await order_lifecycle.get_order_lifecycle_events(limit=10, symbol="aapl")
            self.assertEqual([row["state"] for row in rows], ["FILLED", "SUBMITTED", "CREATED"])
            self.assertEqual(rows[0]["previous_state"], "SUBMITTED")
            self.assertEqual(rows[1]["previous_state"], "CREATED")
            self.assertIsNone(rows[2]["previous_state"])

            latest = await order_lifecycle.get_latest_order_lifecycle_states(limit=10)
            self.assertEqual(len(latest), 1, latest)
            self.assertEqual(latest[0]["state"], "FILLED")

            journal = self.fetch_journal_events()
            self.assertEqual([row["event_type"] for row in journal], ["ORDER_SUBMITTED", "ORDER_FILLED"])
            self.assertEqual(journal[-1]["symbol"], "AAPL")
            self.assertEqual(journal[-1]["decision"], "FILLED")

        asyncio.run(scenario())

    def test_maps_ibkr_statuses_to_lifecycle_states(self):
        self.assertEqual(
            order_lifecycle.map_ibkr_status_to_state("Submitted"),
            order_lifecycle.OrderState.ACKNOWLEDGED,
        )
        self.assertEqual(
            order_lifecycle.map_ibkr_status_to_state("Submitted", filled=1, remaining=2),
            order_lifecycle.OrderState.PARTIALLY_FILLED,
        )
        self.assertEqual(
            order_lifecycle.map_ibkr_status_to_state("Filled", filled=3, remaining=0),
            order_lifecycle.OrderState.FILLED,
        )
        self.assertEqual(
            order_lifecycle.map_ibkr_status_to_state("Cancelled"),
            order_lifecycle.OrderState.CANCELLED,
        )


if __name__ == "__main__":
    unittest.main()

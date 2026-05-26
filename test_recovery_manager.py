from __future__ import annotations

from contextlib import closing
import asyncio
import gc
import os
import time
import sqlite3
import tempfile
import unittest
from datetime import timedelta

import config
import database
import recovery_manager


CREATE_TWS_HEARTBEAT = """
CREATE TABLE IF NOT EXISTS tws_heartbeat (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    connected INTEGER DEFAULT 0,
    account TEXT,
    last_sync_at TEXT,
    error TEXT
)
"""

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

CREATE_RECONCILIATION_ISSUES = """
CREATE TABLE IF NOT EXISTS reconciliation_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    issue_type TEXT,
    severity TEXT,
    db_quantity REAL,
    tws_quantity REAL,
    execution_quantity REAL,
    details TEXT,
    status TEXT DEFAULT 'OPEN',
    created_at TEXT,
    resolved_at TEXT
)
"""

CREATE_BROKER_SYNC_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS broker_sync_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    synced_at TEXT,
    ok INTEGER,
    connected INTEGER,
    account TEXT,
    net_liquidation REAL,
    total_cash REAL,
    available_funds REAL,
    buying_power REAL,
    positions_json TEXT,
    open_orders_json TEXT,
    executions_json TEXT,
    errors_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""


class RecoveryManagerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original_db_path = config.DB_PATH
        self.original_database_db_path = database.DB_PATH
        self.original_thresholds = {
            "RECOVERY_HEARTBEAT_DEGRADED_SECONDS": config.RECOVERY_HEARTBEAT_DEGRADED_SECONDS,
            "RECOVERY_HEARTBEAT_BLOCK_BUY_SECONDS": config.RECOVERY_HEARTBEAT_BLOCK_BUY_SECONDS,
            "RECOVERY_POSITION_STALE_SECONDS": config.RECOVERY_POSITION_STALE_SECONDS,
        }
        config.DB_PATH = self.tmp.name
        database.DB_PATH = self.tmp.name
        config.RECOVERY_HEARTBEAT_DEGRADED_SECONDS = 60
        config.RECOVERY_HEARTBEAT_BLOCK_BUY_SECONDS = 120
        config.RECOVERY_POSITION_STALE_SECONDS = 180
        asyncio.run(database.init_db())

    def tearDown(self):
        config.DB_PATH = self.original_db_path
        database.DB_PATH = self.original_database_db_path
        for key, value in self.original_thresholds.items():
            setattr(config, key, value)
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

    def write_heartbeat(self, *, connected: bool = True, age_seconds: int = 0, error: str | None = None):
        last_sync_at = (recovery_manager.now_utc() - timedelta(seconds=age_seconds)).isoformat()
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.execute(CREATE_TWS_HEARTBEAT)
            db.execute(
                """
                INSERT INTO tws_heartbeat (id, connected, account, last_sync_at, error)
                VALUES (1, ?, 'DU123', ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    connected = excluded.connected,
                    account = excluded.account,
                    last_sync_at = excluded.last_sync_at,
                    error = excluded.error
                """,
                (1 if connected else 0, last_sync_at, error),
            )
            db.commit()

    def add_reconciliation_issue(self, severity: str = "HIGH"):
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.execute(CREATE_RECONCILIATION_ISSUES)
            db.execute(
                """
                INSERT INTO reconciliation_issues (
                    symbol, issue_type, severity, db_quantity, tws_quantity,
                    execution_quantity, details, status, created_at
                )
                VALUES ('AAPL', 'DB_OPEN_BUT_TWS_FLAT', ?, 10, 0, 10, 'mismatch', 'OPEN', ?)
                """,
                (severity, recovery_manager.now_iso()),
            )
            db.commit()

    def test_healthy_state_allows_buy(self):
        self.write_heartbeat(connected=True, age_seconds=5)

        status = asyncio.run(recovery_manager.run_recovery_check())

        self.assertEqual(status["state"], "HEALTHY")
        self.assertFalse(status["buy_blocked"])
        recovery_manager.require_buy_allowed("test")

    def test_stale_heartbeat_enters_recovery_and_blocks_buy(self):
        self.write_heartbeat(connected=True, age_seconds=90)

        status = asyncio.run(recovery_manager.run_recovery_check())

        self.assertEqual(status["state"], "RECOVERY")
        self.assertTrue(status["buy_blocked"])
        with self.assertRaisesRegex(RuntimeError, "BUY blocked by recovery manager"):
            recovery_manager.require_buy_allowed("test")

    def test_very_stale_heartbeat_enters_block_buy(self):
        self.write_heartbeat(connected=True, age_seconds=180)

        status = asyncio.run(recovery_manager.run_recovery_check())

        self.assertEqual(status["state"], "BLOCK_BUY")
        self.assertTrue(status["buy_blocked"])

    def test_disconnected_heartbeat_blocks_buy(self):
        self.write_heartbeat(connected=False, age_seconds=10, error="socket closed")

        status = asyncio.run(recovery_manager.run_recovery_check())

        self.assertEqual(status["state"], "BLOCK_BUY")
        self.assertTrue(status["buy_blocked"])
        self.assertIn("disconnected", status["buy_block_reason"])

    def test_high_reconciliation_issue_requires_manual_review(self):
        self.write_heartbeat(connected=True, age_seconds=5)
        self.add_reconciliation_issue("HIGH")

        status = asyncio.run(recovery_manager.run_recovery_check())

        self.assertEqual(status["state"], "MANUAL_REVIEW_REQUIRED")
        self.assertTrue(status["buy_blocked"])

    def test_medium_reconciliation_issue_is_degraded_warning_only(self):
        self.write_heartbeat(connected=True, age_seconds=5)
        self.add_reconciliation_issue("MEDIUM")

        status = asyncio.run(recovery_manager.run_recovery_check())

        self.assertEqual(status["state"], "DEGRADED")
        self.assertFalse(status["buy_blocked"])

    def test_state_transitions_are_journaled(self):
        self.write_heartbeat(connected=True, age_seconds=5)

        status = asyncio.run(recovery_manager.run_recovery_check())
        rows = asyncio.run(database.get_trade_journal(limit=10))

        self.assertEqual(status["state"], "HEALTHY")
        self.assertTrue(any(row["event_type"] == "RECOVERY_STATE_CHANGED" for row in rows))

    def _seed_fresh_broker_sync_fallback(self):
        with closing(sqlite3.connect(self.tmp.name)) as db:
            db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
            db.execute("INSERT INTO broker_sync_snapshots (synced_at, ok, connected, account, executions_json, errors_json) VALUES (?,1,1,'DU123','[]','[]')", (recovery_manager.now_iso(),))
            db.execute("INSERT INTO app_state (key, value) VALUES ('watchdog_status', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", ('{"stale_data":{"broker_sync":false,"tws_mirror":true,"execution_sync":true}}',))
            db.commit()

    def test_stale_tws_heartbeat_with_fresh_broker_sync_allows_buy(self):
        self.write_heartbeat(connected=True, age_seconds=180)
        self._seed_fresh_broker_sync_fallback()
        status = asyncio.run(recovery_manager.run_recovery_check())
        self.assertFalse(status["buy_blocked"], status)

    def test_disconnected_broker_sync_and_stale_heartbeat_blocks_buy(self):
        self.write_heartbeat(connected=True, age_seconds=180)
        status = asyncio.run(recovery_manager.run_recovery_check())
        self.assertTrue(status["buy_blocked"], status)


if __name__ == "__main__":
    unittest.main()

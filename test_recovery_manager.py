from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timedelta, timezone

import config
import database
import recovery_manager


async def write_heartbeat(connected: bool, last_sync_at: str, error: str | None = None) -> None:
    import aiosqlite

    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tws_heartbeat (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                connected INTEGER DEFAULT 0,
                account TEXT,
                last_sync_at TEXT,
                error TEXT
            )
            """
        )
        await db.execute(
            """
            INSERT INTO tws_heartbeat (id, connected, account, last_sync_at, error)
            VALUES (1, ?, 'DU12345', ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                connected = excluded.connected,
                last_sync_at = excluded.last_sync_at,
                error = excluded.error
            """,
            (1 if connected else 0, last_sync_at, error),
        )
        await db.commit()


async def main() -> None:
    original_config_db_path = config.DB_PATH
    original_database_db_path = database.DB_PATH
    fd, path = tempfile.mkstemp(prefix="recovery_manager_", suffix=".db")
    os.close(fd)

    try:
        config.DB_PATH = path
        database.DB_PATH = path

        await database.init_db()
        await recovery_manager.init_recovery_db()

        stale = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        await write_heartbeat(True, stale)

        stale_result = await recovery_manager.RecoveryManager().check_once()
        stale_state = stale_result["state"]
        assert stale_state["recovery_mode"] == recovery_manager.RECOVERY, stale_state
        assert stale_state["trading_allowed_after_recovery"] is False, stale_state

        try:
            await recovery_manager.require_recovery_healthy_for_buy("AAPL", {"test": "blocked"})
        except RuntimeError as exc:
            assert "BUY blocked while recovery state is RECOVERY" in str(exc), exc
        else:
            raise AssertionError("BUY was not blocked during recovery")

        blocked_events = await database.get_trade_journal(limit=10, symbol="AAPL")
        assert any(row["event_type"] == "BUY_BLOCKED_RECOVERY" for row in blocked_events), blocked_events

        fresh = datetime.now(timezone.utc).isoformat()
        await write_heartbeat(True, fresh)
        await recovery_manager.set_recovery_state(
            connection_status="CONNECTED",
            last_heartbeat_at=fresh,
            recovery_mode=recovery_manager.RECOVERY,
            recovery_reason="test recovery",
            trading_allowed_after_recovery=False,
        )

        async def successful_resync() -> dict:
            return {
                "account_sync": {"connected": True, "synced_at": fresh},
                "execution_sync": {"ok": True, "synced_at": fresh},
                "reconciliation_status": {"ok": True, "mismatches": [], "checked_at": fresh},
            }

        healthy_result = await recovery_manager.RecoveryManager(successful_resync).check_once()
        healthy_state = healthy_result["state"]
        assert healthy_state["recovery_mode"] == recovery_manager.HEALTHY, healthy_state
        assert healthy_state["trading_allowed_after_recovery"] is True, healthy_state

        await recovery_manager.set_recovery_state(
            connection_status="CONNECTED",
            last_heartbeat_at=fresh,
            recovery_mode=recovery_manager.RECOVERY,
            recovery_reason="test mismatch recovery",
            trading_allowed_after_recovery=False,
        )

        async def mismatch_resync() -> dict:
            return {
                "account_sync": {"connected": True, "synced_at": fresh},
                "execution_sync": {"ok": True, "synced_at": fresh},
                "reconciliation_status": {
                    "ok": False,
                    "mismatches": [{"symbol": "MSFT", "issue_type": "POSITION_EXECUTION_QUANTITY_MISMATCH"}],
                    "checked_at": fresh,
                },
            }

        review_result = await recovery_manager.RecoveryManager(mismatch_resync).check_once()
        review_state = review_result["state"]
        assert review_state["recovery_mode"] == recovery_manager.MANUAL_REVIEW_REQUIRED, review_state
        assert review_state["trading_allowed_after_recovery"] is False, review_state

        review_events = await database.get_trade_journal(limit=20)
        assert any(row["event_type"] == "MANUAL_REVIEW_REQUIRED" for row in review_events), review_events
        assert any(row["event_type"] == "RECOVERY_COMPLETED" for row in review_events), review_events

        print("recovery manager smoke test passed")

    finally:
        config.DB_PATH = original_config_db_path
        database.DB_PATH = original_database_db_path
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    asyncio.run(main())

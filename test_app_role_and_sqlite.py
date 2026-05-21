import asyncio
import sqlite3

import database
import main


def test_trading_jobs_disabled_in_dashboard(monkeypatch):
    monkeypatch.setattr(main.config, "APP_ROLE", "dashboard")
    monkeypatch.setattr(main.config, "TRADING_MODE", "OFF")
    monkeypatch.setattr(main.config, "AUTO_SEND_ORDERS", False)
    monkeypatch.setattr(main.config, "IBKR_PAPER_TRADING", False)
    assert main.trading_jobs_enabled() is False


def test_trading_jobs_enabled_in_local_trader(monkeypatch):
    monkeypatch.setattr(main.config, "APP_ROLE", "trader")
    monkeypatch.setattr(main.config, "TRADING_MODE", "PAPER_AUTO")
    monkeypatch.setattr(main.config, "AUTO_SEND_ORDERS", True)
    monkeypatch.setattr(main.config, "IBKR_PAPER_TRADING", True)
    assert main.trading_jobs_enabled() is True


def test_sqlite_pragmas_applied_sync(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    try:
        database.apply_sqlite_pragmas_sync(conn)
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        timeout = conn.execute("PRAGMA busy_timeout;").fetchone()[0]
        sync = conn.execute("PRAGMA synchronous;").fetchone()[0]
        assert str(mode).lower() == "wal"
        assert int(timeout) == 5000
        assert int(sync) in {1, 2}
    finally:
        conn.close()


async def _write_app_state_concurrently():
    await asyncio.gather(
        database.set_app_state("k1", "v1"),
        database.set_app_state("k2", "v2"),
    )


def test_app_state_lock_prevents_crash(tmp_path, monkeypatch):
    monkeypatch.setattr(database, "DB_PATH", str(tmp_path / "lock.db"))
    asyncio.run(database.init_db())
    asyncio.run(_write_app_state_concurrently())

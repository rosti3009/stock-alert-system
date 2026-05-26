import asyncio
import time

from fastapi.testclient import TestClient

import main
import database


class _DummyDB:
    async def fetch_all(self, *_args, **_kwargs):
        return []

    async def fetch_one(self, *_args, **_kwargs):
        return {
            "connected": 0,
            "synced_at": None,
            "positions_json": None,
            "open_orders_json": "bad-json",
            "executions_json": "[]",
            "errors_json": None,
        }

    async def save_broker_sync_snapshot(self, *_args, **_kwargs):
        return None


def test_orders_endpoint_returns_stable_json(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())
    payload = TestClient(main.app).get("/api/orders").json()
    assert isinstance(payload, dict)
    assert "ok" in payload
    assert "errors" in payload
    assert payload["count"] == 0


def test_broker_sync_status_handles_malformed_payload(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())
    payload = TestClient(main.app).get("/api/broker-sync/status").json()
    assert isinstance(payload, dict)
    assert payload["open_orders_count"] == 0
    assert payload["executions_count"] == 0
    assert payload["positions_count"] == 0
    assert "source" in payload


def test_broker_sync_run_handles_none_result(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())

    async def _none_result():
        return None

    monkeypatch.setattr(main.broker_sync, "run_broker_sync_once", _none_result)
    payload = TestClient(main.app).post("/api/broker-sync/run").json()
    assert isinstance(payload, dict)
    assert payload["ok"] is False
    assert isinstance(payload["errors"], list)
    assert "source" in payload


def test_database_fetch_one_returns_dict_or_none(tmp_path):
    original = database.DB_PATH
    database.DB_PATH = str(tmp_path / "fetch_one.db")
    async def _run():
        await database.init_db()
        await database.set_app_state("k", "v")
        row = await database.fetch_one("SELECT key, value FROM app_state WHERE key = ?", ("k",))
        missing = await database.fetch_one("SELECT key FROM app_state WHERE key = ?", ("missing",))
        return row, missing
    try:
        row, missing = asyncio.run(_run())
    finally:
        database.DB_PATH = original
    assert row == {"key": "k", "value": "v"}
    assert missing is None


def test_broker_sync_status_no_snapshot_is_structured(monkeypatch):
    class _NoSnapshotDB:
        async def fetch_one(self, *_args, **_kwargs):
            return None
    monkeypatch.setattr(main, "database", _NoSnapshotDB())
    payload = TestClient(main.app).get("/api/broker-sync/status").json()
    assert payload["ok"] is False
    assert payload["connected"] is False
    assert payload["snapshot"] == {}
    assert payload["errors"] == []
    assert payload["source"] == "broker_sync_snapshots"


def test_broker_sync_status_handles_missing_fetch_one_regression(monkeypatch):
    class _MissingFetchOneDB:
        pass
    monkeypatch.setattr(main, "database", _MissingFetchOneDB())
    payload = TestClient(main.app).get("/api/broker-sync/status").json()
    assert payload["ok"] is False
    assert payload["connected"] is False
    assert isinstance(payload["errors"], list)


def test_broker_sync_run_returns_error_when_tws_unavailable(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())
    async def _unavailable():
        return {"ok": False, "connected": False, "errors": ["TWS unavailable"], "source": "broker_sync"}
    monkeypatch.setattr(main.broker_sync, "run_broker_sync_once", _unavailable)
    payload = TestClient(main.app).post("/api/broker-sync/run").json()
    assert payload["ok"] is False
    assert payload["connected"] is False
    assert "TWS unavailable" in payload["errors"]


def test_broker_sync_run_times_out_and_does_not_hang(monkeypatch):
    monkeypatch.setattr(main, "database", _DummyDB())
    async def _hang():
        await asyncio.sleep(60)
    monkeypatch.setattr(main.broker_sync, "run_broker_sync_once", _hang)
    monkeypatch.setattr(main, "BROKER_SYNC_RUN_TIMEOUT_SECONDS", 0.05)
    start = time.perf_counter()
    payload = TestClient(main.app).post("/api/broker-sync/run").json()
    elapsed = time.perf_counter() - start
    assert elapsed < 2
    assert payload["ok"] is False
    assert payload["connected"] is False
    assert any("timed out" in err for err in payload["errors"])


def test_manual_tws_mirror_endpoint_exists_and_returns_json_in_dashboard(monkeypatch):
    monkeypatch.setattr(main.config, "APP_ROLE", "dashboard")
    payload = TestClient(main.app).post("/api/tws-mirror/run").json()
    assert payload["skipped"] is True
    assert payload["source"] == "tws_mirror"


def test_manual_execution_sync_endpoint_exists_and_returns_json_in_dashboard(monkeypatch):
    monkeypatch.setattr(main.config, "APP_ROLE", "dashboard")
    payload = TestClient(main.app).post("/api/execution-sync/run").json()
    assert payload["skipped"] is True
    assert payload["source"] == "execution_sync"


def test_startup_recovery_run_timeout_does_not_hang(monkeypatch):
    async def _hang():
        await asyncio.sleep(60)

    monkeypatch.setattr(main.startup_recovery, "run_startup_recovery", _hang)
    monkeypatch.setattr(main, "STARTUP_RECOVERY_RUN_TIMEOUT_SECONDS", 0.05)
    start = time.perf_counter()
    payload = TestClient(main.app).post("/api/startup-recovery/run").json()
    elapsed = time.perf_counter() - start
    assert elapsed < 2
    assert payload["operation"]["status"] == "failed"
    assert "timed out" in (payload.get("message") or "") or "timed out" in str(payload)


def test_trading_status_includes_freshness_diagnostics(monkeypatch):
    monkeypatch.setattr(main.watchdog, "get_watchdog_status", _async_return({"stale_data": {"tws_mirror": True, "execution_sync": True}, "trading_blocked": True, "blocking_reasons": ["TWS mirror sync stale", "Execution sync stale"], "last_tws_mirror_sync_at": None, "last_execution_sync_at": None}))
    monkeypatch.setattr(main.database, "get_latest_broker_sync_snapshot", _async_return({"connected": 1, "synced_at": "2026-01-01T00:00:00+00:00", "executions_json": "[]", "positions_json": "[]", "open_orders_json": "[]", "errors_json": "[]"}))
    monkeypatch.setattr(main.database, "get_open_positions", _async_return([]))
    monkeypatch.setattr(main.database, "get_active_paper_session", _async_return({}))
    monkeypatch.setattr(main.database, "get_realized_pnl", _async_return(0.0))
    monkeypatch.setattr(main.database, "get_open_reconciliation_issues", _async_return([]))
    monkeypatch.setattr(main, "get_market_regime", lambda: {"allow_new_buys": True, "regime": "NEUTRAL", "position_size_factor": 1.0, "min_score_override": None})
    monkeypatch.setattr(main, "get_market_hours_status", lambda: {"enabled": False, "allowed": True, "reason": ""})
    monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", _async_return("SWING"))
    monkeypatch.setattr(main, "get_last_auto_recovery", _async_return(None))
    payload = TestClient(main.app).get("/api/trading-status").json()
    assert payload["broker_sync_fresh"] is True
    assert payload["tws_mirror_fresh"] is True
    assert payload["execution_sync_fresh"] is True


def _async_return(value):
    async def _fn(*_args, **_kwargs):
        return value
    return _fn

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone


import database
import main
import startup_recovery
import watchdog
from circuit_breaker import reset_circuit_breaker
from ibkr_asyncio_compat import ensure_event_loop


def run_async(coro):
    try:
        return asyncio.run(coro)
    finally:
        ensure_event_loop()


def payload(response):
    return json.loads(response.body.decode())


def setup_db(tmp_path):
    original_db = database.DB_PATH
    database.DB_PATH = str(tmp_path / "gateway_health.db")
    main.scheduler.remove_all_jobs()
    run_async(database.init_db())
    return original_db


def teardown_db(original_db):
    main.scheduler.remove_all_jobs()
    database.DB_PATH = original_db


def fresh_gateway_snapshot(positions=None):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "ok": True,
        "connected": 1,
        "source": "LOCAL_GATEWAY_PUSH",
        "synced_at": now,
        "received_at": now,
        "positions": positions or [],
        "positions_json": json.dumps(positions or []),
        "open_orders_json": "[]",
        "executions_json": "[]",
        "errors_json": "[]",
    }


def test_system_health_returns_200_and_uses_fresh_local_gateway(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)

    async def fake_watchdog_status():
        return {"healthy": True, "tws_connected": False, "trading_blocked": False, "blocking_reasons": []}

    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))
        monkeypatch.setattr(main.watchdog, "get_watchdog_status", fake_watchdog_status)
        response = run_async(main.api_system_health())
        data = payload(response)

        assert response.status_code == 200
        assert data["local_gateway_connected"] is True
        assert data["broker_connected"] is True
        assert data["broker_snapshot_stale"] is False
    finally:
        teardown_db(original_db)


def test_auto_trading_status_returns_200_and_exposes_disabled_source(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)

    async def fake_watchdog_status():
        return {"healthy": True, "tws_connected": True, "trading_blocked": False, "blocking_reasons": []}

    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))
        run_async(database.set_app_state(main.AUTO_TRADING_ENABLED_KEY, "false"))
        run_async(database.set_app_state(main.AUTO_TRADING_STATE_REASON_KEY, "unit-test disabled"))
        monkeypatch.setattr(main.watchdog, "get_watchdog_status", fake_watchdog_status)

        response = run_async(main.api_auto_trading_status())
        data = payload(response)

        assert response.status_code == 200
        assert data["enabled"] is False
        assert data["source"] == "app_state"
        assert data["blocked"] is True
        assert any("unit-test disabled" in reason for reason in data["blocking_reasons"])
    finally:
        teardown_db(original_db)



def test_auto_trading_enable_status_runtime_state_wins_over_stale_config(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)

    async def fake_watchdog_status():
        return {
            "healthy": True,
            "tws_connected": False,
            "trading_blocked": False,
            "blocking_reasons": [],
            "degraded_reasons": [],
        }

    async def no_reconciliation_issues():
        return {
            "ok": True,
            "issues_count": 0,
            "open_count": 0,
            "issues": [],
            "counters": {},
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    try:
        run_async(reset_circuit_breaker())
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))
        run_async(database.set_app_state(main.AUTO_TRADING_ENABLED_KEY, "false"))
        run_async(startup_recovery.save_startup_recovery_status({
            "ok": False,
            "state": "FAILED",
            "reason": "direct TWS unavailable in gateway mode",
            "steps": [],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }))
        monkeypatch.setattr(main.config, "AUTO_SEND_ORDERS", False)
        monkeypatch.setattr(main.config, "TRADING_MODE", "OFF")
        monkeypatch.setattr(main.config, "IBKR_PAPER_TRADING", True)
        monkeypatch.setattr(main.config, "IBKR_ENABLE_REAL_TRADING", False)
        monkeypatch.setattr(main.watchdog, "get_watchdog_status", fake_watchdog_status)
        monkeypatch.setattr(main.reconciliation_lifecycle, "get_reconciliation_status", no_reconciliation_issues)

        enable_response = run_async(main.api_auto_trading_enable())
        enable_data = payload(enable_response)
        status_response = run_async(main.api_auto_trading_status())
        status_data = payload(status_response)

        assert enable_response.status_code == 200
        assert enable_data["ok"] is True
        assert enable_data["auto_trading_enabled"] is True
        assert enable_data["gateway_mode"] is True
        assert status_response.status_code == 200
        assert status_data["enabled"] is True
        assert status_data["blocked"] is False
        assert "Auto trading disabled by config" not in status_data["blocking_reasons"]
    finally:
        teardown_db(original_db)

def test_watchdog_treats_fresh_local_gateway_as_connected(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)
    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))
        monkeypatch.setattr(watchdog, "is_ib_connected", lambda: False)
        status = run_async(watchdog.run_watchdog_once())

        assert status["tws_connected"] is True
        assert status["local_gateway_connected"] is True
        assert "TWS/API disconnected" not in status["blocking_reasons"]
    finally:
        teardown_db(original_db)


def test_scheduler_status_reports_scan_lock_and_skips_overlap(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)
    try:
        async def fake_mode():
            return main.strategy_mode.StrategyMode.SWING_DEFAULT

        monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", fake_mode)
        run_async(main.configure_scanner_job())
        job = main.scheduler.get_job(main.SCANNER_JOB_ID)
        assert job.max_instances == 1
        assert job.coalesce is True

        async def locked_call():
            async with main._scan_lock:
                before = int(main._scanner_state.get("skipped_jobs_counter") or 0)
                result = await main.run_full_scan()
                after = int(main._scanner_state.get("skipped_jobs_counter") or 0)
                return before, after, result

        before, after, result = run_async(locked_call())
        assert result["status"] == "already running"
        assert after == before + 1

        response = run_async(main.api_scheduler_status())
        data = payload(response)
        assert response.status_code == 200
        assert "scan_running" in data
        assert "active_jobs" in data
        assert data["skipped_jobs_counter"] >= after
    finally:
        teardown_db(original_db)


def test_dashboard_fetch_failure_uses_safe_rendering_fallbacks():
    script = open("dashboard.html", encoding="utf-8").read()

    assert "async function safeFetchNoCache" in script
    assert "renderFetchWarnings(results)" in script
    assert "/api/system-health" in script
    assert "/api/auto-trading/status" in script
    assert "Dashboard render error" in script


def stale_gateway_snapshot():
    old = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
    payload = fresh_gateway_snapshot()
    payload["synced_at"] = old
    payload["received_at"] = old
    return payload


def test_fresh_gateway_and_direct_ibkr_refused_is_not_blocked(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)
    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))
        monkeypatch.setattr(watchdog, "is_ib_connected", lambda: False)
        status = run_async(watchdog.run_watchdog_once())
        health = run_async(main.build_system_health())

        assert status["trading_blocked"] is False
        assert "TWS/API disconnected" not in status["blocking_reasons"]
        assert status["local_gateway_connected"] is True
        assert health["status"] in {"ACTIVE", "DEGRADED"}
        assert health["status"] != "BLOCKED"
        assert health["broker_connected"] is True
        assert health["local_gateway_connected"] is True
    finally:
        teardown_db(original_db)


def test_stale_gateway_and_direct_ibkr_disconnected_blocks(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)
    try:
        run_async(database.save_broker_sync_snapshot(stale_gateway_snapshot()))
        monkeypatch.setattr(watchdog, "is_ib_connected", lambda: False)
        monkeypatch.setattr(watchdog, "_attempt_reconnect_sync", lambda: {"ok": False, "result": "failed", "error": "Connection refused"})
        status = run_async(watchdog.run_watchdog_once())
        health = run_async(main.build_system_health())

        assert status["trading_blocked"] is True
        assert "TWS/API disconnected" in status["blocking_reasons"]
        assert health["status"] == "BLOCKED"
        assert health["broker_connected"] is False
        assert health["local_gateway_connected"] is False
    finally:
        teardown_db(original_db)


def test_live_tracker_stale_with_fresh_gateway_is_degraded_not_blocked(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)
    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot([{"symbol": "AAPL", "position": 1}])))
        stale_tracker = {
            "source": "live_position_tracker",
            "last_refresh_at": datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat(),
            "open_position_count": 1,
            "tracked_count": 1,
            "healthy": False,
            "positions": [{"symbol": "AAPL", "last_refresh_at": datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()}],
        }
        run_async(database.set_app_state(watchdog.LIVE_POSITION_TRACKER_STATE_KEY, json.dumps(stale_tracker)))
        monkeypatch.setattr(watchdog, "is_ib_connected", lambda: False)

        status = run_async(watchdog.run_watchdog_once())
        health = run_async(main.build_system_health())

        assert status["trading_blocked"] is False
        assert not any("Live position tracking" in reason for reason in status["blocking_reasons"])
        assert any("Live position tracking" in reason or "Live position tracker" in reason for reason in status["degraded_reasons"])
        assert health["status"] == "DEGRADED"
        assert health["live_tracker_healthy"] is False
    finally:
        teardown_db(original_db)


def test_refresh_timeout_does_not_block_while_gateway_snapshot_is_fresh(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)
    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))

        async def slow_refresh():
            await asyncio.sleep(0.02)
            return []

        monkeypatch.setattr(main, "POSITION_REFRESH_TIMEOUT_SECONDS", 0.001)
        monkeypatch.setattr(main, "refresh_open_positions", slow_refresh)
        monkeypatch.setattr(watchdog, "is_ib_connected", lambda: False)

        run_async(main.refresh_open_positions_safe())
        status = run_async(watchdog.run_watchdog_once())
        health = run_async(main.build_system_health())

        assert status["trading_blocked"] is False
        assert health["status"] != "BLOCKED"
        assert health["broker_snapshot_fresh"] is True
    finally:
        teardown_db(original_db)


def test_system_health_fields_are_consistent(tmp_path, monkeypatch):
    original_db = setup_db(tmp_path)

    async def fake_watchdog_status():
        return {
            "healthy": True,
            "tws_connected": True,
            "trading_blocked": False,
            "blocking_reasons": [],
            "degraded_reasons": [],
            "live_position_tracking": {"healthy": True},
            "market_data_feed_active": True,
        }

    try:
        run_async(database.save_broker_sync_snapshot(fresh_gateway_snapshot()))
        monkeypatch.setattr(main.watchdog, "get_watchdog_status", fake_watchdog_status)
        data = payload(run_async(main.api_system_health()))

        for field in [
            "ok",
            "status",
            "broker_connected",
            "local_gateway_connected",
            "broker_snapshot_fresh",
            "broker_snapshot_age_seconds",
            "watchdog_connected",
            "live_tracker_healthy",
            "auto_trader_enabled",
            "circuit_breaker_tripped",
            "blocking_reasons",
            "degraded_reasons",
        ]:
            assert field in data
        assert data["broker_connected"] is True
        assert data["local_gateway_connected"] is True
        assert data["status"] in {"ACTIVE", "DEGRADED", "BLOCKED"}
    finally:
        teardown_db(original_db)

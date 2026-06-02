from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone


import database
import main
import watchdog
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


def fresh_gateway_snapshot():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "ok": True,
        "connected": 1,
        "source": "LOCAL_GATEWAY_PUSH",
        "synced_at": now,
        "received_at": now,
        "positions_json": "[]",
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

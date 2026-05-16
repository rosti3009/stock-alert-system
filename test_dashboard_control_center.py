from __future__ import annotations

import asyncio
import json

import database
import main
import strategy_mode
from ibkr_asyncio_compat import ensure_event_loop


def run_async(coro):
    try:
        return asyncio.run(coro)
    finally:
        ensure_event_loop()


def response_payload(response):
    return json.loads(response.body.decode())


def setup_control_db(tmp_path):
    original_db_path = database.DB_PATH
    database.DB_PATH = str(tmp_path / "dashboard_control_center.db")
    main.scheduler.remove_all_jobs()
    run_async(database.init_db())
    return original_db_path


def teardown_control_db(original_db_path):
    main.scheduler.remove_all_jobs()
    database.DB_PATH = original_db_path


def test_dashboard_control_endpoints_are_callable(tmp_path, monkeypatch):
    original_db_path = setup_control_db(tmp_path)

    async def healthy_watchdog():
        return {"healthy": True, "trading_blocked": False, "tws_connected": True, "blocking_reasons": []}

    async def fake_startup_recovery():
        return {"ok": True, "reason": "passed"}

    async def fake_reconcile():
        return {"status": "ok", "issues": []}

    async def fake_refresh():
        return []

    monkeypatch.setattr(main.watchdog, "run_watchdog_once", healthy_watchdog)
    monkeypatch.setattr(main.startup_recovery, "run_startup_recovery", fake_startup_recovery)
    monkeypatch.setattr(main, "refresh_open_positions", fake_refresh)
    monkeypatch.setattr("reconciliation.run_reconciliation_once", fake_reconcile)

    try:
        scanner = response_payload(run_async(main.api_scanner_restart()))
        watchdog = response_payload(run_async(main.api_watchdog_restart()))
        tracker = response_payload(run_async(main.api_live_position_tracker_refresh()))
        startup = response_payload(run_async(main.api_startup_recovery_run()))
        reconcile = response_payload(run_async(main.api_reconcile_positions_control()))

        assert scanner["status"] == "restarted"
        assert scanner["operation"]["action"] == "restart_scanner"
        assert watchdog["status"] == "restarted"
        assert tracker["status"] == "refreshed"
        assert startup["ok"] is True
        assert reconcile["status"] == "ok"
    finally:
        teardown_control_db(original_db_path)


def test_emergency_stop_disables_auto_trading_and_trips_circuit(tmp_path):
    original_db_path = setup_control_db(tmp_path)
    try:
        run_async(database.set_app_state(main.AUTO_TRADING_ENABLED_KEY, "true"))

        response = run_async(main.api_emergency_stop("unit-test emergency"))
        payload = response_payload(response)

        assert response.status_code == 200
        assert payload["auto_trading_enabled"] is False
        assert payload["circuit_breaker"]["tripped"] is True
        assert run_async(database.get_app_state(main.AUTO_TRADING_ENABLED_KEY)) == "false"
        assert payload["operation"]["action"] == "emergency_stop"
    finally:
        teardown_control_db(original_db_path)


def test_reconnect_updates_watchdog_state(tmp_path, monkeypatch):
    original_db_path = setup_control_db(tmp_path)
    try:
        async def reconnected_watchdog():
            return {"healthy": True, "trading_blocked": False, "tws_connected": True, "blocking_reasons": []}

        monkeypatch.setattr(
            main.watchdog,
            "_attempt_reconnect_sync",
            lambda: {"ok": True, "result": "connected", "error": None},
        )
        monkeypatch.setattr(main.watchdog, "run_watchdog_once", reconnected_watchdog)

        response = run_async(main.api_tws_reconnect())
        payload = response_payload(response)
        stored = json.loads(run_async(database.get_app_state(main.watchdog.WATCHDOG_STATUS_KEY)))

        assert response.status_code == 200
        assert payload["tws_connected"] is True
        assert stored["tws_connected"] is True
        assert stored["last_reconnect_result"]["ok"] is True
    finally:
        teardown_control_db(original_db_path)


def test_restart_controls_do_not_create_duplicate_scheduler_jobs(tmp_path, monkeypatch):
    original_db_path = setup_control_db(tmp_path)

    async def healthy_watchdog():
        return {"healthy": True, "trading_blocked": False, "tws_connected": True, "blocking_reasons": []}

    monkeypatch.setattr(main.watchdog, "run_watchdog_once", healthy_watchdog)

    try:
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.SWING_DEFAULT))
        run_async(main.api_scanner_restart())
        run_async(main.api_scanner_restart())
        run_async(main.api_watchdog_restart())
        run_async(main.api_watchdog_restart())

        scanner_jobs = [job for job in main.scheduler.get_jobs() if job.id == main.SCANNER_JOB_ID]
        watchdog_jobs = [job for job in main.scheduler.get_jobs() if job.id == main.WATCHDOG_JOB_ID]

        assert len(scanner_jobs) == 1
        assert len(watchdog_jobs) == 1
    finally:
        teardown_control_db(original_db_path)

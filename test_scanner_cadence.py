from __future__ import annotations

import asyncio

import config
import database
import main
import strategy_mode
from ibkr_asyncio_compat import ensure_event_loop


def run_async(coro):
    try:
        return asyncio.run(coro)
    finally:
        ensure_event_loop()


def test_swing_uses_minute_interval():
    cadence = main.scanner_cadence_for_mode(strategy_mode.StrategyMode.SWING_DEFAULT)

    assert cadence["scan_interval_seconds"] == config.SCAN_INTERVAL_MINUTES * 60
    assert cadence["symbols_per_scan"] == config.MAX_SYMBOLS_PER_SCAN
    assert cadence["intraday_fast_scan_active"] is False


def test_intraday_uses_seconds_interval():
    cadence = main.scanner_cadence_for_mode(strategy_mode.StrategyMode.INTRADAY_TECHNICAL)

    assert cadence["scan_interval_seconds"] == config.INTRADAY_SCAN_INTERVAL_SECONDS == 30
    assert cadence["symbols_per_scan"] == config.INTRADAY_SYMBOLS_PER_SCAN == 100
    assert cadence["batch_size"] == config.INTRADAY_BATCH_SIZE == 20
    assert cadence["intraday_fast_scan_active"] is True


def test_mode_switch_updates_scanner_cadence(tmp_path):
    original_db_path = database.DB_PATH
    database.DB_PATH = str(tmp_path / "scanner_mode_switch.db")
    main.scheduler.remove_all_jobs()

    try:
        run_async(database.init_db())
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.SWING_DEFAULT))
        swing_status = run_async(main.configure_scanner_job())

        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_TECHNICAL))
        intraday_status = run_async(main.configure_scanner_job())

        jobs = [job for job in main.scheduler.get_jobs() if job.id == main.SCANNER_JOB_ID]
        assert len(jobs) == 1
        assert swing_status["scan_interval_seconds"] == config.SCAN_INTERVAL_MINUTES * 60
        assert intraday_status["scan_interval_seconds"] == config.INTRADAY_SCAN_INTERVAL_SECONDS
        assert intraday_status["intraday_fast_scan_active"] is True
    finally:
        main.scheduler.remove_all_jobs()
        database.DB_PATH = original_db_path


def test_scan_lock_prevents_overlap():
    async def scenario():
        await main._scan_lock.acquire()
        try:
            return await main.run_full_scan()
        finally:
            main._scan_lock.release()

    assert run_async(scenario()) == {"status": "already running"}


def test_intraday_open_positions_are_prioritized_and_scan_is_bounded(tmp_path, monkeypatch):
    original_db_path = database.DB_PATH
    database.DB_PATH = str(tmp_path / "scanner_priority.db")
    main._latest.clear()

    universe = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
    monkeypatch.setattr(main, "load_nasdaq_symbols", lambda limit=None: universe)
    monkeypatch.setattr(config, "USE_DYNAMIC_SYMBOLS", True)
    monkeypatch.setattr(config, "INTRADAY_SYMBOLS_PER_SCAN", 5)
    monkeypatch.setattr(config, "INTRADAY_PRIORITY_SYMBOLS_PER_SCAN", 4)
    monkeypatch.setattr(config, "SYMBOLS", ["WATCH1"])

    async def priority_symbols(limit=200):
        return ["PRI1", "AAA"]

    monkeypatch.setattr(database, "get_priority_symbols", priority_symbols)
    main._latest.update({
        "RV1": {"symbol": "RV1", "price": 10, "volume": 4_000_000, "avg_volume": 1_000_000},
        "MOV1": {"symbol": "MOV1", "price": 20, "ma20": 10, "atr": 2, "volume": 2_000_000, "avg_volume": 1_000_000},
    })

    try:
        run_async(database.init_db())
        run_async(strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_TECHNICAL))
        run_async(database.add_position({"symbol": "OPENX", "buy_price": 100, "quantity": 1, "current_price": 101}))
        run_async(database.save_daily_candidate({"symbol": "BUY1", "price": 25, "signal": "BUY", "score": 90, "weekly_score": 90}, 1))

        symbols = run_async(main.get_scan_symbols())

        assert symbols[0] == "OPENX"
        assert "BUY1" in symbols
        assert "RV1" in symbols
        assert len(symbols) == 5
        assert len(symbols) < len(universe)
    finally:
        main._latest.clear()
        database.DB_PATH = original_db_path


def test_dynamic_universe_loaded_once_per_scan_symbol_selection(monkeypatch):
    calls = {"count": 0}

    def fake_cached_symbols(limit=None, force_refresh=False):
        calls["count"] += 1
        return ["AAA", "BBB", "CCC"]

    async def fake_mode():
        return strategy_mode.StrategyMode.SWING_DEFAULT

    async def fake_priority_symbols(limit=200):
        return []

    async def fake_open_positions():
        return []

    async def fake_get_app_state(key, default="0"):
        return "0"

    async def fake_set_app_state(key, value):
        return None

    monkeypatch.setattr(config, "USE_DYNAMIC_SYMBOLS", True)
    monkeypatch.setattr(main, "get_cached_symbols", fake_cached_symbols)
    monkeypatch.setattr(strategy_mode, "get_strategy_mode", fake_mode)
    monkeypatch.setattr(database, "get_priority_symbols", fake_priority_symbols)
    monkeypatch.setattr(database, "get_open_positions", fake_open_positions)
    monkeypatch.setattr(database, "get_app_state", fake_get_app_state)
    monkeypatch.setattr(database, "set_app_state", fake_set_app_state)

    symbols = run_async(main.get_scan_symbols())

    assert symbols
    assert calls["count"] == 1

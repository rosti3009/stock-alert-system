from __future__ import annotations

import asyncio

import main
import strategy_mode


def run_async(coro):
    return asyncio.run(coro)


def _daily_payload(symbol: str = "AAPL") -> dict:
    closes = [100 + (i * 0.1) for i in range(250)]
    return {
        "symbol": symbol,
        "current_price": closes[-1],
        "opens": [c - 0.2 for c in closes],
        "highs": [c + 0.5 for c in closes],
        "lows": [c - 0.5 for c in closes],
        "closes": closes,
        "volumes": [1_000_000 + (i * 1000) for i in range(250)],
    }


def _intraday_bars(count: int = 60) -> list[dict]:
    bars = []
    for i in range(count):
        base = 100 + (i * 0.05)
        bars.append({"open": base - 0.02, "high": base + 0.08, "low": base - 0.1, "close": base + 0.04, "volume": 20_000 + (i * 200), "timestamp": i})
    return bars


def test_intraday_momentum_mode_populates_api_stock_fields(monkeypatch):
    monkeypatch.setattr(main, "fetch_stock_data", lambda symbol: _daily_payload(symbol))
    async def _mode():
        return strategy_mode.StrategyMode.INTRADAY_MOMENTUM
    monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", _mode)
    monkeypatch.setattr(main.watchdog, "refresh_market_data_timestamp", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "maybe_send_alert", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "fetch_intraday_bars", lambda symbol, timeframe="5m": _intraday_bars())

    row = run_async(main.scan_symbol("AAPL"))

    required_fields = [
        "intraday_momentum_score",
        "intraday_signal",
        "intraday_entry_allowed",
        "intraday_rejection_reasons",
        "vwap",
        "ema9",
        "ema20",
        "relative_volume",
        "intraday_take_profit_percent",
        "intraday_force_exit_before_close",
    ]
    for field in required_fields:
        assert field in row
    assert row["intraday_momentum_score"] >= 0
    assert row["vwap"] is not None
    assert row["relative_volume"] is not None


def test_missing_intraday_bars_produces_rejection_reasons(monkeypatch):
    monkeypatch.setattr(main, "fetch_stock_data", lambda symbol: _daily_payload(symbol))
    async def _mode():
        return strategy_mode.StrategyMode.INTRADAY_MOMENTUM
    monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", _mode)
    monkeypatch.setattr(main.watchdog, "refresh_market_data_timestamp", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "maybe_send_alert", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "fetch_intraday_bars", lambda symbol, timeframe="5m": [] if timeframe in {"5m", "15m"} else _intraday_bars())

    row = run_async(main.scan_symbol("MSFT"))

    assert row["intraday_entry_allowed"] is False
    assert row["intraday_rejection_reasons"]
    assert any("Intraday bars unavailable" in reason for reason in row["intraday_rejection_reasons"])


def test_intraday_vwap_and_relative_volume_populated_when_data_exists(monkeypatch):
    monkeypatch.setattr(main, "fetch_stock_data", lambda symbol: _daily_payload(symbol))
    async def _mode():
        return strategy_mode.StrategyMode.INTRADAY_MOMENTUM
    monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", _mode)
    monkeypatch.setattr(main.watchdog, "refresh_market_data_timestamp", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "maybe_send_alert", lambda *args, **kwargs: asyncio.sleep(0))
    bars = _intraday_bars(70)
    monkeypatch.setattr(main, "fetch_intraday_bars", lambda symbol, timeframe="5m": bars)

    row = run_async(main.scan_symbol("NVDA"))

    assert isinstance(row.get("vwap"), float)
    assert isinstance(row.get("relative_volume"), float)
    assert row["relative_volume"] > 0


def test_intraday_momentum_mode_populates_aggressive_fields(monkeypatch):
    monkeypatch.setattr(main, "fetch_stock_data", lambda symbol: _daily_payload(symbol))

    async def _mode():
        return strategy_mode.StrategyMode.INTRADAY_MOMENTUM

    monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", _mode)
    monkeypatch.setattr(main.watchdog, "refresh_market_data_timestamp", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "maybe_send_alert", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "fetch_intraday_bars", lambda symbol, timeframe="5m": _intraday_bars())

    row = run_async(main.scan_symbol("AAPL"))

    assert row["intraday_momentum_score"] > 0
    assert row["aggressive_score"] == row["intraday_aggressive_score"]
    assert row["aggressive_score"] > 0
    assert isinstance(row["aggressive_rejection_reasons"], list)


def test_oust_like_row_can_allow_aggressive_entry(monkeypatch):
    monkeypatch.setattr(main, "fetch_stock_data", lambda symbol: _daily_payload(symbol))

    async def _mode():
        return strategy_mode.StrategyMode.INTRADAY_MOMENTUM

    monkeypatch.setattr(main.strategy_mode, "get_strategy_mode", _mode)
    monkeypatch.setattr(main.watchdog, "refresh_market_data_timestamp", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "maybe_send_alert", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(main, "fetch_intraday_bars", lambda symbol, timeframe="5m": _intraday_bars())

    row = run_async(main.scan_symbol("OUST"))
    row.update({
        "relative_volume": 15.897,
        "price": float(row.get("price") or row.get("current_price") or 110.0),
        "vwap": float(row.get("vwap") or 100.0),
        "ema9": float(row.get("ema9") or 101.0),
        "ema20": None,
        "volume_confirmation": True,
        "volume_expansion": None,
        "spread_percent": 0.8,
        "intraday_entry_allowed": True,
    })
    payload = main.intraday_momentum_engine.detect_intraday_entry_setup(row)

    assert payload["intraday_aggressive_score"] >= 60
    assert payload["aggressive_entry_allowed"] is True
    assert isinstance(payload["aggressive_rejection_reasons"], list)

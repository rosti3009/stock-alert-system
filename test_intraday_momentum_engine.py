from datetime import datetime, timezone

import intraday_momentum_engine as ime


def base_row():
    return {
        "symbol": "AAPL",
        "price": 101,
        "vwap": 100,
        "ema9": 101,
        "ema20": 100,
        "relative_volume": 2.0,
        "dollar_volume": 10_000_000,
        "opening_range_high": 100.5,
        "setup": "breakout continuation",
        "volatility_expansion": True,
        "micro_pullback_continuation": True,
        "consecutive_green_candles": 3,
        "range_expansion": True,
        "volume_confirmation": True,
        "intraday_bars": {"1m": [1], "5m": [1], "15m": [1]},
    }


def test_missing_intraday_bars_rejected():
    row = base_row()
    row["intraday_bars"] = {"1m": [1]}
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is False
    assert payload["intraday_signal"] == "REJECTED"
    assert any("Intraday bars unavailable" in r for r in payload["rejection_reasons"])


def test_strong_setup_allows_entry():
    payload = ime.detect_intraday_entry_setup(base_row())
    assert payload["entry_allowed"] is True
    assert payload["intraday_momentum_score"] >= ime.BUY_THRESHOLD


def test_vwap_loss_creates_exit_signal():
    row = base_row()
    row["price"] = 99
    exit_payload = ime.detect_intraday_exit_setup(row, position={"buy_price": 100})
    assert exit_payload["intraday_exit_signal"] == "EXIT"
    assert "VWAP loss" in exit_payload["intraday_exit_reasons"]


def test_target_profit_range_creates_take_profit():
    row = base_row()
    row["price"] = 103
    exit_payload = ime.detect_intraday_exit_setup(row, position={"buy_price": 100})
    assert exit_payload["intraday_exit_signal"] in {"TAKE_PROFIT", "EXIT"}
    assert any("2%-4% target reached" in r for r in exit_payload["intraday_exit_reasons"])


def test_forced_eod_exit():
    row = base_row()
    payload = ime.detect_intraday_exit_setup(
        row,
        position={"buy_price": 100},
        now=datetime(2026, 5, 19, 19, 50, tzinfo=timezone.utc),
    )
    assert payload["intraday_exit_signal"] == "FORCE_EOD_EXIT"

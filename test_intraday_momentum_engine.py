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
    assert any("TP1 +2% reached" in r for r in exit_payload["intraday_exit_reasons"])


def test_runner_tp2_reaches_exit():
    row = base_row()
    row["price"] = 104.5
    exit_payload = ime.detect_intraday_exit_setup(row, position={"buy_price": 100})
    assert exit_payload["intraday_exit_signal"] == "EXIT"
    assert any("TP2/runner +4% reached" in r for r in exit_payload["intraday_exit_reasons"])


def test_invalid_spread_and_low_relative_volume_blocks_entry():
    row = base_row()
    row["relative_volume"] = 1.1
    row["spread_percent"] = 3.0
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is False
    assert "relative volume below minimum" in payload["aggressive_rejection_reasons"]
    assert "spread too wide" in payload["aggressive_rejection_reasons"]


def test_forced_eod_exit():
    row = base_row()
    payload = ime.detect_intraday_exit_setup(
        row,
        position={"buy_price": 100},
        now=datetime(2026, 5, 19, 19, 50, tzinfo=timezone.utc),
    )
    assert payload["intraday_exit_signal"] == "FORCE_EOD_EXIT"


def test_defensive_regime_high_momentum_exception_allows_entry():
    row = base_row()
    row["market_regime"] = "DEFENSIVE"
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["regime_override_active"] is True
    assert payload["entry_allowed"] is True
    assert payload["regime_override_reason"] == "HIGH_MOMENTUM_EXCEPTION"


def test_low_quality_setup_still_blocked_with_threshold_60():
    row = base_row()
    row["relative_volume"] = 1.2
    row["volatility_expansion"] = False
    row["range_expansion"] = False
    row["setup"] = "none"
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is False
    assert "relative volume below minimum" in payload["aggressive_rejection_reasons"]


def test_high_rvol_vwap_ema_alignment_reaches_threshold():
    row = base_row()
    row["relative_volume"] = 3.5
    row["volume_expansion"] = True
    row["positive_candle_momentum"] = True
    row["intraday_price_change_percent"] = 3.1
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["intraday_momentum_score"] >= 60


def test_high_rvol_but_bad_spread_is_blocked():
    row = base_row()
    row["relative_volume"] = 3.5
    row["bad_spread"] = True
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is False
    assert "bad spread" in payload["aggressive_rejection_reasons"]


def test_aggressive_entry_allowed_never_null():
    payload = ime.detect_intraday_entry_setup(base_row())
    assert payload["aggressive_entry_allowed"] in {True, False}


def test_rejection_reasons_non_empty_when_rejected():
    row = base_row()
    row["relative_volume"] = 1.0
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is False
    assert isinstance(payload["aggressive_rejection_reasons"], list)
    assert len(payload["aggressive_rejection_reasons"]) > 0


def test_impp_like_payload_allows_aggressive_entry_with_missing_optional_enrichment():
    row = base_row()
    row.update(
        {
            "symbol": "IMPP",
            "price": 5.68,
            "volume": 1_878_955,
            "dollar_volume": None,
            "vwap": 5.4023,
            "ema9": 5.4273,
            "ema20": None,
            "relative_volume": 2.3599,
            "intraday_entry_allowed": True,
            "spread_quality_score": None,
            "volume_expansion": None,
            "volume_confirmation": True,
        }
    )
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["aggressive_entry_allowed"] is True
    assert "ema20_missing_but_fast_trend_valid" in payload["aggressive_rejection_reasons"]
    assert "spread_quality_missing_assumed_ok" in payload["aggressive_rejection_reasons"]
    assert "volume_expansion_missing_rvol_confirmed" in payload["aggressive_rejection_reasons"]
    assert "dollar volume below minimum" not in payload["aggressive_rejection_reasons"]


def test_dollar_volume_fallback_uses_price_times_volume():
    row = base_row()
    row["price"] = 5.68
    row["volume"] = 1_878_955
    row["dollar_volume"] = None
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is True
    assert payload["liquidity_quality_score"] == 100


def test_true_low_dollar_volume_still_blocks():
    row = base_row()
    row["price"] = 2.0
    row["volume"] = 500_000
    row["dollar_volume"] = None
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is False
    assert "dollar volume below minimum" in payload["aggressive_rejection_reasons"]


def test_missing_ema20_does_not_add_hard_ema_rejection_for_strong_fast_trend():
    row = base_row()
    row["price"] = 101.2
    row["ema20"] = None
    payload = ime.detect_intraday_entry_setup(row)
    assert "ema_not_aligned" not in payload["aggressive_rejection_reasons"]
    assert "ema20_missing_but_fast_trend_valid" in payload["aggressive_rejection_reasons"]


def test_missing_spread_quality_score_does_not_hard_block_strong_setup():
    row = base_row()
    row["spread_quality_score"] = None
    row["spread_percent"] = None
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is True
    assert "spread_quality_missing_assumed_ok" in payload["aggressive_rejection_reasons"]


def test_missing_volume_expansion_does_not_hard_block_when_rvol_confirms():
    row = base_row()
    row["volume_expansion"] = None
    row["volume_confirmation"] = True
    row["relative_volume"] = 2.2
    payload = ime.detect_intraday_entry_setup(row)
    assert payload["entry_allowed"] is True
    assert "volume_expansion_missing_rvol_confirmed" in payload["aggressive_rejection_reasons"]

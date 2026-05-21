from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

REQUIRED_TIMEFRAMES = ("1m", "5m", "15m")
BUY_THRESHOLD = 60


def validate_required_intraday_bars(row: dict[str, Any]) -> tuple[bool, list[str]]:
    bars = row.get("intraday_bars") or {}
    missing = [tf for tf in REQUIRED_TIMEFRAMES if not (isinstance(bars, dict) and bars.get(tf))]
    return len(missing) == 0, missing


def calculate_intraday_momentum_score(row: dict[str, Any]) -> tuple[int, list[str], dict[str, Any]]:
    reasons: list[str] = []
    components: dict[str, Any] = {}
    score = 0
    price = _f(row.get("price") or row.get("current_price"))
    vwap = _f(row.get("vwap"))
    ema9 = _f(row.get("ema9"))
    ema20 = _f(row.get("ema20"))
    rv = _f(row.get("relative_volume"))
    dollar_volume = _f(row.get("dollar_volume")) or 0.0
    opening_range_high = _f(row.get("opening_range_high"))
    gap_percent = _f(row.get("gap_percent")) or 0.0
    intraday_change = _f(row.get("intraday_price_change_percent") or row.get("price_change_percent") or row.get("change_percent")) or 0.0
    breakout = str(row.get("setup") or row.get("intraday_setup") or "").lower()

    momentum_accel = _f(row.get("momentum_acceleration") or row.get("momentum_acceleration_score") or row.get("momentum_delta"))
    if vwap and price and price >= vwap:
        score += 16
        reasons.append("price above VWAP")
        components["vwap_reclaim"] = True
    if ema9 and ema20 and ema9 > ema20:
        score += 16
        reasons.append("EMA9 above EMA20")
        components["ema9_above_ema20"] = True
    if rv and rv >= 1.5:
        if rv >= 3.0:
            score += 24
            reasons.append("relative volume >= 3.0")
        elif rv >= 2.0:
            score += 18
            reasons.append("relative volume >= 2.0")
        else:
            score += 12
        reasons.append("relative volume surge")
    if row.get("volume_expansion") is True:
        score += 12
        reasons.append("volume expansion")
        components["volume_expansion"] = True
    if momentum_accel and momentum_accel > 0:
        score += 14 if momentum_accel >= 2 else 10
        reasons.append("momentum acceleration positive")
        components["momentum_acceleration_positive"] = True
    if row.get("volatility_expansion") is True:
        score += 12
        reasons.append("volatility expansion")
        components["volatility_expansion"] = True
    if opening_range_high and price and price > opening_range_high:
        score += 10
        reasons.append("opening range breakout")
    if "breakout" in breakout:
        score += 10
        reasons.append("breakout continuation")
    if row.get("micro_pullback_continuation") is True:
        score += 8
        reasons.append("micro pullback continuation")
    if int(row.get("consecutive_green_candles") or 0) >= 3:
        score += 8
        reasons.append("consecutive strong green candles")
    if bool(row.get("positive_candle_momentum")):
        score += 10
        reasons.append("positive candle momentum")
        components["positive_candle_momentum"] = True
    if row.get("range_expansion") is True:
        score += 8
        reasons.append("range expansion")
    if row.get("volume_confirmation") is True:
        score += 8
        reasons.append("volume confirmation")
    if dollar_volume >= 5_000_000:
        score += 5
        reasons.append("liquidity quality")
    if gap_percent > 0:
        score += 3
    if intraday_change > 2.5:
        score += 10
        reasons.append("intraday price change > 2.5%")
        components["intraday_price_change_gt_2_5"] = True

    components["momentum_acceleration_score"] = int(min(100, max(0, (momentum_accel or 0.0) * 20)))
    components["volume_expansion_score"] = min(100, int(((rv or 0.0) / 2.0) * 100))
    components["vwap_reclaim_signal"] = bool(vwap and price and price >= vwap)
    components["volatility_expansion_signal"] = bool(row.get("volatility_expansion") is True or row.get("range_expansion") is True)
    return min(100, max(0, score)), reasons, components


def classify_intraday_regime(row: dict[str, Any]) -> str:
    score, _, _ = calculate_intraday_momentum_score(row)
    if score >= 80:
        return "MOMENTUM_STRONG"
    if score >= 60:
        return "MOMENTUM_NEUTRAL"
    return "MOMENTUM_WEAK"


def generate_rejection_reasons(row: dict[str, Any], score: int, missing_timeframes: list[str]) -> list[str]:
    reasons: list[str] = []
    if missing_timeframes:
        reasons.append(f"Intraday bars unavailable: {', '.join(missing_timeframes)}")
    if score < BUY_THRESHOLD:
        reasons.append(f"intraday_momentum_score too low ({score} < {BUY_THRESHOLD})")
    return reasons


def detect_intraday_entry_setup(row: dict[str, Any]) -> dict[str, Any]:
    bars_ok, missing = validate_required_intraday_bars(row)
    score, score_reasons, components = calculate_intraday_momentum_score(row)
    rejection_reasons = generate_rejection_reasons(row, score, missing if not bars_ok else [])
    vwap = _f(row.get("vwap"))
    ema9 = _f(row.get("ema9"))
    ema20 = _f(row.get("ema20"))
    intraday_entry_allowed = bool(row.get("intraday_entry_allowed", True))
    allowed = bars_ok and intraday_entry_allowed and score >= BUY_THRESHOLD
    if not intraday_entry_allowed:
        rejection_reasons.append("intraday_entry_allowed=false")
    rv = _f(row.get("relative_volume")) or 0.0
    dv = _f(row.get("dollar_volume")) or 0.0
    spread = _f(row.get("spread_percent"))
    stale_data = bool(row.get("stale_data", False))
    low_liquidity = bool(row.get("low_liquidity", False))
    bad_spread_flag = bool(row.get("bad_spread", False))
    no_overnight = bool(row.get("no_overnight", False))
    duplicate_order = bool(row.get("duplicate_order", False))
    market_closed = bool(row.get("market_closed", False))
    spread_quality_missing = row.get("spread_quality_score") is None
    if rv < 1.7:
        rejection_reasons.append("relative volume below minimum")
        allowed = False
    if dv < 3_000_000:
        rejection_reasons.append("dollar volume below minimum")
        allowed = False
    if spread is not None and spread > 2.5:
        rejection_reasons.append("spread too wide")
        allowed = False
    if spread is None or spread_quality_missing:
        rejection_reasons.append("spread_quality_missing")
    if row.get("volume_expansion") is None:
        rejection_reasons.append("volume_expansion_missing")
    execution_safety_ok = bool(row.get("execution_safety_passes", True))
    broker_sync_healthy = bool(row.get("broker_sync_healthy", True))
    reconciliation_healthy = bool(row.get("reconciliation_healthy", True))
    circuit_breaker_clear = bool(row.get("circuit_breaker_clear", True))
    if not execution_safety_ok:
        rejection_reasons.append("execution safety gate blocked")
        allowed = False
    if not broker_sync_healthy:
        rejection_reasons.append("broker sync unhealthy")
        allowed = False
    if not reconciliation_healthy:
        rejection_reasons.append("reconciliation unhealthy")
        allowed = False
    if not circuit_breaker_clear:
        rejection_reasons.append("circuit breaker active")
        allowed = False
    if stale_data:
        rejection_reasons.append("stale data")
        allowed = False
    if low_liquidity:
        rejection_reasons.append("low liquidity")
        allowed = False
    if bad_spread_flag:
        rejection_reasons.append("bad spread")
        allowed = False
    if market_closed:
        rejection_reasons.append("market closed")
        allowed = False
    if duplicate_order:
        rejection_reasons.append("duplicate order")
        allowed = False
    if no_overnight:
        rejection_reasons.append("no overnight")
        allowed = False
    technical_fallback_mode = (
        rv >= 2.0
        and bool(components.get("vwap_reclaim_signal"))
        and bool(components.get("ema9_above_ema20"))
        and (bool(components.get("momentum_acceleration_positive")) or bool(components.get("positive_candle_momentum")))
    )
    breakout_allowed = (
        score >= BUY_THRESHOLD
        and
        rv >= 2.0
        and bool(components.get("vwap_reclaim_signal"))
        and bool(components.get("ema9_above_ema20"))
        and bool(components.get("momentum_acceleration_positive"))
        and (spread is None or spread <= 1.5)
        and int(components.get("volume_expansion_score", 0)) >= 70
    )
    if breakout_allowed:
        allowed = bars_ok and intraday_entry_allowed and dv >= 3_000_000
        if allowed:
            score = max(score, BUY_THRESHOLD + 10)
            score_reasons.append("aggressive breakout override")
    if (not bars_ok) and technical_fallback_mode:
        rejection_reasons = [r for r in rejection_reasons if not r.startswith("Intraday bars unavailable")]
        allowed = intraday_entry_allowed and score >= BUY_THRESHOLD
        score_reasons.append("technical fallback mode")

    if score < BUY_THRESHOLD:
        rejection_reasons.append("score_below_threshold")
    if vwap is None:
        rejection_reasons.append("missing_vwap")
    if not bool(components.get("ema9_above_ema20")):
        rejection_reasons.append("ema_not_aligned")
    if not ("breakout" in str(row.get("setup") or row.get("intraday_setup") or "").lower() or row.get("range_expansion") is True):
        rejection_reasons.append("weak_breakout")
    breakout_strength = min(100, int((score * 0.5) + (components.get("volume_expansion_score", 0) * 0.5)))
    regime = str(row.get("market_regime") or row.get("regime") or "").upper()
    regime_override_active = (
        regime == "DEFENSIVE"
        and score >= 65
        and rv >= 2.0
        and bool(components.get("vwap_reclaim_signal"))
        and bool(components.get("ema9_above_ema20"))
        and int(components.get("volume_expansion_score", 0)) >= 80
        and (spread is None or spread <= 2.5)
        and execution_safety_ok
        and broker_sync_healthy
        and reconciliation_healthy
    )
    if regime == "DEFENSIVE" and not regime_override_active:
        rejection_reasons.append("DEFENSIVE regime without high momentum exception")
        allowed = False
    if regime_override_active:
        allowed = allowed or (bars_ok and intraday_entry_allowed)
    rejection_reasons = list(dict.fromkeys(rejection_reasons))
    return {
        "entry_allowed": allowed,
        "intraday_signal": "BUY" if allowed else "REJECTED",
        "intraday_momentum_score": score,
        "score_reasons": score_reasons,
        "rejection_reasons": rejection_reasons,
        "active_profile": "INTRADAY_AGGRESSIVE",
        "aggressive_entry_allowed": bool(allowed),
        "aggressive_rejection_reasons": rejection_reasons if rejection_reasons else [],
        "momentum_acceleration_score": components.get("momentum_acceleration_score", 0),
        "breakout_strength_score": breakout_strength,
        "vwap_reclaim_signal": components.get("vwap_reclaim_signal", False),
        "volatility_expansion_signal": components.get("volatility_expansion_signal", False),
        "regime_override_active": regime_override_active,
        "regime_override_reason": "HIGH_MOMENTUM_EXCEPTION" if regime_override_active else None,
        "market_regime_before_override": regime,
        "market_regime_after_override": "MOMENTUM_EXCEPTION" if regime_override_active else regime,
        "regime_override": "HIGH_MOMENTUM_EXCEPTION" if regime_override_active else None,
        "expected_stop_percent": min(2.0, max(0.8, _f(row.get("atr_stop_percent")) or 1.2)),
        "expected_tp1_percent": 2.0,
        "expected_tp2_percent": 4.0,
        "expected_position_size_percent": 12.0,
        "liquidity_quality_score": 100 if dv >= 5_000_000 else 70 if dv >= 3_000_000 else 20,
        "spread_quality_score": 100 if (spread is not None and spread <= 1.0) else 70 if (spread is not None and spread <= 2.5) else 20,
        "volume_expansion_score": min(100, int((rv / 2.0) * 100)),
        "intraday_aggressive_score": score,
    }


def detect_intraday_exit_setup(
    row: dict[str, Any],
    position: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    signal = "HOLD"

    price = _f(row.get("price"))
    vwap = _f(row.get("vwap"))
    ema9 = _f(row.get("ema9"))
    ema20 = _f(row.get("ema20"))

    buy_price = _f((position or {}).get("buy_price"))

    if price and vwap and price < vwap:
        signal = "EXIT"
        reasons.append("VWAP loss")

    if price and ema9 and price < ema9:
        signal = "EXIT"
        reasons.append("EMA9 loss")

    if ema9 and ema20 and ema9 < ema20:
        signal = "EXIT"
        reasons.append("ema9_below_ema20")

    if price and buy_price:
        gain_percent = ((price - buy_price) / buy_price) * 100

        if gain_percent >= 4:
            signal = "EXIT"
            reasons.append("TP2/runner +4% reached")
        elif gain_percent >= 2:
            signal = "EXIT"
            reasons.append("TP1 +2% reached")

    # Force EOD only if no technical exit was already triggered.
    if signal == "HOLD" and _is_force_eod_exit(now):
        signal = "FORCE_EOD_EXIT"
        reasons.append("forced end-of-day exit")

    return {
        "intraday_exit_signal": signal,
        "intraday_exit_reasons": reasons,
    }


def build_dashboard_payload(row: dict[str, Any], position: dict[str, Any] | None = None) -> dict[str, Any]:
    entry = detect_intraday_entry_setup(row)
    exit_payload = detect_intraday_exit_setup(row, position=position)
    return {
        "intraday_momentum_score": entry["intraday_momentum_score"],
        "intraday_signal": entry["intraday_signal"],
        "intraday_entry_allowed": entry["entry_allowed"],
        "intraday_rejection_reasons": entry["rejection_reasons"],
        "intraday_exit_signal": exit_payload["intraday_exit_signal"],
        "intraday_exit_reasons": exit_payload["intraday_exit_reasons"],
        "intraday_regime": classify_intraday_regime(row),
        "required_timeframes": list(REQUIRED_TIMEFRAMES),
    }


def _is_force_eod_exit(now: datetime | None) -> bool:
    eastern = ZoneInfo("America/New_York")
    current = (now or datetime.now(timezone.utc)).astimezone(eastern)
    return current.time() >= time(15, 45)


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None

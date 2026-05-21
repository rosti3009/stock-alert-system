from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

REQUIRED_TIMEFRAMES = ("1m", "5m", "15m")
BUY_THRESHOLD = 75


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
    breakout = str(row.get("setup") or row.get("intraday_setup") or "").lower()

    if vwap and price and price >= vwap:
        score += 12
        reasons.append("price above VWAP")
        components["vwap_reclaim"] = True
    if ema9 and ema20 and ema9 > ema20:
        score += 10
        reasons.append("EMA9 above EMA20")
    if rv and rv >= 1.5:
        score += 10
        reasons.append("relative volume surge")
    if row.get("volatility_expansion") is True:
        score += 8
        reasons.append("volatility expansion")
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
    score, score_reasons, _ = calculate_intraday_momentum_score(row)
    rejection_reasons = generate_rejection_reasons(row, score, missing if not bars_ok else [])
    intraday_entry_allowed = bool(row.get("intraday_entry_allowed", True))
    allowed = bars_ok and intraday_entry_allowed and score >= BUY_THRESHOLD
    if not intraday_entry_allowed:
        rejection_reasons.append("intraday_entry_allowed=false")
    rv = _f(row.get("relative_volume")) or 0.0
    dv = _f(row.get("dollar_volume")) or 0.0
    spread = _f(row.get("spread_percent"))
    if rv < 1.5:
        rejection_reasons.append("relative volume below minimum")
        allowed = False
    if dv < 3_000_000:
        rejection_reasons.append("dollar volume below minimum")
        allowed = False
    if spread is not None and spread > 2.5:
        rejection_reasons.append("spread too wide")
        allowed = False
    return {
        "entry_allowed": allowed,
        "intraday_signal": "BUY" if allowed else "REJECTED",
        "intraday_momentum_score": score,
        "score_reasons": score_reasons,
        "rejection_reasons": rejection_reasons,
        "active_profile": "INTRADAY_AGGRESSIVE",
        "aggressive_entry_allowed": allowed,
        "aggressive_rejection_reasons": rejection_reasons,
        "expected_stop_percent": min(2.0, max(0.8, _f(row.get("atr_stop_percent")) or 1.2)),
        "expected_tp1_percent": 2.0,
        "expected_tp2_percent": 4.0,
        "expected_position_size_percent": 12.0,
        "liquidity_quality_score": 100 if dv >= 5_000_000 else 70 if dv >= 3_000_000 else 20,
        "spread_quality_score": 100 if (spread is not None and spread <= 1.0) else 70 if (spread is not None and spread <= 2.5) else 20,
        "volume_expansion_score": min(100, int((rv / 2.0) * 100)),
        "intraday_aggressive_score": score,
    }


def detect_intraday_exit_setup(row: dict[str, Any], position: dict[str, Any] | None = None, now: datetime | None = None) -> dict[str, Any]:
    reasons: list[str] = []
    signal = "HOLD"
    price = _f(row.get("price")) or 0.0
    vwap = _f(row.get("vwap"))
    ema9 = _f(row.get("ema9"))
    buy_price = _f((position or {}).get("buy_price")) or 0.0
    pnl_pct = ((price - buy_price) / buy_price * 100) if buy_price > 0 else 0
    if pnl_pct >= 2.0:
        signal = "TAKE_PROFIT"
        reasons.append("TP1 +2% reached")
    if pnl_pct >= 4.0:
        signal = "EXIT"
        reasons.append("TP2/runner +4% reached")
    if vwap and price < vwap:
        signal = "EXIT"
        reasons.append("VWAP loss")
    if ema9 and price < ema9:
        signal = "EXIT"
        reasons.append("EMA9 loss")
    if row.get("failed_breakout"):
        signal = "EXIT"
        reasons.append("failed breakout")
    if row.get("lower_high_after_breakout"):
        signal = "EXIT"
        reasons.append("lower high after breakout")
    if row.get("momentum_fade"):
        signal = "EXIT"
        reasons.append("momentum fade")
    if row.get("volume_collapse"):
        signal = "EXIT"
        reasons.append("volume collapse")
    if row.get("trailing_stop_hit"):
        signal = "EXIT"
        reasons.append("trailing stop hit")
    if row.get("time_stop"):
        signal = "EXIT"
        reasons.append("time stop")
    if _is_force_eod_exit(now):
        signal = "FORCE_EOD_EXIT"
        reasons.append("forced end-of-day exit")
    return {"intraday_exit_signal": signal, "intraday_exit_reasons": reasons}


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

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo


REQUIRED_TIMEFRAMES = ("1m", "5m", "15m")


@dataclass
class IntradayRegimeProfile:
    buy_threshold: int
    position_size_factor: float
    buys_allowed: bool
    trailing_aggressiveness: float


REGIME_PROFILES: dict[str, IntradayRegimeProfile] = {
    "TREND_DAY": IntradayRegimeProfile(70, 1.15, True, 1.2),
    "CHOPPY_DAY": IntradayRegimeProfile(86, 0.6, False, 1.6),
    "LOW_VOL_DAY": IntradayRegimeProfile(84, 0.7, False, 1.5),
    "HIGH_MOMENTUM_DAY": IntradayRegimeProfile(66, 1.3, True, 1.0),
    "MEAN_REVERSION_DAY": IntradayRegimeProfile(82, 0.8, False, 1.4),
}


def _f(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def required_bars_available(row: dict[str, Any]) -> tuple[bool, list[str]]:
    bars = row.get("intraday_bars") or {}
    missing = []
    for tf in REQUIRED_TIMEFRAMES:
        if not (isinstance(bars, dict) and bars.get(tf)):
            missing.append(tf)
    if row.get("intraday_bars_available") is True and not missing:
        return True, []
    if missing:
        return False, [f"Intraday bars unavailable ({', '.join(missing)})"]
    return True, []


def classify_intraday_regime(row: dict[str, Any]) -> str:
    rv = _f(row.get("relative_volume") or row.get("intraday_relative_volume")) or 0
    mom = _f(row.get("momentum_percent") or row.get("change_percent")) or 0
    rng = _f(row.get("range_percent")) or 0
    if rv >= 2.5 and mom >= 1.0:
        return "HIGH_MOMENTUM_DAY"
    if rng >= 3.0 and mom > 0:
        return "TREND_DAY"
    if rng <= 0.8:
        return "LOW_VOL_DAY"
    if mom < 0 and abs(mom) < 1.0:
        return "MEAN_REVERSION_DAY"
    return "CHOPPY_DAY"


def calculate_intraday_momentum_score(row: dict[str, Any]) -> tuple[int, list[str]]:
    ok, reasons = required_bars_available(row)
    if not ok:
        return 0, reasons
    score = 0
    out: list[str] = []
    checks = {
        "Volatility expansion": (_f(row.get("range_percent")) or 0) >= 1.5,
        "Momentum ignition": (_f(row.get("momentum_percent") or row.get("change_percent")) or 0) >= 0.6,
        "Relative volume surge": (_f(row.get("relative_volume")) or 0) >= 1.6,
        "VWAP reclaim": bool(row.get("vwap_reclaim") is True),
        "VWAP hold": bool(row.get("vwap_hold") is True),
        "EMA9 above EMA20": (_f(row.get("ema9")) or 0) > (_f(row.get("ema20")) or 10**9),
        "Price above VWAP": (_f(row.get("price")) or 0) >= (_f(row.get("vwap")) or 10**9),
        "Opening Range Breakout": bool(row.get("opening_range_breakout") is True),
        "Breakout continuation": bool(row.get("breakout_continuation") is True),
        "Micro pullback continuation": bool(row.get("micro_pullback_continuation") is True),
        "Consecutive strong green candles": int(row.get("consecutive_green_candles") or 0) >= 3,
        "Range expansion": (_f(row.get("range_expansion")) or 0) >= 1.0,
        "Volume confirmation on breakout": bool(row.get("breakout_volume_confirmed") is True),
        "Fast liquidity rotation": (_f(row.get("dollar_volume")) or 0) >= 5_000_000,
    }
    for name, passed in checks.items():
        if passed:
            score += 7
            out.append(name)
    weekly = _f(row.get("weekly_score")) or 0
    if weekly < 35:
        out.append("Weak long-term trend context warning")
        score -= 5
    elif weekly >= 80:
        out.append("Strong long-term trend context boost")
        score += 3
    return max(0, min(100, score)), out


def evaluate_intraday_entry(row: dict[str, Any], *, threshold: int, risk_block_reasons: list[str] | None = None) -> dict[str, Any]:
    score, score_reasons = calculate_intraday_momentum_score(row)
    reasons = list(risk_block_reasons or [])
    if any("Intraday bars unavailable" in r for r in score_reasons):
        reasons.append("Intraday bars unavailable")
    if score < threshold:
        reasons.append(f"Intraday momentum score below threshold ({score} < {threshold})")
    if (_f(row.get("price")) or 0) < (_f(row.get("vwap")) or 10**9):
        reasons.append("VWAP not reclaimed")
    if (_f(row.get("relative_volume")) or 0) < 1.5:
        reasons.append("Relative volume too low")
    if (_f(row.get("dollar_volume")) or 0) < 3_000_000:
        reasons.append("Dollar volume too low")
    return {"entry_allowed": len(reasons) == 0, "intraday_signal": "BUY" if len(reasons) == 0 else "REJECT", "intraday_momentum_score": score, "score_reasons": score_reasons, "rejection_reasons": reasons}


def evaluate_intraday_exit(position: dict[str, Any], row: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    reasons = []
    bp = _f(position.get("buy_price")) or 0
    p = _f(row.get("price") or row.get("current_price")) or 0
    pnl_pct = ((p - bp) / bp * 100) if bp > 0 else 0
    if 2.0 <= pnl_pct <= 4.0:
        reasons.append("Target profit reached (2%-4%)")
    if (_f(row.get("price")) or 0) < (_f(row.get("vwap")) or -1):
        reasons.append("VWAP loss")
    if (_f(row.get("price")) or 0) < (_f(row.get("ema9")) or -1):
        reasons.append("EMA9 loss")
    if ( _f(row.get("momentum_percent")) or 0 ) < -0.25:
        reasons.append("Momentum fade")
    if row.get("failed_breakout") is True:
        reasons.append("Failed breakout")
    if row.get("volume_collapse") is True:
        reasons.append("Volume collapse")
    if row.get("micro_pullback_failure") is True:
        reasons.append("Micro pullback failure")
    if row.get("trailing_stop_hit") is True:
        reasons.append("Trailing stop hit")
    et = ZoneInfo("America/New_York")
    curr = (now or datetime.now(timezone.utc)).astimezone(et)
    if curr.time() >= time(15, 50):
        reasons.append("End-of-day forced exit")
    return {"exit_signal": "EXIT" if reasons else "HOLD", "exit_reasons": reasons}

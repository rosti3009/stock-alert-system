from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

SETUP_TYPES = {
    "BREAKOUT",
    "VWAP_BOUNCE",
    "MOMENTUM_CONTINUATION",
    "REVERSAL",
    "GAP_AND_GO",
    "FAILED_BREAKOUT",
    "UNKNOWN",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def time_of_day(value: Any | None = None) -> str | None:
    dt = parse_dt(value) or datetime.now(timezone.utc)
    return f"{dt.hour:02d}:00"


def rvol(row: dict) -> float | None:
    direct = safe_float(row.get("rvol"), None)
    if direct is not None:
        return direct
    volume = safe_float(row.get("volume"), None)
    avg_volume = safe_float(row.get("avg_volume"), None)
    if volume is None or avg_volume in (None, 0):
        return None
    return round(volume / avg_volume, 4)


def vwap_status(row: dict) -> str:
    explicit = row.get("vwap_status")
    if explicit:
        return str(explicit).upper()
    price = safe_float(row.get("price") or row.get("entry_price"), None)
    vwap = safe_float(row.get("vwap"), None)
    if price is None or vwap is None:
        return "UNKNOWN"
    if price > vwap:
        return "ABOVE_VWAP"
    if price < vwap:
        return "BELOW_VWAP"
    return "AT_VWAP"


def momentum_score(row: dict) -> float | None:
    for key in ("momentum_score", "intraday_technical_score", "weekly_score", "score"):
        value = safe_float(row.get(key), None)
        if value is not None:
            return value
    return None


def classify_setup_type(row: dict) -> str:
    explicit = str(row.get("setup_type") or "").strip().upper()
    if explicit in SETUP_TYPES:
        return explicit

    breakout_status = str(row.get("breakout_status") or row.get("breakout") or "").upper()
    trend = str(row.get("trend") or "").upper()
    reasons = " ".join(str(x) for x in row.get("reasons") or row.get("intraday_score_reasons") or [])
    text = " ".join([breakout_status, trend, reasons]).upper()
    price = safe_float(row.get("price") or row.get("entry_price"), None)
    previous_close = safe_float(row.get("previous_close"), None)
    rv = rvol(row) or 0
    rsi = safe_float(row.get("rsi"), None)
    vw = vwap_status(row)

    if "FAILED_BREAKOUT" in text or "FAILED BREAKOUT" in text:
        return "FAILED_BREAKOUT"
    if "BREAKOUT" in text or breakout_status in {"BREAKOUT", "CONFIRMED", "ABOVE_RESISTANCE"}:
        return "BREAKOUT"
    if previous_close and price and price >= previous_close * 1.02 and rv >= 1.3:
        return "GAP_AND_GO"
    if vw in {"ABOVE_VWAP", "AT_VWAP"} and ("VWAP" in text or rv >= 1.3):
        return "VWAP_BOUNCE"
    if "REVERS" in text or (rsi is not None and rsi < 35):
        return "REVERSAL"
    if "MOMENTUM" in text or trend in {"STRONG BULLISH", "BULLISH"}:
        return "MOMENTUM_CONTINUATION"
    return "UNKNOWN"


def rvol_range(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value < 1:
        return "<1x"
    if value < 1.5:
        return "1-1.5x"
    if value < 2:
        return "1.5-2x"
    if value < 3:
        return "2-3x"
    return "3x+"


def hold_minutes(entry_time: Any, exit_time: Any) -> float | None:
    start = parse_dt(entry_time)
    end = parse_dt(exit_time) or datetime.now(timezone.utc)
    if not start:
        return None
    return round(max((end - start).total_seconds(), 0) / 60, 2)

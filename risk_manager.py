from __future__ import annotations

from config import MAX_RISK_PERCENT, MIN_RR_RATIO, STOP_LOSS_BUFFER, ATR_STOP_MULTIPLIER


def empty_risk() -> dict:
    return {
        "entry_price": None,
        "stop_loss": None,
        "take_profit_1": None,
        "take_profit_2": None,
        "risk_percent": None,
        "risk_per_share": None,
        "rr_ratio": None,
        "risk_ok": False,
    }


def calculate_risk(price: float | None, ma50: float | None, atr: float | None = None) -> dict:
    if price is None or price <= 0:
        return empty_risk()

    entry = price
    stop_candidates: list[float] = []

    if ma50 is not None and ma50 > 0:
        stop_candidates.append(ma50 * STOP_LOSS_BUFFER)
    if atr is not None and atr > 0:
        stop_candidates.append(price - (atr * ATR_STOP_MULTIPLIER))

    if not stop_candidates:
        return empty_risk()

    # For long setup: use the tighter valid stop below entry.
    valid_stops = [s for s in stop_candidates if s < entry]
    if not valid_stops:
        return empty_risk()

    stop_loss = max(valid_stops)
    risk_per_share = entry - stop_loss
    if risk_per_share <= 0:
        return empty_risk()

    risk_percent = (risk_per_share / entry) * 100
    tp1 = entry + risk_per_share * MIN_RR_RATIO
    tp2 = entry + risk_per_share * (MIN_RR_RATIO * 1.5)
    rr_ratio = (tp1 - entry) / risk_per_share

    risk_ok = risk_percent <= MAX_RISK_PERCENT and rr_ratio >= MIN_RR_RATIO

    return {
        "entry_price": round(entry, 4),
        "stop_loss": round(stop_loss, 4),
        "take_profit_1": round(tp1, 4),
        "take_profit_2": round(tp2, 4),
        "risk_percent": round(risk_percent, 2),
        "risk_per_share": round(risk_per_share, 4),
        "rr_ratio": round(rr_ratio, 2),
        "risk_ok": risk_ok,
    }

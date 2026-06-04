from __future__ import annotations

from datetime import datetime, timezone

from strategy_portfolio import STRATEGY_INTRADAY, normalize_strategy_type


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def evaluate_exit(position: dict, signals: dict | None = None) -> dict:
    signals = signals or {}
    if position.get("strategy_type") is not None and normalize_strategy_type(position.get("strategy_type")) != STRATEGY_INTRADAY:
        return {
            "ok": True,
            "triggered": False,
            "reason": "not_intraday_position",
            "position": position,
            "signals": signals,
            "timestamp": _now(),
        }
    pnl_pct = float(signals.get("pnl_pct", 0.0) or 0.0)
    reason = None
    if pnl_pct <= -2.0:
        reason = "hard_stop_loss"
    elif pnl_pct >= 2.0:
        reason = "take_profit_2_to_4"
    elif bool(signals.get("vwap_lost")):
        reason = "vwap_loss"

    return {
        "ok": True,
        "triggered": reason is not None,
        "reason": reason,
        "position": position,
        "signals": signals,
        "timestamp": _now(),
    }

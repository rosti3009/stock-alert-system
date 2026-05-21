from __future__ import annotations

from datetime import datetime, timezone

import config
import database


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def check_execution_safety(symbol: str, side: str = "BUY", metrics: dict | None = None) -> dict:
    metrics = metrics or {}
    blocked: list[str] = []
    warnings: list[str] = []

    if not config.paper_trading_mode_enabled():
        blocked.append("paper_trading_required")
    if bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)):
        blocked.append("real_trading_must_be_disabled")
    if str(getattr(config, "TRADING_MODE", "OFF")).upper() in {"OFF", "LIVE"}:
        blocked.append("trading_mode_not_allowed")

    circuit = await database.get_app_state("circuit_breaker_state")
    if circuit:
        blocked.append("circuit_breaker_tripped")

    spread = float(metrics.get("spread_percent", 0.0) or 0.0)
    if spread > float(getattr(config, "INTRADAY_MAX_SPREAD_PERCENT", config.MAX_SPREAD_PERCENT)):
        blocked.append("spread_too_wide")

    rvol = float(metrics.get("relative_volume", 0.0) or 0.0)
    if rvol and rvol < float(getattr(config, "INTRADAY_MIN_RELATIVE_VOLUME", config.MIN_RELATIVE_VOLUME)):
        blocked.append("relative_volume_too_low")

    return {
        "ok": len(blocked) == 0,
        "blocked_reasons": blocked,
        "warnings": warnings,
        "metrics": metrics,
        "symbol": str(symbol or "").upper(),
        "side": str(side or "BUY").upper(),
        "timestamp": _now(),
    }

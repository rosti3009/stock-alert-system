from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import config
import database

STRATEGY_SWING = "SWING"
STRATEGY_INTRADAY = "INTRADAY"
VALID_STRATEGY_TYPES = {STRATEGY_SWING, STRATEGY_INTRADAY}
ALLOCATION_STATE_KEY = "strategy_allocation_percentages"


def normalize_strategy_type(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in VALID_STRATEGY_TYPES else STRATEGY_SWING


def default_allocation() -> dict[str, float]:
    return {
        STRATEGY_SWING: float(getattr(config, "SWING_CAPITAL_PERCENT", 70.0)),
        STRATEGY_INTRADAY: float(getattr(config, "INTRADAY_CAPITAL_PERCENT", 20.0)),
        "RESERVE": float(getattr(config, "RESERVE_CAPITAL_PERCENT", 10.0)),
    }


def validate_allocation(allocation: dict[str, Any]) -> dict[str, float]:
    merged = {**default_allocation(), **{str(k).strip().upper(): v for k, v in (allocation or {}).items()}}
    result = {
        STRATEGY_SWING: max(0.0, float(merged.get(STRATEGY_SWING, 0) or 0)),
        STRATEGY_INTRADAY: max(0.0, float(merged.get(STRATEGY_INTRADAY, 0) or 0)),
        "RESERVE": max(0.0, float(merged.get("RESERVE", 0) or 0)),
    }
    total = sum(result.values())
    if round(total, 6) != 100.0:
        raise ValueError(f"Strategy allocation must total 100%; got {total:.2f}%")
    return result


async def get_allocation_percentages() -> dict[str, float]:
    raw = await database.get_app_state(ALLOCATION_STATE_KEY)
    if not raw:
        return validate_allocation(default_allocation())
    try:
        import json
        return validate_allocation(json.loads(raw))
    except Exception:
        return validate_allocation(default_allocation())


async def set_allocation_percentages(allocation: dict[str, Any]) -> dict[str, float]:
    import json
    validated = validate_allocation(allocation)
    await database.set_app_state(ALLOCATION_STATE_KEY, json.dumps(validated, sort_keys=True))
    return validated


def _position_value(position: dict[str, Any]) -> float:
    return float(position.get("buy_price") or position.get("entry_price") or 0) * float(position.get("quantity") or 0)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


async def build_strategy_allocation_status() -> dict[str, Any]:
    percentages = await get_allocation_percentages()
    capital = float(config.effective_virtual_trading_capital())
    open_positions = await database.get_open_positions()
    all_positions = await database.get_all_positions(limit=10000)

    realized_by_strategy = {STRATEGY_SWING: 0.0, STRATEGY_INTRADAY: 0.0}
    for pos in all_positions:
        if str(pos.get("status") or "").upper() == "CLOSED":
            realized_by_strategy[normalize_strategy_type(pos.get("strategy_type"))] += _safe_float(pos.get("profit_amount"))

    rows = []
    total_used = 0.0
    total_free = 0.0
    for strategy in (STRATEGY_SWING, STRATEGY_INTRADAY):
        allocated = capital * (percentages[strategy] / 100.0)
        strategy_positions = [p for p in open_positions if normalize_strategy_type(p.get("strategy_type")) == strategy]
        used = sum(_position_value(p) for p in strategy_positions)
        unrealized = sum(_safe_float(p.get("profit_amount")) for p in strategy_positions)
        free = max(0.0, allocated - used)
        total_used += used
        total_free += free
        rows.append({
            "strategy": strategy,
            "capital_percent": percentages[strategy],
            "allocated_capital": round(allocated, 2),
            "used_capital": round(used, 2),
            "free_capital": round(free, 2),
            "open_positions": len(strategy_positions),
            "realized_pnl": round(realized_by_strategy[strategy], 2),
            "unrealized_pnl": round(unrealized, 2),
            "status": "ACTIVE" if free > 0 else "FULLY_ALLOCATED",
        })

    reserve_capital = capital * (percentages["RESERVE"] / 100.0)
    return {
        "ok": True,
        "paper_trading_only": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_capital": round(capital, 2),
        "allocations": percentages,
        "strategies": rows,
        "cards": {
            "swing_capital": round(capital * (percentages[STRATEGY_SWING] / 100.0), 2),
            "intraday_capital": round(capital * (percentages[STRATEGY_INTRADAY] / 100.0), 2),
            "reserve": round(reserve_capital, 2),
            "total_used": round(total_used, 2),
            "total_free": round(total_free, 2),
        },
        "reserve": {"capital_percent": percentages["RESERVE"], "allocated_capital": round(reserve_capital, 2), "used_capital": 0.0, "status": "RESERVED_NEVER_USED"},
    }

from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

import config


class ExecutionQualityState(StrEnum):
    EXECUTION_SAFE = "EXECUTION_SAFE"
    EXECUTION_WARNING = "EXECUTION_WARNING"
    EXECUTION_DANGER = "EXECUTION_DANGER"
    EXECUTION_BLOCK_BUY = "EXECUTION_BLOCK_BUY"


BLOCKING_CATEGORIES = {
    "dangerous_spread",
    "extreme_slippage",
    "low_liquidity",
    "halt_risk",
}


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except Exception:
        return default


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None else None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _threshold(name: str, default: float) -> float:
    return float(getattr(config, name, default))


def _symbol(row: dict[str, Any] | None, quote: dict[str, Any] | None, fallback: str | None) -> str | None:
    value = fallback or (quote or {}).get("symbol") or (row or {}).get("symbol")
    if not value:
        return None
    return str(value).strip().upper()


def _reference_price(row: dict[str, Any] | None, quote: dict[str, Any] | None, limit_price: float | None) -> float:
    row = row or {}
    quote = quote or {}
    for value in (
        quote.get("market_price"),
        quote.get("last"),
        row.get("price"),
        row.get("entry_price"),
        row.get("current_price"),
        quote.get("close"),
        limit_price,
    ):
        parsed = _safe_float(value, None)
        if parsed and parsed > 0:
            return parsed
    return 0.0


def _latest_range_percent(row: dict[str, Any], reference_price: float) -> float | None:
    high = _safe_float(row.get("high") or row.get("day_high") or row.get("latest_high"), None)
    low = _safe_float(row.get("low") or row.get("day_low") or row.get("latest_low"), None)

    highs = row.get("highs") or []
    lows = row.get("lows") or []
    if (high is None or low is None) and highs and lows:
        high = _safe_float(highs[-1], None)
        low = _safe_float(lows[-1], None)

    if high is None or low is None or reference_price <= 0 or high < low:
        return None
    return ((high - low) / reference_price) * 100


def _candle_expansion_percent(row: dict[str, Any], reference_price: float) -> float | None:
    expansion = _safe_float(row.get("candle_expansion_percent"), None)
    if expansion is not None:
        return expansion

    current_range = _latest_range_percent(row, reference_price)
    if current_range is None:
        return None

    ranges = []
    highs = row.get("highs") or []
    lows = row.get("lows") or []
    closes = row.get("closes") or []
    if len(highs) >= 21 and len(lows) >= 21 and len(closes) >= 21:
        for high, low, close in zip(highs[-21:-1], lows[-21:-1], closes[-21:-1]):
            close_value = _safe_float(close, None)
            high_value = _safe_float(high, None)
            low_value = _safe_float(low, None)
            if close_value and close_value > 0 and high_value is not None and low_value is not None and high_value >= low_value:
                ranges.append(((high_value - low_value) / close_value) * 100)

    if not ranges:
        return current_range

    average_range = sum(ranges) / len(ranges)
    if average_range <= 0:
        return current_range
    return (current_range / average_range) * 100


def _estimated_slippage_percent(spread_percent: float | None, row: dict[str, Any], reference_price: float, limit_price: float | None) -> float | None:
    explicit = _safe_float(row.get("estimated_slippage_percent") or row.get("slippage_estimate"), None)
    if explicit is not None:
        return explicit

    if reference_price <= 0:
        return None

    atr_percent = _safe_float(row.get("atr_percent"), 0.0) or 0.0
    half_spread = max(spread_percent or 0.0, 0.0) / 2
    price_drift = 0.0
    if limit_price and limit_price > 0 and reference_price > limit_price:
        price_drift = ((reference_price - limit_price) / limit_price) * 100

    return half_spread + min(max(atr_percent, 0.0) * 0.10, 1.0) + max(price_drift, 0.0)


def evaluate_execution_quality(
    row: dict[str, Any] | None = None,
    quote: dict[str, Any] | None = None,
    limit_price: float | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Evaluate execution quality without changing signal or strategy decisions."""

    row = row or {}
    quote = quote or {}
    thresholds = {
        "max_spread_percent": _threshold("MAX_SPREAD_PERCENT", 3.0),
        "max_spread_dollars": _threshold("MAX_SPREAD_DOLLARS", 0.50),
        "min_relative_volume": _threshold("MIN_RELATIVE_VOLUME", 0.75),
        "min_average_volume": _threshold("MIN_AVERAGE_VOLUME", 500000.0),
        "max_slippage_estimate": _threshold("MAX_SLIPPAGE_ESTIMATE", 1.0),
        "max_intraday_volatility": _threshold("MAX_INTRADAY_VOLATILITY", 6.0),
        "max_candle_expansion_percent": _threshold("MAX_CANDLE_EXPANSION_PERCENT", 250.0),
    }

    bid = _safe_float(quote.get("bid") if quote else row.get("bid"), None)
    ask = _safe_float(quote.get("ask") if quote else row.get("ask"), None)
    reference_price = _reference_price(row, quote, limit_price)

    spread_dollars = None
    spread_percent = None
    if bid is not None and ask is not None:
        spread_dollars = ask - bid
        mid = (bid + ask) / 2
        if mid > 0:
            spread_percent = (spread_dollars / mid) * 100

    avg_volume = _safe_float(row.get("avg_volume") or row.get("average_volume"), None)
    volume = _safe_float(row.get("volume") or row.get("current_volume"), None)
    relative_volume = _safe_float(row.get("relative_volume") or row.get("volume_ratio"), None)
    if relative_volume is None and avg_volume and avg_volume > 0 and volume is not None:
        relative_volume = volume / avg_volume

    intraday_volatility = _safe_float(row.get("intraday_volatility_percent") or row.get("atr_percent"), None)
    if intraday_volatility is None:
        intraday_volatility = _latest_range_percent(row, reference_price)

    candle_expansion = _candle_expansion_percent(row, reference_price)
    average_spread_percent = _safe_float(row.get("average_spread_percent"), None)
    spread_widening = None
    if spread_percent is not None and average_spread_percent and average_spread_percent > 0:
        spread_widening = spread_percent / average_spread_percent

    estimated_slippage = _estimated_slippage_percent(spread_percent, row, reference_price, limit_price)

    warnings: list[str] = []
    dangers: list[str] = []
    block_reasons: list[str] = []
    block_categories: list[str] = []

    def add_warning(message: str) -> None:
        warnings.append(message)

    def add_danger(message: str, category: str | None = None) -> None:
        dangers.append(message)
        if category in BLOCKING_CATEGORIES:
            block_reasons.append(message)
            block_categories.append(str(category))

    if bid is not None and ask is not None:
        if bid <= 0 or ask <= 0 or ask < bid:
            add_danger("Halt-risk quote: invalid or crossed bid/ask", "halt_risk")
        elif spread_percent is not None and spread_dollars is not None:
            if spread_percent > thresholds["max_spread_percent"]:
                add_danger(f"Dangerous spread percent {spread_percent:.2f}%", "dangerous_spread")
            if spread_dollars > thresholds["max_spread_dollars"]:
                add_danger(f"Dangerous absolute spread ${spread_dollars:.4f}", "dangerous_spread")
            if spread_percent > thresholds["max_spread_percent"] * 0.75:
                add_warning(f"Spread percent elevated {spread_percent:.2f}%")
    elif quote:
        add_danger("Halt-risk quote: missing bid/ask", "halt_risk")

    if avg_volume is not None and avg_volume < thresholds["min_average_volume"]:
        add_danger(f"Low average volume {avg_volume:.0f}", "low_liquidity")
    if relative_volume is not None and relative_volume < thresholds["min_relative_volume"]:
        add_danger(f"Low relative volume {relative_volume:.2f}x", "low_liquidity")

    if estimated_slippage is not None and estimated_slippage > thresholds["max_slippage_estimate"]:
        add_danger(f"Extreme slippage estimate {estimated_slippage:.2f}%", "extreme_slippage")

    if intraday_volatility is not None and intraday_volatility > thresholds["max_intraday_volatility"]:
        add_danger(f"Intraday volatility elevated {intraday_volatility:.2f}%")

    if candle_expansion is not None and candle_expansion > thresholds["max_candle_expansion_percent"]:
        add_danger(f"Candle expansion elevated {candle_expansion:.2f}%")

    if spread_widening is not None and spread_widening >= 2.0:
        add_danger(f"Spread widening {spread_widening:.2f}x normal")
    elif spread_widening is not None and spread_widening >= 1.5:
        add_warning(f"Spread widening {spread_widening:.2f}x normal")

    if block_reasons:
        state = ExecutionQualityState.EXECUTION_BLOCK_BUY
    elif dangers:
        state = ExecutionQualityState.EXECUTION_DANGER
    elif warnings:
        state = ExecutionQualityState.EXECUTION_WARNING
    else:
        state = ExecutionQualityState.EXECUTION_SAFE

    return {
        "symbol": _symbol(row, quote, symbol),
        "checked_at": _now_iso(),
        "state": state.value,
        "allowed": state != ExecutionQualityState.EXECUTION_BLOCK_BUY,
        "blocks_buy": state == ExecutionQualityState.EXECUTION_BLOCK_BUY,
        "blocked_buy_reason": "; ".join(dict.fromkeys(block_reasons)) or None,
        "block_categories": list(dict.fromkeys(block_categories)),
        "warnings": warnings,
        "dangers": dangers,
        "metrics": {
            "bid": _round(bid),
            "ask": _round(ask),
            "reference_price": _round(reference_price),
            "spread_percent": _round(spread_percent),
            "spread_dollars": _round(spread_dollars),
            "average_volume": _round(avg_volume, 0),
            "volume": _round(volume, 0),
            "relative_volume": _round(relative_volume),
            "intraday_volatility_percent": _round(intraday_volatility),
            "candle_expansion_percent": _round(candle_expansion),
            "spread_widening_ratio": _round(spread_widening),
            "estimated_slippage_percent": _round(estimated_slippage),
        },
        "thresholds": thresholds,
    }


def summarize_execution_quality(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    severity = {
        ExecutionQualityState.EXECUTION_SAFE.value: 0,
        ExecutionQualityState.EXECUTION_WARNING.value: 1,
        ExecutionQualityState.EXECUTION_DANGER.value: 2,
        ExecutionQualityState.EXECUTION_BLOCK_BUY.value: 3,
    }
    if not evaluations:
        return {
            "checked_at": _now_iso(),
            "state": ExecutionQualityState.EXECUTION_SAFE.value,
            "evaluations": [],
            "blocked_buy_reason": None,
            "spread_risk": "UNKNOWN",
            "liquidity_status": "UNKNOWN",
            "slippage_estimate": None,
        }

    worst = max(evaluations, key=lambda item: severity.get(item.get("state"), 0))
    blocked_reasons = [item.get("blocked_buy_reason") for item in evaluations if item.get("blocked_buy_reason")]
    slippages = [
        item.get("metrics", {}).get("estimated_slippage_percent")
        for item in evaluations
        if item.get("metrics", {}).get("estimated_slippage_percent") is not None
    ]

    return {
        "checked_at": _now_iso(),
        "state": worst.get("state"),
        "evaluations": evaluations,
        "blocked_buy_reason": "; ".join(blocked_reasons) or None,
        "spread_risk": "DANGER" if any("dangerous_spread" in item.get("block_categories", []) for item in evaluations) else ("WARNING" if any("spread" in str(message).lower() for item in evaluations for message in item.get("warnings", [])) else "OK"),
        "liquidity_status": "LOW" if any("low_liquidity" in item.get("block_categories", []) for item in evaluations) else "OK",
        "slippage_estimate": max(slippages) if slippages else None,
    }

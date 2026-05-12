from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

import config
import database
import portfolio_risk_engine
from execution_quality import evaluate_execution_quality, summarize_execution_quality
from market_regime_engine import get_cached_market_regime


class PositionSizingState(StrEnum):
    FULL_SIZE = "FULL_SIZE"
    REDUCED_SIZE = "REDUCED_SIZE"
    SMALL_SIZE = "SMALL_SIZE"
    MICRO_SIZE = "MICRO_SIZE"
    BLOCK_NEW_POSITION = "BLOCK_NEW_POSITION"


STATE_PREFIX = "position_sizing_state"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 4)


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def threshold(name: str, default: float) -> float:
    return safe_float(getattr(config, name, default), default)


def effective_account_equity() -> float:
    return max(0.0, safe_float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0), 5000.0))


def _entry_price(row: dict[str, Any]) -> float:
    for key in ("price", "entry_price", "current_price", "close"):
        value = safe_float(row.get(key))
        if value > 0:
            return value
    return 0.0


def _atr_value(row: dict[str, Any], price: float) -> float:
    atr = safe_float(row.get("atr"))
    if atr > 0:
        return atr
    atr_percent = safe_float(row.get("atr_percent") or row.get("intraday_volatility_percent"))
    if atr_percent > 0 and price > 0:
        return price * (atr_percent / 100.0)
    return 0.0


def _stop_loss(row: dict[str, Any], price: float, atr: float) -> float:
    stop = safe_float(row.get("stop_loss"))
    if stop > 0 and stop < price:
        return stop
    if atr > 0:
        return max(0.01, price - (atr * safe_float(getattr(config, "ATR_STOP_MULTIPLIER", 1.5), 1.5)))
    return price * 0.92


def _relative_volume(row: dict[str, Any]) -> float | None:
    relative = safe_float(row.get("relative_volume") or row.get("volume_ratio"), -1.0)
    if relative >= 0:
        return relative
    avg_volume = safe_float(row.get("avg_volume") or row.get("average_volume"))
    volume = safe_float(row.get("volume") or row.get("current_volume"))
    if avg_volume > 0 and volume > 0:
        return volume / avg_volume
    return None


def _spread_percent(row: dict[str, Any], execution_quality: dict[str, Any] | None) -> float | None:
    metrics = (execution_quality or {}).get("metrics") or {}
    spread = metrics.get("spread_percent")
    if spread is not None:
        return safe_float(spread)
    bid = safe_float(row.get("bid"))
    ask = safe_float(row.get("ask"))
    if bid > 0 and ask >= bid:
        mid = (bid + ask) / 2
        return ((ask - bid) / mid) * 100 if mid > 0 else None
    return None


def _sector_for_symbol(symbol: str) -> str:
    return portfolio_risk_engine.get_sector(symbol)


def _sector_exposure_percent(portfolio_risk: dict[str, Any], sector: str) -> float:
    for item in portfolio_risk.get("exposure_by_sector") or []:
        if str(item.get("sector") or "") == sector:
            return safe_float(item.get("exposure_percent"))
    return 0.0


def _symbol_exposure_percent(portfolio_risk: dict[str, Any], symbol: str) -> float:
    for item in portfolio_risk.get("exposure_by_symbol") or []:
        if str(item.get("symbol") or "").upper() == symbol:
            return safe_float(item.get("exposure_percent"))
    return 0.0


@dataclass(frozen=True)
class PositionSizingInput:
    row: dict[str, Any]
    open_positions: list[dict[str, Any]]
    account_equity: float
    market_regime: dict[str, Any]
    execution_quality: dict[str, Any]
    portfolio_risk: dict[str, Any]
    size_factor: float = 1.0


def evaluate_position_sizing(context: PositionSizingInput) -> dict[str, Any]:
    """Return a read-only position-size recommendation and BUY sizing gate."""

    row = context.row or {}
    symbol = str(row.get("symbol") or "").strip().upper()
    broker_account_equity = max(0.0, safe_float(context.account_equity))
    account_equity = effective_account_equity()
    price = _entry_price(row)
    atr = _atr_value(row, price)
    stop_loss = _stop_loss(row, price, atr) if price > 0 else 0.0
    risk_per_share = max(0.0, price - stop_loss)
    atr_percent = pct(atr, price) if atr > 0 and price > 0 else safe_float(row.get("atr_percent"))

    max_risk_per_trade = account_equity * (threshold("MAX_RISK_PER_TRADE_PERCENT", 1.0) / 100.0)
    max_position_value = account_equity * (threshold("MAX_POSITION_PERCENT", 20.0) / 100.0)

    used = sum(
        safe_float(p.get("buy_price") or p.get("entry_price")) * safe_float(p.get("quantity"))
        for p in context.open_positions or []
        if (p.get("status") or "OPEN") == "OPEN"
    )
    reserve = account_equity * (threshold("MIN_CASH_RESERVE_PERCENT", 10.0) / 100.0)
    available = max(0.0, account_equity - reserve - used)

    portfolio_risk = context.portfolio_risk or {}
    market_regime = context.market_regime or {}
    execution_quality = context.execution_quality or {}
    thresholds = {
        "max_risk_per_trade_percent": threshold("MAX_RISK_PER_TRADE_PERCENT", 1.0),
        "max_position_percent": threshold("MAX_POSITION_PERCENT", 20.0),
        "max_sector_exposure_percent": threshold("MAX_SECTOR_EXPOSURE_PERCENT", 45.0),
        "max_single_symbol_exposure": threshold("MAX_SINGLE_SYMBOL_EXPOSURE", 25.0),
        "high_volatility_reduction": threshold("HIGH_VOLATILITY_REDUCTION", 0.5),
        "low_liquidity_reduction": threshold("LOW_LIQUIDITY_REDUCTION", 0.5),
        "micro_size_threshold": threshold("MICRO_SIZE_THRESHOLD", 0.2),
        "min_average_volume": threshold("MIN_AVERAGE_VOLUME", 500000.0),
        "min_relative_volume": threshold("MIN_RELATIVE_VOLUME", 0.75),
        "max_spread_percent": threshold("MAX_SPREAD_PERCENT", 3.0),
        "max_intraday_volatility": threshold("MAX_INTRADAY_VOLATILITY", 6.0),
        "max_daily_drawdown_percent": threshold("MAX_DAILY_DRAWDOWN_PERCENT", 5.0),
    }

    blocking_reasons: list[str] = []
    reductions: list[str] = []

    avg_volume = safe_float(row.get("avg_volume") or row.get("average_volume"))
    relative_volume = _relative_volume(row)
    spread_percent = _spread_percent(row, execution_quality)

    if price <= 0 or account_equity <= 0 or risk_per_share <= 0:
        blocking_reasons.append("Invalid price, equity, or stop-loss risk for sizing")

    # Volatility and ATR adjustment.
    volatility_adjustment = 1.0
    high_vol_threshold = thresholds["max_intraday_volatility"]
    if atr_percent > high_vol_threshold * 2:
        blocking_reasons.append(f"Dangerous volatility: ATR {atr_percent:.2f}%")
    elif atr_percent > high_vol_threshold:
        volatility_adjustment = clamp(thresholds["high_volatility_reduction"], 0.05, 1.0)
        reductions.append(f"High volatility: ATR {atr_percent:.2f}%")
    elif atr_percent > high_vol_threshold * 0.75:
        volatility_adjustment = 0.75
        reductions.append(f"Elevated volatility: ATR {atr_percent:.2f}%")

    # Liquidity and spread adjustment.
    liquidity_adjustment = 1.0
    if avg_volume > 0 and avg_volume < thresholds["min_average_volume"] * 0.5:
        blocking_reasons.append(f"Insufficient liquidity: average volume {avg_volume:.0f}")
    elif avg_volume > 0 and avg_volume < thresholds["min_average_volume"]:
        liquidity_adjustment = min(liquidity_adjustment, clamp(thresholds["low_liquidity_reduction"], 0.05, 1.0))
        reductions.append(f"Low average volume: {avg_volume:.0f}")

    if relative_volume is not None and relative_volume < thresholds["min_relative_volume"] * 0.5:
        blocking_reasons.append(f"Insufficient liquidity: relative volume {relative_volume:.2f}x")
    elif relative_volume is not None and relative_volume < thresholds["min_relative_volume"]:
        liquidity_adjustment = min(liquidity_adjustment, clamp(thresholds["low_liquidity_reduction"], 0.05, 1.0))
        reductions.append(f"Low relative volume: {relative_volume:.2f}x")

    if spread_percent is not None and spread_percent > thresholds["max_spread_percent"]:
        liquidity_adjustment = min(liquidity_adjustment, 0.5)
        reductions.append(f"Wide spread: {spread_percent:.2f}%")

    # Regime adjustment. Only crash protection blocks here; other regimes resize.
    regime_value = str(market_regime.get("regime") or "NEUTRAL").upper()
    regime_adjustment = clamp(safe_float(market_regime.get("position_size_factor"), 1.0), 0.0, 1.0)
    if regime_value == "CRASH_PROTECTION":
        blocking_reasons.append("Crash protection regime")
    elif regime_adjustment < 1.0:
        reductions.append(f"Market regime size factor {regime_adjustment:.2f}")

    # Execution-quality adjustment. Existing execution gate still owns final execution blocks.
    execution_adjustment = 1.0
    execution_state = str(execution_quality.get("state") or "").upper()
    if execution_state == "EXECUTION_DANGER":
        execution_adjustment = 0.5
        reductions.append("Execution quality danger")
    elif execution_state == "EXECUTION_WARNING":
        execution_adjustment = 0.75
        reductions.append("Execution quality warning")
    elif execution_quality.get("blocks_buy"):
        categories = set(execution_quality.get("block_categories") or [])
        if "low_liquidity" in categories:
            blocking_reasons.append(execution_quality.get("blocked_buy_reason") or "Insufficient liquidity")
        else:
            execution_adjustment = 0.25
            reductions.append(execution_quality.get("blocked_buy_reason") or "Execution quality block reduced sizing")

    # Portfolio risk, open risk, drawdown, and concentration adjustment.
    concentration_adjustment = 1.0
    current_drawdown = max(
        safe_float(portfolio_risk.get("daily_drawdown_percent")),
        safe_float(portfolio_risk.get("unrealized_drawdown_percent")),
    )
    if current_drawdown >= thresholds["max_daily_drawdown_percent"]:
        blocking_reasons.append(f"Extreme drawdown: {current_drawdown:.2f}%")
    elif current_drawdown >= thresholds["max_daily_drawdown_percent"] * 0.8:
        concentration_adjustment = min(concentration_adjustment, 0.5)
        reductions.append(f"Drawdown near limit: {current_drawdown:.2f}%")

    total_open_risk_percent = safe_float(portfolio_risk.get("total_open_risk_percent"))
    if total_open_risk_percent >= thresholds["max_risk_per_trade_percent"] * 4:
        concentration_adjustment = min(concentration_adjustment, 0.5)
        reductions.append(f"Open risk elevated: {total_open_risk_percent:.2f}%")

    sector = _sector_for_symbol(symbol)
    current_sector_exposure = _sector_exposure_percent(portfolio_risk, sector)
    current_symbol_exposure = _symbol_exposure_percent(portfolio_risk, symbol)
    prospective_by_risk = max_risk_per_trade / risk_per_share * price if risk_per_share > 0 else 0.0
    prospective_position_value = min(prospective_by_risk, max_position_value, available)
    projected_position_percent = pct(prospective_position_value, account_equity)
    projected_sector_exposure = current_sector_exposure + projected_position_percent
    projected_symbol_exposure = current_symbol_exposure + projected_position_percent

    if projected_sector_exposure >= thresholds["max_sector_exposure_percent"]:
        blocking_reasons.append(f"Extreme concentration: {sector} sector projected at {projected_sector_exposure:.2f}%")
    elif projected_sector_exposure >= thresholds["max_sector_exposure_percent"] * 0.8:
        concentration_adjustment = min(concentration_adjustment, 0.5)
        reductions.append(f"Sector concentration elevated: {sector} {projected_sector_exposure:.2f}%")

    if projected_symbol_exposure >= thresholds["max_single_symbol_exposure"]:
        blocking_reasons.append(f"Extreme concentration: {symbol} projected at {projected_symbol_exposure:.2f}%")
    elif projected_symbol_exposure >= thresholds["max_single_symbol_exposure"] * 0.8:
        concentration_adjustment = min(concentration_adjustment, 0.5)
        reductions.append(f"Symbol concentration elevated: {symbol} {projected_symbol_exposure:.2f}%")

    total_adjustment = clamp(
        safe_float(context.size_factor, 1.0)
        * volatility_adjustment
        * liquidity_adjustment
        * regime_adjustment
        * execution_adjustment
        * concentration_adjustment,
        0.0,
        1.0,
    )

    base_by_risk = max_risk_per_trade / risk_per_share * price if risk_per_share > 0 else 0.0
    unadjusted_recommendation = min(base_by_risk, max_position_value, available)
    recommended_position_size_usd = 0.0 if blocking_reasons else unadjusted_recommendation * total_adjustment

    if recommended_position_size_usd < threshold("MIN_TRADE_USD", 50.0):
        if blocking_reasons:
            pass
        elif unadjusted_recommendation >= threshold("MIN_TRADE_USD", 50.0):
            recommended_position_size_usd = min(unadjusted_recommendation, threshold("MIN_TRADE_USD", 50.0))
        else:
            blocking_reasons.append("Insufficient available capital for minimum trade size")

    recommended_share_quantity = recommended_position_size_usd / price if price > 0 else 0.0
    if not getattr(config, "ALLOW_FRACTIONAL_SHARES", False):
        recommended_share_quantity = float(int(recommended_share_quantity))
        recommended_position_size_usd = recommended_share_quantity * price

    if blocking_reasons or recommended_share_quantity <= 0:
        state = PositionSizingState.BLOCK_NEW_POSITION
    elif total_adjustment <= thresholds["micro_size_threshold"]:
        state = PositionSizingState.MICRO_SIZE
    elif total_adjustment <= 0.35:
        state = PositionSizingState.SMALL_SIZE
    elif total_adjustment < 0.75:
        state = PositionSizingState.REDUCED_SIZE
    else:
        state = PositionSizingState.FULL_SIZE

    blocks_buy = state == PositionSizingState.BLOCK_NEW_POSITION

    return {
        "symbol": symbol,
        "checked_at": now_iso(),
        "state": state.value,
        "allowed": not blocks_buy,
        "blocks_buy": blocks_buy,
        "block_reasons": list(dict.fromkeys(blocking_reasons)),
        "reduction_reasons": list(dict.fromkeys(reductions)),
        "recommended_position_size_usd": round(recommended_position_size_usd, 2),
        "recommended_share_quantity": round(recommended_share_quantity, 6),
        "quantity": round(recommended_share_quantity, 6),
        "entry_price": round(price, 4),
        "stop_loss": round(stop_loss, 4),
        "position_size": round(recommended_position_size_usd, 2),
        "risk": round(recommended_share_quantity * risk_per_share, 2),
        "account_equity": round(account_equity, 2),
        "effective_equity": round(account_equity, 2),
        "virtual_trading_capital": round(account_equity, 2),
        "broker_account_equity": round(broker_account_equity, 2),
        "risk_calculation_basis": "virtual_trading_capital",
        "available": round(available, 2),
        "used": round(used, 2),
        "reserve": round(reserve, 2),
        "max_risk_per_trade": round(max_risk_per_trade, 2),
        "risk_per_share": round(risk_per_share, 4),
        "volatility_adjustment": round(volatility_adjustment, 4),
        "liquidity_adjustment": round(liquidity_adjustment, 4),
        "regime_adjustment": round(regime_adjustment, 4),
        "execution_adjustment": round(execution_adjustment, 4),
        "concentration_adjustment": round(concentration_adjustment, 4),
        "total_adjustment": round(total_adjustment, 4),
        "inputs": {
            "account_equity": round(account_equity, 2),
            "effective_equity": round(account_equity, 2),
            "virtual_trading_capital": round(account_equity, 2),
            "broker_account_equity": round(broker_account_equity, 2),
            "risk_calculation_basis": "virtual_trading_capital",
            "portfolio_exposure_percent": portfolio_risk.get("total_portfolio_exposure_percent"),
            "open_risk_percent": portfolio_risk.get("total_open_risk_percent"),
            "current_drawdown_percent": round(current_drawdown, 4),
            "market_regime": market_regime.get("regime"),
            "execution_quality_state": execution_quality.get("state"),
            "symbol_volatility_percent": round(atr_percent, 4),
            "atr": round(atr, 4),
            "average_volume": round(avg_volume, 0) if avg_volume else None,
            "relative_volume": round(relative_volume, 4) if relative_volume is not None else None,
            "spread_percent": round(spread_percent, 4) if spread_percent is not None else None,
            "sector": sector,
            "current_sector_exposure_percent": round(current_sector_exposure, 4),
            "current_symbol_exposure_percent": round(current_symbol_exposure, 4),
            "available_capital": round(available, 2),
        },
        "thresholds": thresholds,
    }


def summarize_position_sizing(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    severity = {
        PositionSizingState.FULL_SIZE.value: 0,
        PositionSizingState.REDUCED_SIZE.value: 1,
        PositionSizingState.SMALL_SIZE.value: 2,
        PositionSizingState.MICRO_SIZE.value: 3,
        PositionSizingState.BLOCK_NEW_POSITION.value: 4,
    }
    if not evaluations:
        return {
            "checked_at": now_iso(),
            "state": PositionSizingState.FULL_SIZE.value,
            "evaluations": [],
            "recommended_position_size_usd": 0.0,
            "recommended_share_quantity": 0.0,
            "max_risk_per_trade": 0.0,
            "account_equity": round(effective_account_equity(), 2),
            "effective_equity": round(effective_account_equity(), 2),
            "virtual_trading_capital": round(effective_account_equity(), 2),
            "risk_calculation_basis": "virtual_trading_capital",
            "volatility_adjustment": 1.0,
            "liquidity_adjustment": 1.0,
            "regime_adjustment": 1.0,
            "execution_adjustment": 1.0,
            "concentration_adjustment": 1.0,
            "blocks_buy": False,
            "block_reasons": [],
        }

    worst = max(evaluations, key=lambda item: severity.get(item.get("state"), 0))
    return {
        "checked_at": now_iso(),
        "state": worst.get("state"),
        "evaluations": evaluations,
        "recommended_position_size_usd": round(sum(safe_float(item.get("recommended_position_size_usd")) for item in evaluations), 2),
        "recommended_share_quantity": round(sum(safe_float(item.get("recommended_share_quantity")) for item in evaluations), 6),
        "max_risk_per_trade": worst.get("max_risk_per_trade"),
        "account_equity": worst.get("account_equity"),
        "effective_equity": worst.get("effective_equity"),
        "virtual_trading_capital": worst.get("virtual_trading_capital"),
        "risk_calculation_basis": "virtual_trading_capital",
        "volatility_adjustment": worst.get("volatility_adjustment"),
        "liquidity_adjustment": worst.get("liquidity_adjustment"),
        "regime_adjustment": worst.get("regime_adjustment"),
        "execution_adjustment": worst.get("execution_adjustment"),
        "concentration_adjustment": worst.get("concentration_adjustment"),
        "blocks_buy": any(item.get("blocks_buy") for item in evaluations),
        "block_reasons": list(dict.fromkeys(reason for item in evaluations for reason in item.get("block_reasons", []))),
    }


async def record_position_sizing_event(symbol: str, sizing: dict[str, Any]) -> None:
    symbol = str(symbol or sizing.get("symbol") or "").strip().upper()
    key = f"{STATE_PREFIX}:{symbol or 'GLOBAL'}"
    previous_state = await database.get_app_state(key, "")
    current_state = str(sizing.get("state") or "")
    if previous_state == current_state:
        return

    await database.set_app_state(key, current_state)

    if current_state == PositionSizingState.BLOCK_NEW_POSITION.value:
        event_type = "POSITION_SIZE_BLOCKED"
        decision = "BLOCKED"
        reason = "; ".join(sizing.get("block_reasons") or []) or "Position sizing blocked new BUY."
    elif previous_state == PositionSizingState.BLOCK_NEW_POSITION.value:
        event_type = "POSITION_SIZE_RECOVERED"
        decision = "RECOVERED"
        reason = "Position sizing recovered from blocked state."
    elif current_state != PositionSizingState.FULL_SIZE.value:
        event_type = "POSITION_SIZE_REDUCED"
        decision = current_state
        reason = "; ".join(sizing.get("reduction_reasons") or []) or "Position size reduced by sizing engine."
    else:
        return

    await database.safe_record_trade_journal_event({
        "symbol": symbol,
        "event_type": event_type,
        "decision": decision,
        "reason": reason,
        "source_module": "position_sizing_engine",
        "quantity": sizing.get("recommended_share_quantity"),
        "risk_percent": sizing.get("inputs", {}).get("open_risk_percent"),
        "raw_payload": sizing,
    })


async def build_position_sizing_for_row(row: dict[str, Any], size_factor: float = 1.0) -> dict[str, Any]:
    portfolio_risk = await portfolio_risk_engine.get_portfolio_risk()
    market_regime = await get_cached_market_regime()
    open_positions = await database.get_open_positions()
    account_equity = effective_account_equity()
    symbol = str((row or {}).get("symbol") or "").strip().upper()
    price = _entry_price(row or {})
    execution_quality = evaluate_execution_quality(row=row, limit_price=price, symbol=symbol)
    return evaluate_position_sizing(PositionSizingInput(
        row=row or {},
        open_positions=open_positions,
        account_equity=account_equity,
        market_regime=market_regime,
        execution_quality=execution_quality,
        portfolio_risk=portfolio_risk,
        size_factor=size_factor,
    ))


async def get_position_sizing(rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    rows = rows or []
    portfolio_risk = await portfolio_risk_engine.get_portfolio_risk()
    market_regime = await get_cached_market_regime()
    open_positions = await database.get_open_positions()
    account_equity = effective_account_equity()
    evaluations: list[dict[str, Any]] = []
    execution_evaluations = []
    for row in rows:
        symbol = str((row or {}).get("symbol") or "").strip().upper()
        price = _entry_price(row or {})
        execution_quality = evaluate_execution_quality(row=row, limit_price=price, symbol=symbol)
        execution_evaluations.append(execution_quality)
        evaluations.append(evaluate_position_sizing(PositionSizingInput(
            row=row or {},
            open_positions=open_positions,
            account_equity=account_equity,
            market_regime=market_regime,
            execution_quality=execution_quality,
            portfolio_risk=portfolio_risk,
        )))
    summary = summarize_position_sizing(evaluations)
    summary["portfolio_risk_context"] = portfolio_risk
    summary["market_regime_context"] = market_regime
    summary["execution_quality_context"] = summarize_execution_quality(execution_evaluations)
    return summary


async def get_position_sizing_for_symbol(symbol: str, row: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = str(symbol or "").strip().upper()
    if row is None:
        row = {"symbol": normalized}
    else:
        row = {**row, "symbol": normalized}
    return await build_position_sizing_for_row(row)

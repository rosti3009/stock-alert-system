from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import database
import sector_intelligence
from execution_quality import evaluate_execution_quality
from market_regime_engine import get_cached_market_regime
from portfolio_risk_engine import (
    aggregate_sector_exposure,
    build_position_exposures,
    effective_account_equity,
    get_sector,
    pct,
    safe_float,
)


class ExitPriorityState(str, Enum):
    KEEP = "KEEP"
    REVIEW = "REVIEW"
    REDUCE = "REDUCE"
    HIGH_RISK = "HIGH_RISK"
    EXIT_CANDIDATE = "EXIT_CANDIDATE"


STATE_KEY = "position_exit_priority_states"
LATEST_KEY = "position_exit_priority_latest"


@dataclass(frozen=True)
class ScoreComponent:
    name: str
    score: float
    weight: float
    reason: str | None = None
    flag: str | None = None

    @property
    def weighted_score(self) -> float:
        return max(0.0, min(self.score, self.weight))

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "score": round(self.weighted_score, 4),
            "weight": self.weight,
            "reason": self.reason,
            "flag": self.flag,
        }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def _position_value(position: dict) -> tuple[float, float, float, float]:
    quantity = safe_float(position.get("quantity"))
    buy_price = safe_float(position.get("buy_price"))
    current_price = safe_float(position.get("current_price"), buy_price)
    market_value = max(0.0, quantity * current_price)
    cost_basis = max(0.0, quantity * buy_price)
    return quantity, buy_price, current_price, market_value if market_value > 0 else cost_basis


def _state_for_score(score: float) -> ExitPriorityState:
    if score >= 80:
        return ExitPriorityState.EXIT_CANDIDATE
    if score >= 65:
        return ExitPriorityState.HIGH_RISK
    if score >= 45:
        return ExitPriorityState.REDUCE
    if score >= 25:
        return ExitPriorityState.REVIEW
    return ExitPriorityState.KEEP


def _event_for_transition(previous: str | None, current: str) -> tuple[str, str] | None:
    severity = {
        ExitPriorityState.KEEP.value: 0,
        ExitPriorityState.REVIEW.value: 1,
        ExitPriorityState.REDUCE.value: 2,
        ExitPriorityState.HIGH_RISK.value: 3,
        ExitPriorityState.EXIT_CANDIDATE.value: 4,
    }
    current_level = severity.get(current, 0)
    previous_level = severity.get(previous or "", 0)

    if current == ExitPriorityState.EXIT_CANDIDATE.value and previous != current:
        return "POSITION_EXIT_CANDIDATE", "REVIEW_EXIT_CANDIDATE"
    if current in {ExitPriorityState.REDUCE.value, ExitPriorityState.HIGH_RISK.value} and current_level > previous_level:
        return "POSITION_REVIEW_REQUIRED", current
    if previous_level >= 3 and current_level == 2:
        return "POSITION_RISK_REDUCED", current
    if previous_level >= 2 and current_level <= 1:
        return "POSITION_RECOVERED", current
    return None


def _component(name: str, raw_score: float, weight: float, reason: str | None = None, flag: str | None = None) -> ScoreComponent:
    return ScoreComponent(name=name, score=clamp(raw_score, 0.0, weight), weight=weight, reason=reason, flag=flag)


def _loss_component(position: dict, buy_price: float, current_price: float, quantity: float) -> ScoreComponent:
    profit_percent = safe_float(position.get("profit_percent"), None)
    if profit_percent is None and buy_price > 0:
        profit_percent = ((current_price - buy_price) / buy_price) * 100
    unrealized_pnl = (current_price - buy_price) * quantity if quantity > 0 and buy_price > 0 else safe_float(position.get("profit_amount"))
    loss_percent = abs(min(0.0, profit_percent or 0.0))
    score = min(14.0, loss_percent * 1.4)
    reason = None
    flag = None
    if loss_percent >= 10:
        reason = f"Unrealized loss {loss_percent:.2f}% (${unrealized_pnl:.2f})."
        flag = "UNREALIZED_PNL_PRESSURE"
    elif loss_percent >= 4:
        reason = f"Unrealized PnL is negative by {loss_percent:.2f}%."
        flag = "UNREALIZED_PNL_WEAK"
    return _component("unrealized_pnl", score, 14.0, reason, flag)


def _momentum_components(row: dict, current_price: float) -> list[ScoreComponent]:
    momentum_5d = safe_float(row.get("momentum_5d"), None)
    momentum_20d = safe_float(row.get("momentum_20d"), None)
    momentum_60d = safe_float(row.get("momentum_60d"), None)
    ma20 = safe_float(row.get("ma20"), None)
    ma50 = safe_float(row.get("ma50"), None)
    ma200 = safe_float(row.get("ma200"), None)
    trend = str(row.get("trend") or "").upper()

    deterioration = 0.0
    reasons: list[str] = []
    if momentum_5d is not None and momentum_5d < 0:
        deterioration += min(4.0, abs(momentum_5d) * 0.8)
        reasons.append(f"5d momentum {momentum_5d:.2f}%")
    if momentum_20d is not None and momentum_20d < 0:
        deterioration += min(5.0, abs(momentum_20d) * 0.5)
        reasons.append(f"20d momentum {momentum_20d:.2f}%")
    if ma20 and current_price < ma20:
        deterioration += 2.0
        reasons.append("price below MA20")
    if ma50 and current_price < ma50:
        deterioration += 2.0
        reasons.append("price below MA50")
    if "BEAR" in trend:
        deterioration += 2.0
        reasons.append(f"{trend.title()} trend")

    relative = 0.0
    if momentum_20d is not None:
        if momentum_20d < -8:
            relative += 6.0
        elif momentum_20d < -3:
            relative += 4.0
        elif momentum_20d < 0:
            relative += 2.0
    if momentum_60d is not None and momentum_60d < -5:
        relative += 2.0
    if "BEAR" in trend:
        relative += 2.0

    return [
        _component(
            "relative_weakness_vs_market",
            relative,
            10.0,
            "Relative weakness vs market proxies: " + ", ".join(reasons) if reasons and relative else None,
            "RELATIVE_WEAKNESS" if relative >= 5 else None,
        ),
        _component(
            "momentum_deterioration",
            deterioration,
            11.0,
            "Momentum deterioration: " + ", ".join(reasons) if reasons else None,
            "MOMENTUM_DETERIORATION" if deterioration >= 6 else None,
        ),
    ]


def _execution_liquidity_components(row: dict, symbol: str) -> list[ScoreComponent]:
    quality = evaluate_execution_quality(row=row, symbol=symbol)
    metrics = quality.get("metrics", {}) or {}
    state = str(quality.get("state") or "")
    dangers = quality.get("dangers") or []
    warnings = quality.get("warnings") or []
    rel_volume = safe_float(metrics.get("relative_volume"), None)
    avg_volume = safe_float(metrics.get("average_volume"), None)
    spread_percent = safe_float(metrics.get("spread_percent"), None)
    spread_widening = safe_float(metrics.get("spread_widening_ratio"), None)
    slippage = safe_float(metrics.get("estimated_slippage_percent"), None)

    execution = 0.0
    if "BLOCK" in state:
        execution = 7.0
    elif "DANGER" in state:
        execution = 5.5
    elif "WARNING" in state:
        execution = 3.0
    if slippage is not None:
        execution = max(execution, min(7.0, slippage * 3.0))

    liquidity = 0.0
    if avg_volume is not None and avg_volume < 250000:
        liquidity += 4.0
    elif avg_volume is not None and avg_volume < 500000:
        liquidity += 2.0
    if rel_volume is not None and rel_volume < 0.4:
        liquidity += 3.0
    elif rel_volume is not None and rel_volume < 0.75:
        liquidity += 2.0

    spread = 0.0
    if spread_percent is not None:
        if spread_percent > 3:
            spread += 6.0
        elif spread_percent > 1.5:
            spread += 4.0
        elif spread_percent > 0.75:
            spread += 2.0
    if spread_widening is not None and spread_widening >= 2:
        spread += 2.0

    volume = 0.0
    if rel_volume is not None:
        if rel_volume < 0.25:
            volume = 6.0
        elif rel_volume < 0.5:
            volume = 4.0
        elif rel_volume < 0.75:
            volume = 2.0

    quality_reason = "; ".join(dangers[:2] or warnings[:2]) or None
    return [
        _component("execution_quality_deterioration", execution, 7.0, quality_reason, "EXECUTION_QUALITY_DANGER" if execution >= 5 else None),
        _component("liquidity_deterioration", liquidity, 7.0, quality_reason, "LIQUIDITY_DETERIORATION" if liquidity >= 4 else None),
        _component("spread_deterioration", spread, 6.0, quality_reason, "SPREAD_DETERIORATION" if spread >= 4 else None),
        _component("volume_collapse", volume, 6.0, f"Relative volume collapsed to {rel_volume:.2f}x." if rel_volume is not None and rel_volume < 0.75 else None, "VOLUME_COLLAPSE" if volume >= 4 else None),
    ]


def _portfolio_components(
    position: dict,
    sector_totals: dict[str, dict],
    market_value: float,
    account_equity: float,
    total_market_value: float,
    total_negative_pnl: float,
    portfolio_open_risk: float,
) -> list[ScoreComponent]:
    symbol = str(position.get("symbol") or "").upper()
    sector = get_sector(symbol)
    sector_exposure = safe_float(sector_totals.get(sector, {}).get("exposure_percent"))
    exposure_percent = pct(market_value, account_equity)
    unrealized = safe_float(position.get("profit_amount"))
    buy_price = safe_float(position.get("buy_price"))
    current_price = safe_float(position.get("current_price"), buy_price)
    quantity = safe_float(position.get("quantity"))
    stop_loss = safe_float(position.get("stop_loss"))
    open_risk = max(0.0, (current_price - stop_loss) * quantity) if stop_loss > 0 else max(0.0, market_value * 0.05)
    risk_contribution = pct(open_risk, portfolio_open_risk) if portfolio_open_risk > 0 else pct(market_value, total_market_value)
    drawdown_contribution = pct(abs(min(0.0, unrealized)), total_negative_pnl) if total_negative_pnl > 0 else 0.0

    concentration = 0.0
    if sector_exposure >= 45:
        concentration = 6.0
    elif sector_exposure >= 36:
        concentration = 4.0
    elif sector_exposure >= 25:
        concentration = 2.0

    size = 0.0
    if exposure_percent >= 25:
        size = 7.0
    elif exposure_percent >= 20:
        size = 5.0
    elif exposure_percent >= 15:
        size = 3.0

    drawdown = 0.0
    if drawdown_contribution >= 50:
        drawdown = 6.0
    elif drawdown_contribution >= 30:
        drawdown = 4.0
    elif drawdown_contribution >= 15:
        drawdown = 2.0

    risk = 0.0
    if risk_contribution >= 50:
        risk = 6.0
    elif risk_contribution >= 30:
        risk = 4.0
    elif risk_contribution >= 20:
        risk = 2.0

    cluster = 0.0
    sector_symbols = sector_totals.get(sector, {}).get("symbols") or []
    if len(sector_symbols) >= 4 and sector_exposure >= 35:
        cluster = 4.0
    elif len(sector_symbols) >= 3 and sector_exposure >= 25:
        cluster = 3.0
    elif len(sector_symbols) >= 2 and sector_exposure >= 20:
        cluster = 1.5

    return [
        _component("sector_overconcentration", concentration, 6.0, f"{sector} exposure is {sector_exposure:.2f}%." if concentration else None, "SECTOR_OVERCONCENTRATION" if concentration >= 4 else None),
        _component("position_size_vs_virtual_capital", size, 7.0, f"Position uses {exposure_percent:.2f}% of virtual capital." if size else None, "POSITION_SIZE_LARGE" if size >= 5 else None),
        _component("drawdown_contribution", drawdown, 6.0, f"Contributes {drawdown_contribution:.2f}% of portfolio unrealized drawdown." if drawdown else None, "DRAWDOWN_CONTRIBUTOR" if drawdown >= 4 else None),
        _component("portfolio_risk_contribution", risk, 6.0, f"Contributes {risk_contribution:.2f}% of open portfolio risk." if risk else None, "PORTFOLIO_RISK_CONCENTRATED" if risk >= 4 else None),
        _component("correlation_clustering", cluster, 4.0, f"{len(sector_symbols)} holdings clustered in {sector}." if cluster else None, "CORRELATION_CLUSTER" if cluster >= 3 else None),
    ]


def _regime_component(row: dict, market_regime: dict) -> ScoreComponent:
    regime = str(market_regime.get("regime") or "UNKNOWN").upper()
    trend = str(row.get("trend") or "").upper()
    score = 0.0
    if regime in {"RISK_OFF", "CRASH_PROTECTION"}:
        score = 6.0 if "BEAR" in trend or safe_float(row.get("momentum_20d")) < 0 else 4.0
    elif regime == "DEFENSIVE":
        score = 4.0 if "BEAR" in trend or safe_float(row.get("momentum_5d")) < 0 else 2.0
    return _component("market_regime_compatibility", score, 6.0, f"Holding is weak during {regime} regime." if score >= 4 else None, "REGIME_INCOMPATIBLE" if score >= 4 else None)


def _stale_gap_vol_components(position: dict, row: dict) -> list[ScoreComponent]:
    opened_at = parse_dt(position.get("buy_date") or position.get("created_at"))
    held_days = None
    stale_score = 0.0
    if opened_at:
        held_days = max(0, (datetime.now(timezone.utc) - opened_at).days)
        profit_percent = safe_float(position.get("profit_percent"))
        if held_days >= 45 and profit_percent <= 0:
            stale_score = 5.0
        elif held_days >= 21 and profit_percent <= 0:
            stale_score = 3.0
        elif held_days >= 60:
            stale_score = 2.0

    atr_percent = safe_float(row.get("atr_percent"), None)
    atr_score = 0.0
    if atr_percent is not None:
        if atr_percent >= 10:
            atr_score = 5.0
        elif atr_percent >= 6:
            atr_score = 3.0
        elif atr_percent >= 4:
            atr_score = 1.5

    gap_score = 0.0
    gap_percent = safe_float(row.get("gap_percent") or row.get("overnight_gap_percent"), None)
    if gap_percent is not None:
        if abs(gap_percent) >= 5:
            gap_score = 4.0
        elif abs(gap_percent) >= 2.5:
            gap_score = 2.0
    elif atr_percent is not None and atr_percent >= 6:
        gap_score = 2.0

    return [
        _component("atr_volatility_expansion", atr_score, 5.0, f"ATR volatility is {atr_percent:.2f}% of price." if atr_score and atr_percent is not None else None, "ATR_VOLATILITY_EXPANSION" if atr_score >= 3 else None),
        _component("time_held_stale_position", stale_score, 5.0, f"Held {held_days} days without positive PnL." if stale_score and held_days is not None else None, "STALE_POSITION" if stale_score >= 3 else None),
        _component("gap_overnight_risk", gap_score, 4.0, f"Gap/overnight risk elevated ({gap_percent:.2f}%)." if gap_percent is not None and gap_score else ("ATR implies elevated overnight gap risk." if gap_score else None), "GAP_OVERNIGHT_RISK" if gap_score >= 3 else None),
    ]


def evaluate_position_exit_priority(
    position: dict,
    market_row: dict | None = None,
    *,
    account_equity: float | None = None,
    sector_totals: dict[str, dict] | None = None,
    total_market_value: float = 0.0,
    total_negative_pnl: float = 0.0,
    portfolio_open_risk: float = 0.0,
    market_regime: dict | None = None,
) -> dict[str, Any]:
    symbol = str(position.get("symbol") or "").strip().upper()
    row = {**(market_row or {})}
    row.setdefault("symbol", symbol)
    quantity, buy_price, current_price, market_value = _position_value(position)
    equity = account_equity if account_equity is not None else effective_account_equity()
    sector_totals = sector_totals or {}
    market_regime = market_regime or {}
    classification = sector_intelligence.classify_symbol(symbol)

    unrealized_pnl = (current_price - buy_price) * quantity if quantity > 0 and buy_price > 0 else safe_float(position.get("profit_amount"))
    profit_percent = safe_float(position.get("profit_percent"), None)
    if profit_percent is None and buy_price > 0:
        profit_percent = ((current_price - buy_price) / buy_price) * 100
    stop_loss = safe_float(position.get("stop_loss"))
    open_risk = max(0.0, (current_price - stop_loss) * quantity) if stop_loss > 0 else max(0.0, market_value * 0.05)

    components: list[ScoreComponent] = []
    components.append(_loss_component(position, buy_price, current_price, quantity))
    components.extend(_momentum_components(row, current_price))
    components.extend(_execution_liquidity_components(row, symbol))
    components.extend(_portfolio_components(position, sector_totals, market_value, equity, total_market_value, total_negative_pnl, portfolio_open_risk))
    components.append(_regime_component(row, market_regime))
    components.extend(_stale_gap_vol_components(position, row))

    score = round(clamp(sum(component.weighted_score for component in components)), 2)
    state = _state_for_score(score).value
    ranked_components = sorted(components, key=lambda item: item.weighted_score, reverse=True)
    primary = next((item.reason for item in ranked_components if item.reason), "No material exit-priority risks detected.")
    secondary = next((item.reason for item in ranked_components if item.reason and item.reason != primary), None)
    risk_flags = [item.flag for item in ranked_components if item.flag]

    return {
        "symbol": symbol,
        "checked_at": now_iso(),
        "priority_state": state,
        "exit_priority_score": score,
        "primary_reason": primary,
        "secondary_reason": secondary,
        "risk_flags": list(dict.fromkeys(risk_flags)),
        "capital_locked": round(market_value, 2),
        "portfolio_risk_contribution": pct(open_risk, portfolio_open_risk) if portfolio_open_risk > 0 else pct(market_value, total_market_value),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "unrealized_pnl_percent": round(profit_percent or 0.0, 4),
        "market_value": round(market_value, 2),
        "exposure_percent": pct(market_value, equity),
        "sector": get_sector(symbol),
        "industry": classification.get("industry"),
        "theme": classification.get("theme"),
        "correlation_cluster": classification.get("correlation_cluster"),
        "sector_intelligence": classification,
        "quantity": round(quantity, 6),
        "current_price": round(current_price, 4),
        "buy_price": round(buy_price, 4),
        "recommendation_only": True,
        "read_only": True,
        "components": [item.as_dict() for item in ranked_components],
    }


def evaluate_exit_priorities(
    positions: list[dict],
    market_rows: dict[str, dict] | None = None,
    *,
    account_equity: float | None = None,
    market_regime: dict | None = None,
    checked_at: str | None = None,
) -> dict[str, Any]:
    rows = {str(k).upper(): v for k, v in (market_rows or {}).items()}
    equity = account_equity if account_equity is not None else effective_account_equity()
    exposures = build_position_exposures(positions, equity)
    sector_exposures = aggregate_sector_exposure(exposures, equity)
    sector_summary = sector_intelligence.build_portfolio_summary(positions, equity, checked_at=checked_at)
    sector_totals = {item["sector"]: item for item in sector_exposures}
    total_market_value = sum(item.market_value for item in exposures)
    total_negative_pnl = sum(abs(min(0.0, item.unrealized_pnl)) for item in exposures)
    portfolio_open_risk = sum(item.open_risk for item in exposures)

    evaluations = [
        evaluate_position_exit_priority(
            position,
            rows.get(str(position.get("symbol") or "").upper(), {"symbol": position.get("symbol")}),
            account_equity=equity,
            sector_totals=sector_totals,
            total_market_value=total_market_value,
            total_negative_pnl=total_negative_pnl,
            portfolio_open_risk=portfolio_open_risk,
            market_regime=market_regime or {},
        )
        for position in positions
        if str(position.get("status") or "OPEN").upper() == "OPEN"
    ]
    evaluations.sort(key=lambda item: item["exit_priority_score"], reverse=True)

    exit_candidates = [item for item in evaluations if item["priority_state"] == ExitPriorityState.EXIT_CANDIDATE.value]
    high_risk = [item for item in evaluations if item["priority_state"] in {ExitPriorityState.HIGH_RISK.value, ExitPriorityState.EXIT_CANDIDATE.value}]
    capital_trapped_positions = [item for item in evaluations if item["priority_state"] in {ExitPriorityState.REDUCE.value, ExitPriorityState.HIGH_RISK.value, ExitPriorityState.EXIT_CANDIDATE.value}]
    weakest_momentum = [item for item in evaluations if any(flag in item["risk_flags"] for flag in ("MOMENTUM_DETERIORATION", "RELATIVE_WEAKNESS"))]
    concentration_warnings = [
        {
            "sector": sector.get("sector"),
            "exposure_percent": sector.get("exposure_percent"),
            "market_value": sector.get("market_value"),
            "symbols": sector.get("symbols"),
            "message": f"{sector.get('sector')} exposure is {sector.get('exposure_percent')}% across {len(sector.get('symbols') or [])} holdings.",
        }
        for sector in sector_exposures
        if safe_float(sector.get("exposure_percent")) >= 25 or len(sector.get("symbols") or []) >= 3
    ]

    capital_trapped = sum(item["capital_locked"] for item in capital_trapped_positions)
    return {
        "checked_at": checked_at or now_iso(),
        "read_only": True,
        "recommendation_only": True,
        "no_order_actions": True,
        "positions": evaluations,
        "worst_positions": evaluations[:5],
        "exit_candidates": exit_candidates,
        "high_risk_positions": high_risk,
        "weakest_momentum_positions": weakest_momentum[:5],
        "highest_risk_positions": high_risk[:5],
        "capital_trapped": round(capital_trapped, 2),
        "capital_trapped_percent": pct(capital_trapped, equity),
        "capital_trapped_positions": capital_trapped_positions,
        "concentration_warnings": concentration_warnings,
        "sector_intelligence": sector_summary,
        "correlation_clusters": sector_summary["correlation_clusters"],
        "diversification_score": sector_summary["diversification_score"],
        "portfolio": {
            "account_equity": round(equity, 2),
            "total_market_value": round(total_market_value, 2),
            "total_open_risk": round(portfolio_open_risk, 2),
            "open_positions": len(evaluations),
        },
        "priority_counts": {state.value: sum(1 for item in evaluations if item["priority_state"] == state.value) for state in ExitPriorityState},
        "market_regime": market_regime or {},
    }


async def _record_position_events(snapshot: dict[str, Any]) -> None:
    raw_previous = await database.get_app_state(STATE_KEY, "{}")
    try:
        previous_states = json.loads(raw_previous or "{}")
    except Exception:
        previous_states = {}

    current_states = {item["symbol"]: item["priority_state"] for item in snapshot.get("positions", [])}
    await database.set_app_state(STATE_KEY, json.dumps(current_states, ensure_ascii=False, default=str))

    for item in snapshot.get("positions", []):
        symbol = item.get("symbol")
        current = item.get("priority_state")
        previous = previous_states.get(symbol)
        transition = _event_for_transition(previous, current)
        if not transition:
            continue
        event_type, decision = transition
        await database.safe_record_trade_journal_event({
            "symbol": symbol,
            "event_type": event_type,
            "decision": decision,
            "reason": item.get("primary_reason"),
            "source_module": "position_exit_priority_engine",
            "price": item.get("current_price"),
            "quantity": item.get("quantity"),
            "unrealized_pnl": item.get("unrealized_pnl"),
            "risk_percent": item.get("portfolio_risk_contribution"),
            "market_regime": (snapshot.get("market_regime") or {}).get("regime"),
            "raw_payload": item,
        })


async def get_position_exit_priority(market_rows: dict[str, dict] | None = None, record_events: bool = True) -> dict[str, Any]:
    positions = await database.get_open_positions()
    market_regime = await get_cached_market_regime()
    snapshot = evaluate_exit_priorities(
        positions,
        market_rows=market_rows,
        account_equity=effective_account_equity(),
        market_regime=market_regime,
    )
    await database.set_app_state(LATEST_KEY, json.dumps(snapshot, ensure_ascii=False, default=str))
    if record_events:
        await _record_position_events(snapshot)
    return snapshot


async def get_position_exit_priority_for_symbol(symbol: str, market_row: dict | None = None, record_events: bool = True) -> dict[str, Any] | None:
    normalized = str(symbol or "").strip().upper()
    snapshot = await get_position_exit_priority({normalized: market_row or {"symbol": normalized}}, record_events=record_events)
    for item in snapshot.get("positions", []):
        if item.get("symbol") == normalized:
            return item
    return None

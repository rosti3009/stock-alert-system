from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any

import config
import database
from execution_quality import evaluate_execution_quality
from strategy_portfolio import STRATEGY_INTRADAY, STRATEGY_SWING, normalize_strategy_type
from trading_safety import get_market_hours_status

STRATEGY_REJECT = "REJECT"
QUALITY_COMPONENTS = (
    "trend_score",
    "momentum_score",
    "volume_score",
    "liquidity_score",
    "volatility_score",
    "risk_reward_score",
    "execution_score",
    "market_regime_score",
    "strategy_fit_score",
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _first_float(row: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if row.get(key) not in (None, ""):
            return _safe_float(row.get(key), default)
    return default


def _boolish(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "y", "ok", "above"}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _score_from_threshold(value: float, minimum: float, excellent: float) -> float:
    if value <= 0:
        return 0.0
    if value < minimum:
        return _clamp((value / minimum) * 55.0)
    if excellent <= minimum:
        return 100.0
    return _clamp(70.0 + ((value - minimum) / (excellent - minimum)) * 30.0)


def _inverse_score(value: float | None, maximum: float, excellent: float = 0.0) -> float:
    if value is None:
        return 65.0
    if value <= excellent:
        return 100.0
    if value >= maximum:
        return 0.0
    return _clamp(100.0 - ((value - excellent) / (maximum - excellent)) * 100.0)


def _is_stale(row: dict[str, Any]) -> bool:
    if _boolish(row.get("stale") or row.get("is_stale") or row.get("stale_market_data")):
        return True
    ts = row.get("quote_timestamp") or row.get("market_data_time") or row.get("last_update")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        max_age = float(getattr(config, "TRADE_QUALITY_MAX_DATA_AGE_SECONDS", 300.0))
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() > max_age
    except Exception:
        return False


@dataclass(frozen=True)
class StrategyThresholds:
    min_quality_score: float
    min_relative_volume: float
    min_dollar_volume: float
    max_spread_percent: float
    max_slippage_estimate: float
    force_close_before_market_close: bool = False
    max_hold_days: int | None = None


def thresholds_for(strategy_type: str) -> StrategyThresholds:
    if strategy_type == STRATEGY_INTRADAY:
        return StrategyThresholds(
            min_quality_score=float(getattr(config, "TRADE_QUALITY_INTRADAY_MIN_SCORE", 75.0)),
            min_relative_volume=float(getattr(config, "TRADE_QUALITY_INTRADAY_MIN_RELATIVE_VOLUME", getattr(config, "INTRADAY_MIN_RELATIVE_VOLUME", 1.5))),
            min_dollar_volume=float(getattr(config, "TRADE_QUALITY_INTRADAY_MIN_DOLLAR_VOLUME", 3_000_000.0)),
            max_spread_percent=float(getattr(config, "TRADE_QUALITY_INTRADAY_MAX_SPREAD_PERCENT", 1.5)),
            max_slippage_estimate=float(getattr(config, "TRADE_QUALITY_INTRADAY_MAX_SLIPPAGE_ESTIMATE", 1.0)),
            force_close_before_market_close=True,
            max_hold_days=1,
        )
    return StrategyThresholds(
        min_quality_score=float(getattr(config, "TRADE_QUALITY_SWING_MIN_SCORE", 70.0)),
        min_relative_volume=float(getattr(config, "TRADE_QUALITY_SWING_MIN_RELATIVE_VOLUME", 0.8)),
        min_dollar_volume=float(getattr(config, "TRADE_QUALITY_SWING_MIN_DOLLAR_VOLUME", 1_000_000.0)),
        max_spread_percent=float(getattr(config, "TRADE_QUALITY_SWING_MAX_SPREAD_PERCENT", 2.5)),
        max_slippage_estimate=float(getattr(config, "TRADE_QUALITY_SWING_MAX_SLIPPAGE_ESTIMATE", 1.5)),
        force_close_before_market_close=False,
        max_hold_days=int(getattr(config, "TRADE_QUALITY_SWING_MAX_HOLD_DAYS", getattr(config, "SWING_MAX_HOLD_DAYS", 20))),
    )


def _market_session_ok(strategy_type: str, market_hours: dict[str, Any] | None, row: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if strategy_type != STRATEGY_INTRADAY:
        return True, reasons
    status = market_hours if market_hours is not None else get_market_hours_status()
    if not bool(status.get("allowed", True)):
        reasons.append(status.get("reason") or "Market session does not allow intraday entries")
    minutes_to_close = row.get("minutes_to_close") or status.get("minutes_to_close")
    if minutes_to_close is not None and _safe_float(minutes_to_close, 9999) <= float(getattr(config, "INTRADAY_FORCE_EXIT_MINUTES_BEFORE_CLOSE", 15)):
        reasons.append("Too close to market close for intraday entry")
    return not reasons, reasons


def _candidate_metrics(row: dict[str, Any], execution_quality: dict[str, Any] | None = None) -> dict[str, float | None]:
    eq = execution_quality or evaluate_execution_quality(row=row, symbol=row.get("symbol"))
    metrics = eq.get("metrics", {}) or {}
    price = _first_float(row, "price", "entry_price", "current_price", default=_safe_float(metrics.get("reference_price"), 0.0))
    dollar_volume_value = metrics.get("dollar_volume")
    dollar_volume = _first_float(row, "dollar_volume", default=_safe_float(dollar_volume_value, 0.0))
    avg_volume = _first_float(row, "avg_volume", "average_volume", default=_safe_float(metrics.get("average_volume"), 0.0))
    if dollar_volume <= 0 and avg_volume and price:
        dollar_volume = avg_volume * price
    rel_volume_value = metrics.get("relative_volume")
    rel_volume = _first_float(row, "relative_volume", "volume_ratio", default=_safe_float(rel_volume_value, 0.0))
    has_liquidity_data = any(row.get(k) not in (None, "") for k in ("dollar_volume", "avg_volume", "average_volume", "volume", "current_volume", "relative_volume", "volume_ratio")) or dollar_volume_value is not None or rel_volume_value is not None
    return {
        "price": price,
        "relative_volume": rel_volume,
        "dollar_volume": dollar_volume,
        "spread_percent": _safe_float(row.get("spread_percent"), _safe_float(metrics.get("spread_percent"), 0.0)),
        "slippage_estimate": _safe_float(row.get("slippage_estimate"), _safe_float(metrics.get("estimated_slippage_percent"), 0.0)),
        "atr_percent": _first_float(row, "atr_percent", "intraday_volatility_percent", default=_safe_float(metrics.get("intraday_volatility_percent"), 0.0)),
        "has_liquidity_data": has_liquidity_data,
    }


def _components_for(row: dict[str, Any], strategy_type: str, market_regime: dict[str, Any] | None, execution_quality: dict[str, Any]) -> tuple[dict[str, float], dict[str, Any], list[str]]:
    thresholds = thresholds_for(strategy_type)
    metrics = _candidate_metrics(row, execution_quality)
    price = _safe_float(metrics.get("price"), 0.0)
    ma20 = _first_float(row, "ma20", "sma20", "ema20")
    ma50 = _first_float(row, "ma50", "sma50")
    ema9 = _first_float(row, "ema9", "ema_9", "ema9_1m", "ema9_5m")
    ema20 = _first_float(row, "ema20", "ema_20", "ema20_1m", "ema20_5m")
    vwap = _first_float(row, "vwap", "intraday_vwap")
    momentum_5d = _first_float(row, "momentum_5d", "change_5d")
    momentum_20d = _first_float(row, "momentum_20d", "change_20d")
    intraday_momentum = _first_float(row, "aggressive_score", "intraday_aggressive_score", "intraday_momentum_score", "momentum_score", "momentum_percent", "change_percent")
    rr = _first_float(row, "rr_ratio", "risk_reward", default=0.0)
    has_liquidity_data = bool(metrics.get("has_liquidity_data"))
    rel_volume = _safe_float(metrics.get("relative_volume"), 0.0)
    dollar_volume = _safe_float(metrics.get("dollar_volume"), 0.0)
    scoring_rel_volume = rel_volume if has_liquidity_data else thresholds.min_relative_volume
    scoring_dollar_volume = dollar_volume if has_liquidity_data else thresholds.min_dollar_volume
    spread = metrics.get("spread_percent")
    slippage = metrics.get("slippage_estimate")
    atr_percent = _safe_float(metrics.get("atr_percent"), 0.0)
    rejection_reasons: list[str] = []

    if strategy_type == STRATEGY_INTRADAY:
        trend_points = [price > 0 and vwap > 0 and price > vwap, price > 0 and ema9 > 0 and price > ema9, price > 0 and ema20 > 0 and price > ema20]
        trend_score = 40.0 + 20.0 * sum(1 for ok in trend_points if ok)
        momentum_score = _score_from_threshold(intraday_momentum, 60.0, 90.0) if intraday_momentum > 5 else _score_from_threshold(intraday_momentum, 2.0, 8.0)
        strategy_fit_score = 100.0 if ((price > vwap > 0 or vwap <= 0) and scoring_rel_volume >= thresholds.min_relative_volume) else 60.0
    else:
        above_ma20 = price > 0 and ma20 > 0 and price > ma20
        above_ma50 = price > 0 and ma50 > 0 and price > ma50
        trend_score = 35.0 + (40.0 if above_ma20 else 0.0) + (25.0 if above_ma50 else 0.0)
        momentum_score = _clamp(50.0 + momentum_5d * 3.0 + momentum_20d * 1.5)
        strategy_fit_score = 100.0 if above_ma20 and momentum_5d > 0 and momentum_20d > 0 else 55.0

    volume_score = (_score_from_threshold(scoring_rel_volume, thresholds.min_relative_volume, thresholds.min_relative_volume * 2.0) * 0.55) + (_score_from_threshold(scoring_dollar_volume, thresholds.min_dollar_volume, thresholds.min_dollar_volume * 5.0) * 0.45)
    liquidity_score = _score_from_threshold(scoring_dollar_volume, thresholds.min_dollar_volume, thresholds.min_dollar_volume * 6.0)
    volatility_score = _inverse_score(atr_percent if atr_percent > 0 else None, 12.0 if strategy_type == STRATEGY_SWING else 8.0, 2.0)
    risk_reward_score = _score_from_threshold(rr, 1.5, 3.0) if rr else 65.0
    execution_score = (_inverse_score(spread, thresholds.max_spread_percent, 0.1) * 0.55) + (_inverse_score(slippage, thresholds.max_slippage_estimate, 0.05) * 0.45)
    regime = str((market_regime or {}).get("regime") or row.get("market_regime") or "NEUTRAL").upper()
    allow_new_buys = bool((market_regime or {}).get("allow_new_buys", True))
    if strategy_type == STRATEGY_SWING and (regime == "RISK_OFF" or not allow_new_buys):
        market_regime_score = 0.0
    elif regime in {"RISK_OFF", "BEAR"}:
        market_regime_score = 40.0
    else:
        market_regime_score = 100.0

    components = {
        "trend_score": round(_clamp(trend_score), 2),
        "momentum_score": round(_clamp(momentum_score), 2),
        "volume_score": round(_clamp(volume_score), 2),
        "liquidity_score": round(_clamp(liquidity_score), 2),
        "volatility_score": round(_clamp(volatility_score), 2),
        "risk_reward_score": round(_clamp(risk_reward_score), 2),
        "execution_score": round(_clamp(execution_score), 2),
        "market_regime_score": round(_clamp(market_regime_score), 2),
        "strategy_fit_score": round(_clamp(strategy_fit_score), 2),
    }

    if has_liquidity_data and rel_volume < thresholds.min_relative_volume:
        rejection_reasons.append(f"Relative volume {rel_volume:.2f}x below {thresholds.min_relative_volume:.2f}x")
    if has_liquidity_data and dollar_volume < thresholds.min_dollar_volume:
        rejection_reasons.append(f"Dollar volume ${dollar_volume:,.0f} below ${thresholds.min_dollar_volume:,.0f}")
    if spread is not None and spread > thresholds.max_spread_percent:
        rejection_reasons.append(f"Spread {spread:.2f}% above {thresholds.max_spread_percent:.2f}%")
    if slippage is not None and slippage > thresholds.max_slippage_estimate:
        rejection_reasons.append(f"Slippage {slippage:.2f}% above {thresholds.max_slippage_estimate:.2f}%")
    if _is_stale(row):
        rejection_reasons.append("Stale quote or market data")
    if strategy_type == STRATEGY_INTRADAY:
        if price > 0 and vwap > 0 and price <= vwap:
            rejection_reasons.append("Price is below VWAP")
        if price > 0 and ema9 > 0 and price <= ema9:
            rejection_reasons.append("Price is below EMA9")
        if price > 0 and ema20 > 0 and price <= ema20:
            rejection_reasons.append("Price is below EMA20")
    else:
        if ma20 > 0 and price <= ma20:
            rejection_reasons.append("Price is below MA20")
        if momentum_5d <= 0 or momentum_20d <= 0:
            rejection_reasons.append("Conflicting or weak 5d/20d momentum")
        if atr_percent >= 12.0:
            rejection_reasons.append("ATR volatility is dangerously high")
        if market_regime_score <= 0:
            rejection_reasons.append("Market regime blocks SWING strategy")
        if _boolish(row.get("major_deterioration") or row.get("deterioration_signal")):
            rejection_reasons.append("Major deterioration signal detected")
    return components, metrics, rejection_reasons


def _quality_grade(score: float, rejected: bool) -> str:
    if rejected:
        return "REJECT"
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    return "C"


def classify_candidate(row: dict[str, Any], market_regime: dict[str, Any] | None = None, market_hours: dict[str, Any] | None = None) -> dict[str, Any]:
    row = dict(row or {})
    symbol = str(row.get("symbol") or "").strip().upper()
    execution_quality = evaluate_execution_quality(row=row, symbol=symbol)
    evaluations: dict[str, dict[str, Any]] = {}
    for strategy_type in (STRATEGY_INTRADAY, STRATEGY_SWING):
        components, metrics, rejection_reasons = _components_for(row, strategy_type, market_regime, execution_quality)
        session_ok, session_reasons = _market_session_ok(strategy_type, market_hours, row)
        rejection_reasons.extend(session_reasons)
        score = sum(components.values()) / len(components)
        thresholds = thresholds_for(strategy_type)
        if score < thresholds.min_quality_score:
            rejection_reasons.append(f"Quality score {score:.1f} below {thresholds.min_quality_score:.0f}")
        rejected = bool(rejection_reasons)
        suggested_stop = _suggest_stop(row, strategy_type, metrics)
        take_profit = _suggest_take_profit(row, strategy_type, suggested_stop)
        evaluations[strategy_type] = {
            "symbol": symbol,
            "strategy_type": STRATEGY_REJECT if rejected else strategy_type,
            "candidate_strategy_type": strategy_type,
            "trade_quality_score": round(score, 2),
            "quality_components": components,
            "quality_grade": _quality_grade(score, rejected),
            "primary_reason": (rejection_reasons[0] if rejected else f"{strategy_type} setup passed trade-quality filters"),
            "rejection_reasons": list(dict.fromkeys(rejection_reasons)),
            "suggested_holding_period": "same day; force close before market close" if strategy_type == STRATEGY_INTRADAY else f"up to {thresholds.max_hold_days or 20} trading days",
            "suggested_stop_loss": suggested_stop,
            "suggested_take_profit": take_profit,
            "suggested_trailing_stop": _suggest_trailing_stop(row, strategy_type, metrics),
            "execution_risk": {
                "spread_percent": metrics.get("spread_percent"),
                "slippage_estimate": metrics.get("slippage_estimate"),
                "dollar_volume": metrics.get("dollar_volume"),
                "relative_volume": metrics.get("relative_volume"),
                "state": execution_quality.get("state"),
            },
            "thresholds": thresholds.__dict__,
            "execution_quality": execution_quality,
        }

    preferred = normalize_strategy_type(row.get("strategy_type")) if row.get("strategy_type") else None
    ordered = []
    if preferred in evaluations and evaluations[preferred]["quality_grade"] != "REJECT":
        ordered.append(evaluations[preferred])
    ordered.extend(sorted((v for v in evaluations.values() if v not in ordered and v["quality_grade"] != "REJECT"), key=lambda v: v["trade_quality_score"], reverse=True))
    chosen = ordered[0] if ordered else max(evaluations.values(), key=lambda v: v["trade_quality_score"])
    if not ordered:
        chosen = {**chosen, "strategy_type": STRATEGY_REJECT, "quality_grade": "REJECT"}
        all_reasons = []
        for ev in evaluations.values():
            all_reasons.extend(ev.get("rejection_reasons") or [])
        chosen["rejection_reasons"] = list(dict.fromkeys(all_reasons))
        chosen["primary_reason"] = chosen["rejection_reasons"][0] if chosen["rejection_reasons"] else "Candidate rejected by quality classifier"
    return {**chosen, "evaluated_strategies": evaluations}


def _suggest_stop(row: dict[str, Any], strategy_type: str, metrics: dict[str, Any]) -> float | None:
    explicit = _safe_float(row.get("stop_loss"), 0.0)
    price = _safe_float(metrics.get("price"), _first_float(row, "price", "entry_price"))
    if explicit > 0:
        return round(explicit, 4)
    if price <= 0:
        return None
    atr = _safe_float(row.get("atr"), 0.0)
    if atr > 0:
        multiplier = 1.0 if strategy_type == STRATEGY_INTRADAY else 1.8
        return round(max(0.01, price - atr * multiplier), 4)
    return round(price * (0.985 if strategy_type == STRATEGY_INTRADAY else 0.92), 4)


def _suggest_take_profit(row: dict[str, Any], strategy_type: str, stop: float | None) -> float | None:
    explicit = _safe_float(row.get("take_profit_1") or row.get("take_profit"), 0.0)
    price = _first_float(row, "price", "entry_price")
    if explicit > 0:
        return round(explicit, 4)
    if price <= 0:
        return None
    if stop and stop > 0 and stop < price:
        rr = 1.5 if strategy_type == STRATEGY_INTRADAY else 2.0
        return round(price + (price - stop) * rr, 4)
    return round(price * (1.025 if strategy_type == STRATEGY_INTRADAY else 1.12), 4)


def _suggest_trailing_stop(row: dict[str, Any], strategy_type: str, metrics: dict[str, Any]) -> float | None:
    price = _safe_float(metrics.get("price"), _first_float(row, "price", "entry_price"))
    if price <= 0:
        return None
    return round(price * (0.99 if strategy_type == STRATEGY_INTRADAY else 0.97), 4)


async def classify_latest_candidates(limit: int = 200) -> list[dict[str, Any]]:
    rows = await database.get_latest_candidates(limit=limit)
    return [classify_candidate(row) for row in rows]


async def build_trade_quality_summary(candidates: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    candidates = candidates if candidates is not None else await classify_latest_candidates()
    by_grade = Counter(item.get("quality_grade") for item in candidates)
    by_strategy = Counter(item.get("strategy_type") for item in candidates)
    rejected_reasons = Counter(reason for item in candidates for reason in (item.get("rejection_reasons") or []))
    analytics = await build_quality_performance_analytics()
    return {
        "ok": True,
        "candidate_count": len(candidates),
        "by_grade": dict(by_grade),
        "by_strategy": dict(by_strategy),
        "rejected_reason_frequency": dict(rejected_reasons.most_common(20)),
        "top_swing_count": sum(1 for item in candidates if item.get("strategy_type") == STRATEGY_SWING),
        "top_intraday_count": sum(1 for item in candidates if item.get("strategy_type") == STRATEGY_INTRADAY),
        "analytics": analytics,
    }


async def build_quality_performance_analytics() -> dict[str, Any]:
    outcomes = await database.get_trade_outcomes(limit=1000)
    positions = await database.get_all_positions(limit=1000)
    rejected = await database.get_rejected_setups(limit=1000)
    grade_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
    strategy_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"trades": 0, "pnl": 0.0, "hold_minutes_total": 0.0})
    component_winners: Counter[str] = Counter()
    by_symbol = {p.get("symbol"): p for p in positions}
    for outcome in outcomes:
        raw = outcome.get("raw_json")
        try:
            raw_payload = json.loads(raw or "{}") if isinstance(raw, str) else (raw or {})
        except Exception:
            raw_payload = {}
        pos = by_symbol.get(outcome.get("symbol"), {})
        grade = raw_payload.get("quality_grade") or pos.get("quality_grade") or "UNKNOWN"
        strategy = normalize_strategy_type(raw_payload.get("strategy_type") or pos.get("strategy_type"))
        pnl = _safe_float(outcome.get("profit_amount"), 0.0)
        grade_stats[grade]["trades"] += 1
        grade_stats[grade]["wins"] += 1 if pnl > 0 else 0
        grade_stats[grade]["pnl"] += pnl
        strategy_stats[strategy]["trades"] += 1
        strategy_stats[strategy]["pnl"] += pnl
        strategy_stats[strategy]["hold_minutes_total"] += _safe_float(outcome.get("hold_minutes"), 0.0)
        if pnl > 0:
            comps = raw_payload.get("quality_components") or pos.get("quality_components") or {}
            if isinstance(comps, str):
                try:
                    comps = json.loads(comps)
                except Exception:
                    comps = {}
            for name, value in (comps or {}).items():
                if _safe_float(value, 0.0) >= 80:
                    component_winners[name] += 1
    return {
        "win_rate_by_quality_grade": {k: round((v["wins"] / v["trades"] * 100.0) if v["trades"] else 0.0, 2) for k, v in grade_stats.items()},
        "pnl_by_quality_grade": {k: round(v["pnl"], 2) for k, v in grade_stats.items()},
        "pnl_by_strategy_type": {k: round(v["pnl"], 2) for k, v in strategy_stats.items()},
        "average_hold_time_by_strategy": {k: round((v["hold_minutes_total"] / v["trades"]) if v["trades"] else 0.0, 2) for k, v in strategy_stats.items()},
        "rejected_reasons_frequency": dict(Counter(item.get("failed_filter") or item.get("rejection_reason") or "UNKNOWN" for item in rejected).most_common(20)),
        "best_scoring_components_that_predict_winners": dict(component_winners.most_common(10)),
    }

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

import logging

import config
from strategy_portfolio import STRATEGY_INTRADAY, STRATEGY_SWING, normalize_strategy_type

log = logging.getLogger(__name__)

RANKING_COMPONENTS = (
    "technical_score",
    "trend_score",
    "momentum_score",
    "relative_volume_score",
    "dollar_volume_score",
    "liquidity_score",
    "spread_score",
    "slippage_score",
    "volatility_score",
    "risk_reward_score",
    "market_regime_score",
    "strategy_fit_score",
)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float) -> int:
    return int(max(0, min(100, round(value))))


def _grade(score: float, minimum: float | None = None) -> str:
    min_score = float(getattr(config, "RANKING_MIN_SCORE", 65.0) if minimum is None else minimum)
    if score < min_score:
        return "REJECT"
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    return "C"


def _relative_volume(row: dict) -> float:
    explicit = _f(row.get("relative_volume") or row.get("volume_ratio") or row.get("intraday_relative_volume"), 0.0)
    if explicit:
        return explicit
    avg = _f(row.get("avg_volume") or row.get("average_volume"), 0.0)
    vol = _f(row.get("volume") or row.get("current_volume"), 0.0)
    return vol / avg if avg > 0 and vol > 0 else 0.0


def _dollar_volume(row: dict) -> float:
    explicit = _f(row.get("dollar_volume") or row.get("intraday_dollar_volume"), 0.0)
    if explicit:
        return explicit
    return _f(row.get("price") or row.get("current_price") or row.get("entry_price"), 0.0) * _f(row.get("volume"), 0.0)


def _spread(row: dict) -> float | None:
    for key in ("spread_percent", "average_spread_percent"):
        if row.get(key) is not None:
            return _f(row.get(key), 0.0)
    bid = _f(row.get("bid"), 0.0)
    ask = _f(row.get("ask"), 0.0)
    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0
    return ((ask - bid) / mid) * 100 if mid > 0 else None


def _slippage(row: dict) -> float | None:
    for key in ("slippage_estimate", "estimated_slippage_percent", "max_slippage_estimate"):
        if row.get(key) is not None:
            return _f(row.get(key), 0.0)
    return None


def calculate_ranking(row: dict, strategy_type: str | None = None, *, minimum_score: float | None = None) -> dict:
    strategy = normalize_strategy_type(strategy_type or row.get("strategy_type"))
    price = _f(row.get("price") or row.get("current_price") or row.get("entry_price"), 0.0)
    rv = _relative_volume(row)
    dv = _dollar_volume(row)
    spread = _spread(row)
    slippage = _slippage(row)
    score_value = _f(row.get("intraday_momentum_score") or row.get("aggressive_score") or row.get("weekly_score") or row.get("score"), 0.0)
    trend_text = str(row.get("trend") or row.get("intraday_trend") or "").lower()
    setup_text = str(row.get("setup") or row.get("intraday_setup") or row.get("reason") or "").lower()
    regime = str(row.get("market_regime") or row.get("regime") or "").upper()
    ma20 = _f(row.get("ma20"), 0.0)
    ma50 = _f(row.get("ma50"), 0.0)
    vwap = _f(row.get("vwap"), 0.0)
    ema9 = _f(row.get("ema9"), 0.0)
    rr = _f(row.get("rr_ratio"), 0.0)
    atr_pct = _f(row.get("atr_percent") or row.get("atr_pct"), 0.0)
    momentum = max(_f(row.get("momentum_score"), 0.0), _f(row.get("momentum_5d"), 0.0) * 8, _f(row.get("momentum_20d"), 0.0) * 4, _f(row.get("intraday_price_change_percent") or row.get("change_percent"), 0.0) * 12)

    components: dict[str, int] = {}
    components["technical_score"] = _clamp(score_value)
    components["trend_score"] = _clamp(85 if "strong" in trend_text and "bull" in trend_text else 75 if "bull" in trend_text else 60 if price and ((ma20 and price > ma20) or (ema9 and price > ema9)) else 35)
    components["momentum_score"] = _clamp(momentum if momentum else score_value)
    components["relative_volume_score"] = _clamp((rv / (3.0 if strategy == STRATEGY_INTRADAY else 1.8)) * 100)
    components["dollar_volume_score"] = _clamp((dv / (10_000_000 if strategy == STRATEGY_INTRADAY else 5_000_000)) * 100)
    components["liquidity_score"] = _clamp((components["relative_volume_score"] * 0.4) + (components["dollar_volume_score"] * 0.6))
    components["spread_score"] = 75 if spread is None else _clamp(100 - (spread / (2.5 if strategy == STRATEGY_INTRADAY else 3.0)) * 100)
    components["slippage_score"] = 75 if slippage is None else _clamp(100 - (slippage / 1.5) * 100)
    components["volatility_score"] = _clamp(85 if atr_pct == 0 else 100 - abs(atr_pct - (3.0 if strategy == STRATEGY_INTRADAY else 4.0)) * 12)
    components["risk_reward_score"] = _clamp(50 + rr * 20 if rr else 65)
    components["market_regime_score"] = 35 if regime == "DEFENSIVE" else 90 if regime in {"BULLISH", "RISK_ON", "MOMENTUM_EXCEPTION"} else 70

    if strategy == STRATEGY_INTRADAY:
        fit = 0
        fit += 25 if rv >= 1.5 else 0
        fit += 20 if price and vwap and price >= vwap else 0
        fit += 20 if price and ema9 and price >= ema9 else 0
        fit += 20 if any(t in setup_text for t in ("breakout", "momentum", "opening range", "vwap")) else 0
        fit += 15 if spread is None or spread <= 2.5 else 0
        weights = {
            "technical_score": 0.13, "trend_score": 0.10, "momentum_score": 0.13,
            "relative_volume_score": 0.14, "dollar_volume_score": 0.07, "liquidity_score": 0.08,
            "spread_score": 0.10, "slippage_score": 0.10, "volatility_score": 0.05,
            "risk_reward_score": 0.04, "market_regime_score": 0.03, "strategy_fit_score": 0.13,
        }
    else:
        fit = 0
        fit += 25 if price and ma20 and price >= ma20 else 0
        fit += 20 if price and ma50 and price >= ma50 else 0
        fit += 20 if _f(row.get("momentum_5d"), 0.0) >= 0 or _f(row.get("momentum_20d"), 0.0) >= 0 else 0
        fit += 20 if regime != "DEFENSIVE" else 0
        fit += 15 if "bull" in trend_text or "continuation" in setup_text else 0
        weights = {
            "technical_score": 0.12, "trend_score": 0.15, "momentum_score": 0.14,
            "relative_volume_score": 0.07, "dollar_volume_score": 0.08, "liquidity_score": 0.10,
            "spread_score": 0.06, "slippage_score": 0.05, "volatility_score": 0.08,
            "risk_reward_score": 0.08, "market_regime_score": 0.04, "strategy_fit_score": 0.13,
        }
    components["strategy_fit_score"] = _clamp(fit)
    if strategy == STRATEGY_INTRADAY and (row.get("aggressive_entry_allowed") or row.get("intraday_entry_allowed") or row.get("entry_allowed")):
        for key in ("technical_score", "trend_score", "momentum_score", "relative_volume_score", "dollar_volume_score", "liquidity_score", "spread_score", "slippage_score", "strategy_fit_score"):
            components[key] = max(components[key], 75)
    ranking_score = _clamp(sum(components[k] * weights[k] for k in RANKING_COMPONENTS))
    grade = _grade(ranking_score, minimum_score)
    reason_bits = [f"{strategy} rank {ranking_score}/100 grade {grade}"]
    if grade == "REJECT":
        reason_bits.append("below ranking minimum")
    elif strategy == STRATEGY_INTRADAY:
        reason_bits.append("intraday preference: RVOL/VWAP/spread/slippage/momentum")
    else:
        reason_bits.append("swing preference: MA trend/momentum/liquidity/regime")
    return {
        "strategy_type": strategy,
        "ranking_score": ranking_score,
        "ranking_grade": grade,
        "ranking_components": components,
        "ranking_reason": "; ".join(reason_bits),
        "rejected_by_ranking": grade == "REJECT",
        "ranking_checked_at": datetime.now(timezone.utc).isoformat(),
    }


def rank_candidates(rows: list[dict], strategy_type: str, *, top_n: int | None = None, minimum_score: float | None = None) -> dict[str, list[dict]]:
    strategy = normalize_strategy_type(strategy_type)
    limit = int(top_n if top_n is not None else (getattr(config, "INTRADAY_TOP_N", 5) if strategy == STRATEGY_INTRADAY else getattr(config, "SWING_TOP_N", 5)))
    ranked: list[dict] = []
    for row in rows:
        enriched = {**row, "strategy_type": strategy}
        ranking = calculate_ranking(enriched, strategy, minimum_score=minimum_score)
        ranked.append({**enriched, **ranking})
    ranked.sort(key=lambda item: (item.get("ranking_score") or 0, item.get("score") or item.get("weekly_score") or 0), reverse=True)
    selected: list[dict] = []
    rejected: list[dict] = []
    for idx, row in enumerate(ranked, start=1):
        row = {**row, "ranking_rank": idx}
        if idx <= limit and not row.get("rejected_by_ranking"):
            selected.append(row)
        else:
            rejected.append({**row, "rejected_by_ranking": True, "ranking_reason": row.get("ranking_reason") if row.get("rejected_by_ranking") else f"Outside top {limit} {strategy} candidates"})
    return {"selected": selected, "rejected": rejected, "ranked": selected + rejected}


# Existing weekly ranking API kept for compatibility.
def calculate_weekly_score(row: dict) -> tuple[int, list[str]]:
    ranking = calculate_ranking({**row, "strategy_type": STRATEGY_SWING}, STRATEGY_SWING)
    if str(row.get("signal") or row.get("signal_type") or "").upper() != "BUY":
        return 0, ["Only BUY signals are eligible for weekly TOP ranking"]
    return int(ranking["ranking_score"]), [ranking["ranking_reason"]]


def rank_top_weekly_setups(rows: list[dict], limit: int = 10) -> list[dict]:
    ranked = rank_candidates([r for r in rows if str(r.get("signal") or "").upper() == "BUY"], STRATEGY_SWING, top_n=limit)["selected"]
    for index, row in enumerate(ranked, start=1):
        row["weekly_rank"] = index
        row["weekly_score"] = row.get("ranking_score")
        row["weekly_reasons"] = [row.get("ranking_reason")]
    return ranked


def intraday_debug_thresholds() -> dict[str, float]:
    return {
        "min_score": float(getattr(config, "RANKING_MIN_SCORE", 65.0) or 0.0),
        "min_relative_volume": float(getattr(config, "INTRADAY_MIN_RELATIVE_VOLUME", getattr(config, "MIN_RELATIVE_VOLUME", 1.0)) or 0.0),
        "min_average_volume": float(getattr(config, "MIN_AVG_VOLUME", getattr(config, "MIN_AVERAGE_VOLUME", 0.0)) or 0.0),
        "min_dollar_volume": float(getattr(config, "INTRADAY_MIN_DOLLAR_VOLUME", getattr(config, "MIN_DOLLAR_VOLUME", 0.0)) or 0.0),
    }


def _intraday_average_volume(row: dict) -> float:
    return _f(row.get("avg_volume") or row.get("average_volume") or row.get("volume") or row.get("current_volume"), 0.0)


def intraday_filter_rejection_reasons(row: dict, thresholds: dict[str, float] | None = None) -> list[str]:
    thresholds = thresholds or intraday_debug_thresholds()
    reasons: list[str] = []
    if _relative_volume(row) < thresholds["min_relative_volume"]:
        reasons.append("low_relative_volume")
    if _intraday_average_volume(row) < thresholds["min_average_volume"]:
        reasons.append("low_average_volume")
    if _dollar_volume(row) < thresholds["min_dollar_volume"]:
        reasons.append("low_dollar_volume")
    score = _f(row.get("ranking_score") or row.get("intraday_momentum_score") or row.get("aggressive_score") or row.get("score"), 0.0)
    if score < thresholds["min_score"]:
        reasons.append("score_too_low")
    if str(row.get("signal") or "").strip().upper() != "BUY":
        reasons.append("not_buy_signal")
    if _f(row.get("entry_price"), 0.0) <= 0:
        reasons.append("missing_entry_price")
    if _f(row.get("stop_loss"), 0.0) <= 0:
        reasons.append("missing_stop_loss")
    if row.get("rejected_by_ranking"):
        reasons.append("rejected_by_ranking")
    return list(dict.fromkeys(reasons))


def build_intraday_ranking_debug(rows: list[dict], *, final_candidates: list[dict] | None = None, top_log_limit: int = 25) -> dict[str, Any]:
    thresholds = intraday_debug_thresholds()
    scanned = len(rows)
    pass_counts = defaultdict(int)
    rejection_counter: Counter[str] = Counter()
    rejected_symbols_by_reason: dict[str, list[str]] = defaultdict(list)
    diagnostics: list[dict[str, Any]] = []

    ranked_rows: list[dict] = []
    for row in rows:
        enriched = {**row, "strategy_type": STRATEGY_INTRADAY}
        if enriched.get("ranking_score") is None:
            enriched = {**enriched, **calculate_ranking(enriched, STRATEGY_INTRADAY, minimum_score=thresholds["min_score"])}
        ranked_rows.append(enriched)

    ranked_rows.sort(key=lambda item: (_f(item.get("ranking_score"), 0.0), _f(item.get("score") or item.get("weekly_score"), 0.0)), reverse=True)

    for row in ranked_rows:
        symbol = str(row.get("symbol") or "").upper()
        rv = _relative_volume(row)
        avg_volume = _intraday_average_volume(row)
        dollar_volume = _dollar_volume(row)
        ranking_score = _f(row.get("ranking_score"), 0.0)
        passed_volume = avg_volume >= thresholds["min_average_volume"] and dollar_volume >= thresholds["min_dollar_volume"]
        passed_relative_volume = passed_volume and rv >= thresholds["min_relative_volume"]
        passed_score = passed_relative_volume and ranking_score >= thresholds["min_score"]
        reasons = intraday_filter_rejection_reasons(row, thresholds)
        passed_risk = passed_score and not any(reason in reasons for reason in ("not_buy_signal", "missing_entry_price", "missing_stop_loss", "rejected_by_ranking"))
        if passed_volume:
            pass_counts["passed_volume"] += 1
        if passed_relative_volume:
            pass_counts["passed_relative_volume"] += 1
        if passed_score:
            pass_counts["passed_score"] += 1
        if passed_risk:
            pass_counts["passed_risk"] += 1
        if reasons:
            rejection_counter.update(reasons)
            for reason in reasons:
                if symbol and len(rejected_symbols_by_reason[reason]) < 50:
                    rejected_symbols_by_reason[reason].append(symbol)
        diagnostics.append({
            "symbol": symbol,
            "ranking_score": ranking_score,
            "relative_volume": rv,
            "average_volume": avg_volume,
            "dollar_volume": dollar_volume,
            "rejection_reasons": reasons,
            "ranking_reason": row.get("ranking_reason"),
        })

    for row in diagnostics[:max(0, int(top_log_limit or 0))]:
        if row["rejection_reasons"]:
            log.info(
                "INTRADAY ranking rejection symbol=%s score=%.2f rvol=%.2f avg_volume=%.0f dollar_volume=%.0f reasons=%s ranking_reason=%s",
                row["symbol"],
                row["ranking_score"],
                row["relative_volume"],
                row["average_volume"],
                row["dollar_volume"],
                ",".join(row["rejection_reasons"]),
                row.get("ranking_reason"),
            )

    final_count = len(final_candidates) if final_candidates is not None else pass_counts["passed_risk"]
    payload: dict[str, Any] = {
        "scanned": scanned,
        "passed_volume": pass_counts["passed_volume"],
        "passed_relative_volume": pass_counts["passed_relative_volume"],
        "passed_score": pass_counts["passed_score"],
        "passed_risk": pass_counts["passed_risk"],
        "final_candidates": final_count,
        "top_rejections": dict(rejection_counter.most_common(10)),
        "thresholds": thresholds,
        "rejected_symbols_by_reason": dict(rejected_symbols_by_reason),
        "top_ranked_diagnostics": diagnostics[:max(0, int(top_log_limit or 0))],
    }
    if final_count == 0 and rejection_counter:
        recommendations = []
        most_common = rejection_counter.most_common(3)
        for reason, _count in most_common:
            if reason == "low_relative_volume":
                recommendations.append(f"Consider lowering min_relative_volume below {thresholds['min_relative_volume']:.2f} or prioritizing symbols with live RVOL enrichment.")
            elif reason == "low_dollar_volume":
                recommendations.append(f"Consider lowering min_dollar_volume below ${thresholds['min_dollar_volume']:,.0f} if fills remain acceptable.")
            elif reason == "low_average_volume":
                recommendations.append(f"Consider lowering min_average_volume below {thresholds['min_average_volume']:,.0f} or expanding the scanner universe to more liquid symbols.")
            elif reason == "score_too_low":
                recommendations.append(f"Consider lowering min_score below {thresholds['min_score']:.0f} or tuning intraday scoring inputs.")
        payload["recommendations"] = recommendations
    return payload

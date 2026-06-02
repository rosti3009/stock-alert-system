from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import config
from strategy_portfolio import STRATEGY_INTRADAY, STRATEGY_SWING, normalize_strategy_type

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

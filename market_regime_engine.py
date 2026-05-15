from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

import config
import database
import watchdog
from reason_summarizer import summarize_reason_list
import sector_intelligence
from data_fetcher import fetch_stock_data
from indicators import compute_indicators

log = logging.getLogger(__name__)


class MarketRegimeState(str, Enum):
    STRONG_BULL = "STRONG_BULL"
    BULL = "BULL"
    NEUTRAL = "NEUTRAL"
    DEFENSIVE = "DEFENSIVE"
    RISK_OFF = "RISK_OFF"
    CRASH_PROTECTION = "CRASH_PROTECTION"


STATE_KEY = "market_regime_engine_state"
LATEST_KEY = "market_regime_engine_latest"
HISTORY_KEY = "market_regime_engine_history"
MAX_HISTORY = 200


@dataclass(frozen=True)
class RegimeRecommendation:
    recommended_max_exposure: float
    recommended_position_size: float
    allow_new_buys: bool
    allow_aggressive_entries: bool
    allow_averaging_down: bool
    allow_breakout_entries: bool

    def as_dict(self) -> dict:
        return {
            "recommended_max_exposure": self.recommended_max_exposure,
            "recommended_position_size": self.recommended_position_size,
            "allow_new_buys": self.allow_new_buys,
            "allow_aggressive_entries": self.allow_aggressive_entries,
            "allow_averaging_down": self.allow_averaging_down,
            "allow_breakout_entries": self.allow_breakout_entries,
        }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pct_change(new_value: float | None, old_value: float | None) -> float | None:
    if new_value is None or old_value in (None, 0):
        return None
    return round(((new_value - old_value) / old_value) * 100, 4)


def configured_threshold(name: str, default: float) -> float:
    return safe_float(getattr(config, name, default), default)


def _ma_ok(price: float | None, ma: float | None) -> bool:
    return price is not None and ma is not None and price >= ma


def _trend_summary(ind: dict | None) -> dict:
    ind = ind or {}
    price = safe_float(ind.get("price"), None)
    ma20 = safe_float(ind.get("ma20"), None)
    ma50 = safe_float(ind.get("ma50"), None)
    ma200 = safe_float(ind.get("ma200"), None)
    momentum_5d = ind.get("momentum_5d")
    momentum_20d = ind.get("momentum_20d")
    trend = ind.get("trend") or "Unknown"

    score = 0
    if _ma_ok(price, ma20):
        score += 1
    if _ma_ok(price, ma50):
        score += 1
    if _ma_ok(price, ma200):
        score += 2
    if safe_float(momentum_5d) > 0:
        score += 1
    if safe_float(momentum_20d) > 0:
        score += 1

    return {
        "price": price,
        "ma20": ma20,
        "ma50": ma50,
        "ma200": ma200,
        "trend": trend,
        "momentum_5d": momentum_5d,
        "momentum_20d": momentum_20d,
        "momentum_60d": ind.get("momentum_60d"),
        "atr_percent": ind.get("atr_percent"),
        "score": score,
        "above_ma20": _ma_ok(price, ma20),
        "above_ma50": _ma_ok(price, ma50),
        "above_ma200": _ma_ok(price, ma200),
    }


def _drawdown_from_recent_high(raw: dict | None, lookback: int = 20) -> float:
    if not raw:
        return 0.0
    closes = raw.get("closes") or []
    if len(closes) < 2:
        return 0.0
    recent = [safe_float(value) for value in closes[-lookback:] if safe_float(value) > 0]
    if not recent:
        return 0.0
    high = max(recent)
    current = recent[-1]
    if high <= 0:
        return 0.0
    return round(((current - high) / high) * 100, 4)


def _gap_instability(raw_by_symbol: dict[str, dict]) -> float:
    gaps: list[float] = []
    for raw in raw_by_symbol.values():
        opens = raw.get("opens") or []
        closes = raw.get("closes") or []
        if len(opens) >= 1 and len(closes) >= 2:
            gap = pct_change(safe_float(opens[-1]), safe_float(closes[-2]))
            if gap is not None:
                gaps.append(abs(gap))
    if not gaps:
        return 0.0
    return round(sum(gaps) / len(gaps), 4)


def _breadth(candidates: list[dict]) -> dict:
    advancing = 0
    declining = 0
    neutral = 0

    for row in candidates or []:
        if row.get("error") or row.get("signal") == "ERROR":
            continue

        momentum = row.get("momentum_5d")
        price = row.get("price")
        ma20 = row.get("ma20")
        trend = str(row.get("trend") or "").upper()

        if safe_float(momentum) > 0 or _ma_ok(safe_float(price, None), safe_float(ma20, None)) or "BULL" in trend:
            advancing += 1
        elif safe_float(momentum) < 0 or "BEAR" in trend:
            declining += 1
        else:
            neutral += 1

    total = advancing + declining + neutral
    ratio = round((advancing / total) * 100, 4) if total else 50.0

    return {
        "advancing": advancing,
        "declining": declining,
        "neutral": neutral,
        "total": total,
        "advancing_percent": ratio,
    }


def _relative_volume(candidates: list[dict]) -> dict:
    values = [safe_float(row.get("volume_ratio")) for row in candidates or [] if row.get("volume_ratio") is not None]
    if not values:
        return {"average_relative_volume": 1.0, "symbols_count": 0, "state": "UNKNOWN"}
    avg = round(sum(values) / len(values), 4)
    if avg >= 1.25:
        state = "EXPANDING"
    elif avg <= 0.75:
        state = "THIN"
    else:
        state = "NORMAL"
    return {"average_relative_volume": avg, "symbols_count": len(values), "state": state}


def _momentum_quality(candidates: list[dict]) -> dict:
    usable = [row for row in candidates or [] if not row.get("error")]
    if not usable:
        return {"quality_percent": 50.0, "state": "UNKNOWN", "positive_count": 0, "total": 0}
    positive = [
        row for row in usable
        if safe_float(row.get("momentum_5d")) > 0 and "BEAR" not in str(row.get("trend") or "").upper()
    ]
    quality = round((len(positive) / len(usable)) * 100, 4)
    if quality >= 60:
        state = "HEALTHY"
    elif quality >= 40:
        state = "MIXED"
    else:
        state = "POOR"
    return {"quality_percent": quality, "state": state, "positive_count": len(positive), "total": len(usable)}


def _risk_concentration(positions: list[dict]) -> dict:
    account_equity = safe_float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0), 5000.0) or 1.0
    exposures: list[tuple[str, float]] = []
    for position in positions or []:
        symbol = str(position.get("symbol") or "").strip().upper()
        quantity = safe_float(position.get("quantity"))
        price = safe_float(position.get("current_price"), safe_float(position.get("buy_price")))
        value = quantity * price
        if symbol and value > 0:
            exposures.append((symbol, value))

    total = sum(value for _, value in exposures)
    largest_symbol = max(exposures, key=lambda item: item[1], default=(None, 0.0))
    largest_percent = round((largest_symbol[1] / account_equity) * 100, 4) if account_equity else 0.0
    total_percent = round((total / account_equity) * 100, 4) if account_equity else 0.0

    if largest_percent >= 35 or total_percent >= 90:
        state = "HIGH"
    elif largest_percent >= 25 or total_percent >= 70:
        state = "ELEVATED"
    else:
        state = "NORMAL"

    sector_summary = sector_intelligence.build_portfolio_summary(positions or [], account_equity)

    return {
        "state": state,
        "total_exposure_percent": total_percent,
        "largest_symbol": largest_symbol[0],
        "largest_symbol_exposure_percent": largest_percent,
        "positions_count": len(exposures),
        "diversification_score": sector_summary["diversification_score"],
        "top_correlated_groups": sector_summary["top_correlated_groups"],
        "sector_concentration_percent": sector_summary["concentration_percent"],
    }


def _fetch_market_indicators(fetcher: Callable[[str], dict | None] = fetch_stock_data) -> tuple[dict[str, dict], dict[str, dict]]:
    raw_by_symbol: dict[str, dict] = {}
    indicators_by_symbol: dict[str, dict] = {}

    for symbol in ("SPY", "QQQ", "VIX"):
        raw = fetcher(symbol)
        if raw is None and symbol == "VIX":
            raw = fetcher("VXX")
        if raw is None:
            indicators_by_symbol[symbol] = {"symbol": symbol, "error": "No market data"}
            continue
        raw_by_symbol[symbol] = raw
        indicators = compute_indicators(raw)
        indicators["symbol"] = symbol
        indicators_by_symbol[symbol] = indicators

    return raw_by_symbol, indicators_by_symbol


def _recommendations(regime: MarketRegimeState, buy_blocked: bool) -> RegimeRecommendation:
    table = {
        MarketRegimeState.STRONG_BULL: RegimeRecommendation(100.0, 1.0, True, True, False, True),
        MarketRegimeState.BULL: RegimeRecommendation(85.0, 0.75, True, True, False, True),
        MarketRegimeState.NEUTRAL: RegimeRecommendation(60.0, 0.5, True, False, False, True),
        MarketRegimeState.DEFENSIVE: RegimeRecommendation(35.0, 0.25, True, False, False, False),
        MarketRegimeState.RISK_OFF: RegimeRecommendation(10.0, 0.1, True, False, False, False),
        MarketRegimeState.CRASH_PROTECTION: RegimeRecommendation(0.0, 0.0, False, False, False, False),
    }
    rec = table[regime]
    if buy_blocked:
        return RegimeRecommendation(
            rec.recommended_max_exposure,
            0.0,
            False,
            False,
            False,
            False,
        )
    return rec


def evaluate_market_regime(
    indicators_by_symbol: dict[str, dict] | None = None,
    raw_by_symbol: dict[str, dict] | None = None,
    candidates: list[dict] | None = None,
    positions: list[dict] | None = None,
    checked_at: str | None = None,
) -> dict:
    indicators_by_symbol = indicators_by_symbol or {}
    raw_by_symbol = raw_by_symbol or {}
    candidates = candidates or []
    positions = positions or []

    thresholds = {
        "vix_warning": configured_threshold("REGIME_VIX_WARNING", 25.0),
        "vix_danger": configured_threshold("REGIME_VIX_DANGER", 35.0),
        "drawdown_warning": configured_threshold("REGIME_DRAWDOWN_WARNING", 5.0),
        "drawdown_block": configured_threshold("REGIME_DRAWDOWN_BLOCK", 10.0),
        "breadth_warning": configured_threshold("REGIME_BREADTH_WARNING", 45.0),
        "breadth_danger": configured_threshold("REGIME_BREADTH_DANGER", 35.0),
    }

    spy = _trend_summary(indicators_by_symbol.get("SPY"))
    qqq = _trend_summary(indicators_by_symbol.get("QQQ"))
    vix_level = safe_float((indicators_by_symbol.get("VIX") or {}).get("price"))
    if vix_level <= 0:
        vix_level = safe_float((indicators_by_symbol.get("VXX") or {}).get("price"))

    spy_drawdown = _drawdown_from_recent_high(raw_by_symbol.get("SPY"))
    qqq_drawdown = _drawdown_from_recent_high(raw_by_symbol.get("QQQ"))
    breadth = _breadth(candidates)
    rel_volume = _relative_volume(candidates)
    volatility_values = [safe_float(spy.get("atr_percent")), safe_float(qqq.get("atr_percent"))]
    market_volatility = round(sum(volatility_values) / len(volatility_values), 4) if volatility_values else 0.0
    gap_instability = _gap_instability({k: v for k, v in raw_by_symbol.items() if k in ("SPY", "QQQ")})
    momentum_quality = _momentum_quality(candidates)
    concentration = _risk_concentration(positions)

    warnings: list[str] = []
    dangers: list[str] = []
    block_reasons: list[str] = []

    if vix_level >= thresholds["vix_warning"]:
        warnings.append(f"VIX elevated at {round(vix_level, 2)}")
    if vix_level >= thresholds["vix_danger"]:
        dangers.append(f"VIX danger threshold reached at {round(vix_level, 2)}")
        block_reasons.append("Extreme VIX")

    if spy_drawdown <= -thresholds["drawdown_warning"] or qqq_drawdown <= -thresholds["drawdown_warning"]:
        warnings.append("Index drawdown warning threshold reached")
    if spy_drawdown <= -thresholds["drawdown_block"] and qqq_drawdown <= -thresholds["drawdown_block"]:
        dangers.append("SPY and QQQ severe drawdown threshold reached")

    if breadth["advancing_percent"] <= thresholds["breadth_warning"]:
        warnings.append("Market breadth is weakening")
    if breadth["advancing_percent"] <= thresholds["breadth_danger"]:
        dangers.append("Market breadth danger threshold reached")

    spy_weak = not spy["above_ma50"] or not spy["above_ma200"]
    qqq_weak = not qqq["above_ma50"] or not qqq["above_ma200"]
    severe_deterioration = (
        spy_weak
        and qqq_weak
        and breadth["advancing_percent"] <= thresholds["breadth_danger"]
        and (spy_drawdown <= -thresholds["drawdown_block"] or qqq_drawdown <= -thresholds["drawdown_block"])
    )
    if severe_deterioration:
        block_reasons.append("Severe market deterioration")

    crash_conditions = [
        vix_level >= thresholds["vix_danger"] * 1.5 if thresholds["vix_danger"] > 0 else False,
        severe_deterioration,
        spy_drawdown <= -(thresholds["drawdown_block"] * 1.5) and qqq_drawdown <= -(thresholds["drawdown_block"] * 1.5),
    ]

    if any(crash_conditions):
        regime = MarketRegimeState.CRASH_PROTECTION
    elif vix_level >= thresholds["vix_danger"] or severe_deterioration or (spy_weak and qqq_weak):
        regime = MarketRegimeState.RISK_OFF
    elif warnings or dangers or spy_weak or qqq_weak or momentum_quality["state"] == "POOR":
        regime = MarketRegimeState.DEFENSIVE
    elif spy["score"] >= 5 and qqq["score"] >= 5 and breadth["advancing_percent"] >= 60 and vix_level < thresholds["vix_warning"]:
        regime = MarketRegimeState.STRONG_BULL
    elif spy["score"] >= 4 and qqq["score"] >= 4 and breadth["advancing_percent"] >= thresholds["breadth_warning"]:
        regime = MarketRegimeState.BULL
    else:
        regime = MarketRegimeState.NEUTRAL

    buy_blocked = regime == MarketRegimeState.CRASH_PROTECTION or bool(block_reasons)
    recommendation = _recommendations(regime, buy_blocked).as_dict()

    if not block_reasons and regime == MarketRegimeState.CRASH_PROTECTION:
        block_reasons.append("Crash protection")

    risk_level = "LOW"
    if regime in (MarketRegimeState.DEFENSIVE,):
        risk_level = "ELEVATED"
    elif regime in (MarketRegimeState.RISK_OFF,):
        risk_level = "HIGH"
    elif regime == MarketRegimeState.CRASH_PROTECTION:
        risk_level = "CRITICAL"
    elif warnings:
        risk_level = "WATCH"

    vix_state = "UNKNOWN"
    if vix_level > 0:
        if vix_level >= thresholds["vix_danger"]:
            vix_state = "DANGER"
        elif vix_level >= thresholds["vix_warning"]:
            vix_state = "WARNING"
        else:
            vix_state = "NORMAL"

    warning_summary = summarize_reason_list(
        warnings + dangers,
        default_text="No market regime warnings.",
    )
    block_reason_summary = summarize_reason_list(
        block_reasons,
        default_text="No market-regime BUY block active.",
    )

    return {
        "checked_at": checked_at or now_iso(),
        "regime": regime.value,
        "risk_level": risk_level,
        "vix_state": vix_state,
        "vix_level": round(vix_level, 4) if vix_level else None,
        "allow_new_buys": recommendation["allow_new_buys"],
        "allow_aggressive_entries": recommendation["allow_aggressive_entries"],
        "allow_averaging_down": recommendation["allow_averaging_down"],
        "allow_breakout_entries": recommendation["allow_breakout_entries"],
        "recommended_max_exposure": recommendation["recommended_max_exposure"],
        "recommended_position_size": recommendation["recommended_position_size"],
        "position_size_factor": recommendation["recommended_position_size"],
        "min_score_override": 80,
        "buy_blocked": not recommendation["allow_new_buys"],
        "buy_block_reasons": block_reasons,
        "block_reason_summary": block_reason_summary,
        "warning_reason_summary": warning_summary,
        "raw_buy_block_reasons": block_reasons,
        "raw_reasons": block_reasons + warnings + dangers,
        "full_details": {"buy_block_reasons": block_reasons, "warnings": warnings, "dangers": dangers},
        "warnings": warnings,
        "dangers": dangers,
        "metrics": {
            "spy_trend": spy,
            "qqq_trend": qqq,
            "spy_drawdown_percent": spy_drawdown,
            "qqq_drawdown_percent": qqq_drawdown,
            "breadth": breadth,
            "relative_volume_environment": rel_volume,
            "market_volatility_percent": market_volatility,
            "gap_instability_percent": gap_instability,
            "intraday_momentum_quality": momentum_quality,
            "risk_concentration": concentration,
        },
        "thresholds": thresholds,
        "details": {
            "SPY": spy,
            "QQQ": qqq,
            "VIX": indicators_by_symbol.get("VIX") or indicators_by_symbol.get("VXX") or {"error": "No market data"},
        },
    }


def get_market_regime(fetcher: Callable[[str], dict | None] = fetch_stock_data) -> dict:
    raw_by_symbol, indicators_by_symbol = _fetch_market_indicators(fetcher)
    return evaluate_market_regime(
        indicators_by_symbol=indicators_by_symbol,
        raw_by_symbol=raw_by_symbol,
        candidates=[],
        positions=[],
    )


async def _load_history() -> list[dict]:
    raw = await database.get_app_state(HISTORY_KEY, "[]")
    try:
        history = json.loads(raw or "[]")
        return history if isinstance(history, list) else []
    except Exception:
        return []


async def get_market_regime_history(limit: int = 100) -> list[dict]:
    limit = max(1, min(int(limit or 100), MAX_HISTORY))
    history = await _load_history()
    return history[-limit:][::-1]


async def _append_history(snapshot: dict) -> None:
    history = await _load_history()
    history.append(snapshot)
    history = history[-MAX_HISTORY:]
    await database.set_app_state(HISTORY_KEY, json.dumps(history, ensure_ascii=False, default=str))


async def refresh_market_regime(candidates: list[dict] | None = None, positions: list[dict] | None = None) -> dict:
    raw_by_symbol, indicators_by_symbol = _fetch_market_indicators()
    snapshot = evaluate_market_regime(
        indicators_by_symbol=indicators_by_symbol,
        raw_by_symbol=raw_by_symbol,
        candidates=candidates or [],
        positions=positions or [],
    )

    previous_state = await database.get_app_state(STATE_KEY)
    previous_latest_raw = await database.get_app_state(LATEST_KEY)
    previous_latest = {}
    if previous_latest_raw:
        try:
            previous_latest = json.loads(previous_latest_raw)
        except Exception:
            previous_latest = {}

    await database.set_app_state(STATE_KEY, snapshot["regime"])
    await database.set_app_state(LATEST_KEY, json.dumps(snapshot, ensure_ascii=False, default=str))
    await _append_history(snapshot)
    if not snapshot.get("error"):
        await watchdog.refresh_market_data_timestamp(
            "market_snapshot_update",
            metadata={
                "regime": snapshot.get("regime"),
                "risk_level": snapshot.get("risk_level"),
                "candidate_count": len(candidates or []),
                "position_count": len(positions or []),
            },
        )

    if previous_state and previous_state != snapshot["regime"]:
        await database.safe_record_trade_journal_event({
            "event_type": "MARKET_REGIME_CHANGED",
            "decision": snapshot["regime"],
            "reason": f"Market regime changed from {previous_state} to {snapshot['regime']}",
            "source_module": "market_regime_engine.refresh_market_regime",
            "market_regime": snapshot["regime"],
            "raw_payload": snapshot,
        })

    previous_risk = previous_latest.get("risk_level")
    if snapshot["risk_level"] in ("ELEVATED", "HIGH", "CRITICAL") and previous_risk != snapshot["risk_level"]:
        await database.safe_record_trade_journal_event({
            "event_type": "MARKET_RISK_WARNING",
            "decision": snapshot["risk_level"],
            "reason": " · ".join(snapshot.get("warnings") or snapshot.get("dangers") or ["Market risk elevated"]),
            "source_module": "market_regime_engine.refresh_market_regime",
            "market_regime": snapshot["regime"],
            "raw_payload": snapshot,
        })

    if snapshot["buy_blocked"] and not previous_latest.get("buy_blocked"):
        await database.safe_record_trade_journal_event({
            "event_type": "MARKET_BUY_BLOCKED",
            "decision": "BLOCK_NEW_BUYS",
            "reason": " · ".join(snapshot.get("buy_block_reasons") or ["Market regime blocks new buys"]),
            "source_module": "market_regime_engine.refresh_market_regime",
            "market_regime": snapshot["regime"],
            "raw_payload": snapshot,
        })

    if previous_latest and previous_latest.get("risk_level") in ("ELEVATED", "HIGH", "CRITICAL") and snapshot["risk_level"] in ("LOW", "WATCH"):
        await database.safe_record_trade_journal_event({
            "event_type": "MARKET_RISK_RECOVERED",
            "decision": snapshot["risk_level"],
            "reason": f"Market risk recovered to {snapshot['risk_level']}",
            "source_module": "market_regime_engine.refresh_market_regime",
            "market_regime": snapshot["regime"],
            "raw_payload": snapshot,
        })

    return snapshot


async def get_cached_market_regime() -> dict:
    raw = await database.get_app_state(LATEST_KEY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return get_market_regime()

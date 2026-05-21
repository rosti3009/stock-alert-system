from __future__ import annotations

from datetime import datetime, time, timezone
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

import config
import database
from execution_quality import evaluate_execution_quality


class StrategyMode(StrEnum):
    SWING_DEFAULT = "SWING_DEFAULT"
    INTRADAY_TECHNICAL = "INTRADAY_TECHNICAL"
    INTRADAY_MOMENTUM = "INTRADAY_MOMENTUM"


STRATEGY_MODE_KEY = "strategy_mode"
INTRADAY_ENRICHMENT_KEYS = (
    "news_catalyst",
    "earnings_calendar",
    "sector_strength",
    "index_trend",
)


def normalize_strategy_mode(value: Any) -> StrategyMode:
    normalized = str(value or "").strip().upper()
    if normalized in {StrategyMode.INTRADAY_TECHNICAL.value, StrategyMode.INTRADAY_MOMENTUM.value}:
        return StrategyMode.INTRADAY_MOMENTUM
    return StrategyMode.SWING_DEFAULT


async def get_strategy_mode() -> StrategyMode:
    raw = await database.get_app_state(STRATEGY_MODE_KEY, getattr(config, "STRATEGY_MODE_DEFAULT", StrategyMode.SWING_DEFAULT.value))
    return normalize_strategy_mode(raw)


async def set_strategy_mode(mode: StrategyMode | str) -> dict[str, Any]:
    normalized = normalize_strategy_mode(mode)
    previous = await get_strategy_mode()
    await database.set_app_state(STRATEGY_MODE_KEY, normalized.value)
    open_positions = await database.get_open_positions()
    return strategy_mode_payload(
        normalized,
        previous_mode=previous,
        open_positions_count=len(open_positions),
    )


def is_intraday_mode(mode: StrategyMode | str | None) -> bool:
    return normalize_strategy_mode(mode) == StrategyMode.INTRADAY_MOMENTUM


def intraday_rules() -> dict[str, Any]:
    rules = {
        "min_score_to_buy": int(getattr(config, "INTRADAY_MIN_SCORE_TO_BUY", 85)),
        "max_open_positions": int(getattr(config, "INTRADAY_MAX_OPEN_POSITIONS", 3)),
        "position_size_factor": float(getattr(config, "INTRADAY_POSITION_SIZE_FACTOR", 0.25)),
        "min_relative_volume": float(getattr(config, "INTRADAY_MIN_RELATIVE_VOLUME", 1.5)),
        "min_dollar_volume": float(getattr(config, "INTRADAY_MIN_DOLLAR_VOLUME", getattr(config, "MIN_DOLLAR_VOLUME", 5000000.0))),
        "max_spread_percent": float(getattr(config, "INTRADAY_MAX_SPREAD_PERCENT", getattr(config, "MAX_SPREAD_PERCENT", 3.0))),
        "max_slippage_estimate": float(getattr(config, "INTRADAY_MAX_SLIPPAGE_ESTIMATE", getattr(config, "MAX_SLIPPAGE_ESTIMATE", 2.0))),
        "max_daily_trades": int(getattr(config, "INTRADAY_MAX_DAILY_TRADES", 5)),
        "max_consecutive_losses": int(getattr(config, "INTRADAY_MAX_CONSECUTIVE_LOSSES", 2)),
        "max_daily_loss_percent": float(getattr(config, "INTRADAY_MAX_DAILY_LOSS_PERCENT", 2.0)),
        "force_exit_minutes_before_close": int(getattr(config, "INTRADAY_FORCE_EXIT_MINUTES_BEFORE_CLOSE", 15)),
        "allow_overnight": bool(getattr(config, "INTRADAY_ALLOW_OVERNIGHT", False)),
        "require_vwap_when_available": True,
        "required_timeframes": ["1m", "5m", "15m"],
        "max_open_intraday_positions": int(getattr(config, "INTRADAY_MAX_OPEN_POSITIONS", 3)),
    }
    profile_rules = config.active_paper_training_profile_rules()
    rules.update(profile_rules.get("intraday") or {})
    rules["training_profile"] = profile_rules["profile"]
    rules["hard_protections_kept"] = profile_rules["hard_protections_kept"]
    rules["allow_overnight"] = False
    rules["profile_name"] = rules.get("training_profile")
    return rules


def swing_rules() -> dict[str, Any]:
    return {
        "min_score_to_buy": int(getattr(config, "MIN_SCORE_TO_BUY", 80)),
        "max_open_positions": int(getattr(config, "MAX_OPEN_POSITIONS", 10)),
        "position_size_factor": 1.0,
    }


def active_rules(mode: StrategyMode | str | None) -> dict[str, Any]:
    return intraday_rules() if is_intraday_mode(mode) else swing_rules()


def has_intraday_bars(row: dict[str, Any]) -> bool:
    if row.get("intraday_bars_available") is True:
        return True
    bars = row.get("intraday_bars") or {}
    if isinstance(bars, dict):
        return any(bars.get(tf) for tf in ("1m", "5m", "15m"))
    return bool(bars)


def relative_volume(row: dict[str, Any]) -> float | None:
    for key in ("relative_volume", "volume_ratio", "intraday_relative_volume"):
        value = row.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    try:
        avg_volume = float(row.get("avg_volume") or row.get("average_volume") or 0)
        volume = float(row.get("volume") or row.get("current_volume") or 0)
    except (TypeError, ValueError):
        return None
    if avg_volume > 0 and volume > 0:
        return volume / avg_volume
    return None


def dollar_volume(row: dict[str, Any]) -> float:
    explicit = row.get("dollar_volume") or row.get("intraday_dollar_volume")
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            return 0.0
    try:
        price = float(row.get("price") or row.get("current_price") or row.get("entry_price") or 0)
        volume = float(row.get("volume") or row.get("current_volume") or row.get("avg_volume") or 0)
        return price * volume
    except (TypeError, ValueError):
        return 0.0


def calculate_intraday_technical_score(row: dict[str, Any]) -> tuple[int, list[str]]:
    reasons: list[str] = []
    if row.get("error"):
        return 0, ["Data error"]
    if not has_intraday_bars(row):
        return 0, ["Intraday bars unavailable"]

    score = int(row.get("intraday_technical_score") or row.get("intraday_score") or 0)
    if score > 0:
        reasons.append("Provided intraday technical score")
    else:
        price = _float(row.get("price") or row.get("current_price"))
        vwap = _float(row.get("vwap") or row.get("intraday_vwap"))
        rv = relative_volume(row) or 0.0
        trend = str(row.get("trend") or row.get("intraday_trend") or "").lower()
        setup = str(row.get("setup") or row.get("intraday_setup") or "").lower()
        momentum = _float(row.get("momentum_percent") or row.get("change_percent") or row.get("rsi"))

        if price and vwap and price >= vwap:
            score += 20
            reasons.append("Price confirmed above VWAP")
        if rv >= 2.0:
            score += 25
            reasons.append("Relative volume above 2.0x")
        elif rv >= intraday_rules()["min_relative_volume"]:
            score += 18
            reasons.append("Relative volume above intraday minimum")
        if any(token in setup for token in ("breakout", "momentum", "opening range")):
            score += 25
            reasons.append("Breakout/momentum setup")
        if "strong" in trend or "bullish" in trend:
            score += 20
            reasons.append("Strong intraday trend")
        if momentum and momentum > 0:
            score += 10
            reasons.append("Positive intraday momentum")

    if _is_weak_trend(row):
        score = min(score, 74)
        reasons.append("Weak trend setup avoided")

    return max(0, min(100, score)), reasons or ["Intraday technical score calculated"]


def validate_intraday_buy(row: dict[str, Any]) -> dict[str, Any]:
    rules = intraday_rules()
    reasons: list[str] = []

    if not has_intraday_bars(row):
        reasons.append("Intraday BUY blocked: fresh 1m/5m/15m bars are unavailable")
    if row.get("intraday_entry_allowed") is False:
        reasons.append("intraday_entry_allowed=false")

    score, score_reasons = calculate_intraday_technical_score(row)
    if score < rules["min_score_to_buy"]:
        reasons.append(f"Intraday score too low ({score} < {rules['min_score_to_buy']})")

    price = _float(row.get("price") or row.get("current_price") or row.get("entry_price"))
    vwap = _float(row.get("vwap") or row.get("intraday_vwap"))
    if vwap and price and price < vwap:
        reasons.append("VWAP confirmation failed")

    rv = relative_volume(row)
    if rv is None or rv < rules["min_relative_volume"]:
        rv_text = "missing" if rv is None else f"{rv:.2f}x"
        reasons.append(f"Relative volume below intraday minimum ({rv_text} < {rules['min_relative_volume']:.2f}x)")

    dv = dollar_volume(row)
    if dv < rules["min_dollar_volume"]:
        reasons.append(f"Dollar volume below intraday minimum (${dv:,.0f} < ${rules['min_dollar_volume']:,.0f})")

    execution_quality = evaluate_execution_quality(row=row, limit_price=price, symbol=row.get("symbol"))
    metrics = execution_quality.get("metrics") or {}
    spread = metrics.get("spread_percent")
    slippage = metrics.get("estimated_slippage_percent")
    if spread is not None and float(spread) > rules["max_spread_percent"]:
        reasons.append(f"Spread too wide for intraday ({float(spread):.2f}% > {rules['max_spread_percent']:.2f}%)")
    if slippage is not None and float(slippage) > rules["max_slippage_estimate"]:
        reasons.append(f"Slippage estimate too high for intraday ({float(slippage):.2f}% > {rules['max_slippage_estimate']:.2f}%)")
    if execution_quality.get("blocks_buy"):
        reasons.append(execution_quality.get("blocked_buy_reason") or "Execution quality blocked intraday BUY")

    setup = str(row.get("setup") or row.get("intraday_setup") or "").lower()
    if not any(token in setup for token in ("breakout", "momentum", "opening range")):
        reasons.append("Intraday setup is not breakout/momentum preferred")
    if _is_weak_trend(row):
        reasons.append("Weak trend setup blocked for intraday")

    daily_trades = _float(row.get("intraday_daily_trades"))
    if daily_trades is not None and daily_trades >= rules["max_daily_trades"]:
        reasons.append(f"Max intraday daily trades reached ({int(daily_trades)}/{rules['max_daily_trades']})")

    consecutive_losses = _float(row.get("intraday_consecutive_losses"))
    if consecutive_losses is not None and consecutive_losses >= rules["max_consecutive_losses"]:
        reasons.append(f"Max consecutive intraday losses reached ({int(consecutive_losses)}/{rules['max_consecutive_losses']})")

    daily_loss_percent = _float(row.get("intraday_daily_loss_percent"))
    if daily_loss_percent is not None and daily_loss_percent >= rules["max_daily_loss_percent"]:
        reasons.append(f"Max intraday daily loss reached ({daily_loss_percent:.2f}% >= {rules['max_daily_loss_percent']:.2f}%)")

    allowed = not reasons
    return {
        "allowed": allowed,
        "reasons": reasons,
        "score": score,
        "score_reasons": score_reasons,
        "execution_quality": execution_quality,
        "rules": rules,
        "enrichment_status": intraday_enrichment_status(row),
    }


def intraday_enrichment_status(row: dict[str, Any] | None = None) -> dict[str, Any]:
    row = row or {}
    available = {}
    for key in INTRADAY_ENRICHMENT_KEYS:
        available[key] = row.get(key) is not None or row.get(f"{key}_available") is True
    missing = [key for key, ok in available.items() if not ok]
    return {
        "technical_only_allowed": True,
        "enrichment_missing": bool(missing),
        "missing": missing,
        "available": available,
    }


def force_exit_before_close_status(now: datetime | None = None) -> dict[str, Any]:
    rules = intraday_rules()
    eastern = ZoneInfo("America/New_York")
    current = (now or datetime.now(timezone.utc)).astimezone(eastern)
    close_dt = datetime.combine(current.date(), time(16, 0), tzinfo=eastern)
    minutes_to_close = (close_dt - current).total_seconds() / 60
    active = 0 <= minutes_to_close <= rules["force_exit_minutes_before_close"]
    return {
        "enabled": not rules["allow_overnight"],
        "active": bool(active and not rules["allow_overnight"]),
        "minutes_before_close": rules["force_exit_minutes_before_close"],
        "minutes_to_close": round(minutes_to_close, 2),
        "allow_overnight": rules["allow_overnight"],
    }


def strategy_mode_payload(
    mode: StrategyMode | str,
    *,
    previous_mode: StrategyMode | str | None = None,
    open_positions_count: int = 0,
) -> dict[str, Any]:
    normalized = normalize_strategy_mode(mode)
    rules = active_rules(normalized)
    return {
        "strategy_mode": normalized.value,
        "previous_strategy_mode": normalize_strategy_mode(previous_mode).value if previous_mode else None,
        "active_buy_engine": "intraday_momentum_engine" if is_intraday_mode(normalized) else "swing_default",
        "active_sell_engine": "intraday_exit_engine" if is_intraday_mode(normalized) else "swing_position_manager",
        "active_risk_profile": (
            f"intraday_{config.active_paper_training_profile().lower()}"
            if is_intraday_mode(normalized) else "swing_default"
        ),
        "active_training_profile": config.active_paper_training_profile(),
        "profile_rules": config.active_paper_training_profile_rules(),
        "effective_max_positions": int(rules.get("max_open_positions", 0)),
        "effective_score_threshold": int(rules.get("min_score_to_buy", 0)),
        "effective_risk_factor": float(rules.get("position_size_factor", 1.0)),
        "effective_max_daily_trades": int(rules.get("max_daily_trades", 0)) if "max_daily_trades" in rules else None,
        "rules": rules,
        "intraday_rules": intraday_rules(),
        "intraday_enrichment_status": intraday_enrichment_status(),
        "force_exit_before_close": force_exit_before_close_status(),
        "open_positions": int(open_positions_count),
        "switch_warning": (
            "Open positions exist; switching strategy mode will not close, reset, or delete them."
            if open_positions_count else None
        ),
        "force_close_existing": False,
    }


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_weak_trend(row: dict[str, Any]) -> bool:
    trend = str(row.get("trend") or row.get("intraday_trend") or "").strip().lower()
    setup = str(row.get("setup") or row.get("intraday_setup") or "").strip().lower()
    return "weak" in trend or "weak" in setup or trend in {"neutral", "bearish", "sideways"}

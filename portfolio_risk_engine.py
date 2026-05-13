from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, time, timezone
from enum import Enum
from typing import Any

import aiosqlite

import account_sync
import config
import database
import sector_intelligence
from reason_summarizer import summarize_reason_list


class RiskState(str, Enum):
    SAFE = "SAFE"
    WARNING = "WARNING"
    DANGER = "DANGER"
    BLOCK_NEW_BUYS = "BLOCK_NEW_BUYS"


STATE_KEY = "portfolio_risk_state"
LATEST_KEY = "portfolio_risk_latest"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 4)


def configured_threshold(name: str, default: float) -> float:
    return safe_float(getattr(config, name, default), default)


def get_sector(symbol: str) -> str:
    return str(sector_intelligence.classify_symbol(symbol).get("sector") or sector_intelligence.UNKNOWN)


def account_value(summary: list[dict], tag: str) -> float:
    for row in summary:
        if str(row.get("tag") or "").lower() == tag.lower():
            return safe_float(row.get("value"))
    return 0.0


def effective_account_equity() -> float:
    return max(0.0, safe_float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0), 5000.0))


def infer_account_equity(summary: list[dict], realized_pnl: float = 0.0) -> float:
    return effective_account_equity()


def broker_account_values(summary: list[dict]) -> dict[str, float]:
    return {
        "broker_net_liquidation": round(account_value(summary, "NetLiquidation"), 2),
        "broker_cash": round(account_value(summary, "TotalCashValue"), 2),
        "broker_buying_power": round(account_value(summary, "BuyingPower"), 2),
    }


@dataclass(frozen=True)
class PositionExposure:
    symbol: str
    sector: str
    quantity: float
    buy_price: float
    current_price: float
    market_value: float
    cost_basis: float
    unrealized_pnl: float
    exposure_percent: float
    open_risk: float

    def as_dict(self) -> dict:
        classification = sector_intelligence.classify_symbol(self.symbol)
        return {
            "symbol": self.symbol,
            "sector": self.sector,
            "industry": classification.get("industry"),
            "subsector": classification.get("subsector"),
            "theme": classification.get("theme"),
            "volatility_group": classification.get("volatility_group"),
            "correlation_cluster": classification.get("correlation_cluster"),
            "classification_source": classification.get("classification_source") or classification.get("source"),
            "confidence": classification.get("confidence"),
            "normalized_sector": classification.get("normalized_sector") or classification.get("sector"),
            "quantity": round(self.quantity, 6),
            "buy_price": round(self.buy_price, 4),
            "current_price": round(self.current_price, 4),
            "market_value": round(self.market_value, 2),
            "cost_basis": round(self.cost_basis, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "exposure_percent": round(self.exposure_percent, 4),
            "open_risk": round(self.open_risk, 2),
        }


def build_position_exposures(positions: list[dict], account_equity: float) -> list[PositionExposure]:
    exposures: list[PositionExposure] = []

    for position in positions:
        symbol = str(position.get("symbol") or "").strip().upper()
        quantity = safe_float(position.get("quantity"))
        buy_price = safe_float(position.get("buy_price"))
        current_price = safe_float(position.get("current_price"), buy_price)
        stop_loss = safe_float(position.get("stop_loss"))

        if not symbol or quantity <= 0 or current_price <= 0:
            continue

        market_value = quantity * current_price
        cost_basis = quantity * buy_price
        unrealized_pnl = market_value - cost_basis
        open_risk = max(0.0, (current_price - stop_loss) * quantity) if stop_loss > 0 else 0.0

        exposures.append(PositionExposure(
            symbol=symbol,
            sector=get_sector(symbol),
            quantity=quantity,
            buy_price=buy_price,
            current_price=current_price,
            market_value=market_value,
            cost_basis=cost_basis,
            unrealized_pnl=unrealized_pnl,
            exposure_percent=pct(market_value, account_equity),
            open_risk=open_risk,
        ))

    return exposures


def aggregate_sector_exposure(position_exposures: list[PositionExposure], account_equity: float) -> list[dict]:
    totals: dict[str, float] = {}
    symbols: dict[str, list[str]] = {}

    for exposure in position_exposures:
        totals[exposure.sector] = totals.get(exposure.sector, 0.0) + exposure.market_value
        symbols.setdefault(exposure.sector, []).append(exposure.symbol)

    return [
        {
            "sector": sector,
            "market_value": round(value, 2),
            "exposure_percent": pct(value, account_equity),
            "symbols": sorted(set(symbols.get(sector, []))),
        }
        for sector, value in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]


def make_alert(severity: str, metric: str, message: str, value: float, threshold: float, blocks_new_buys: bool = False) -> dict:
    return {
        "severity": severity,
        "metric": metric,
        "message": message,
        "value": round(value, 4),
        "threshold": round(threshold, 4),
        "blocks_new_buys": bool(blocks_new_buys),
    }


def evaluate_risk_snapshot(
    positions: list[dict],
    account_summary: list[dict] | None = None,
    daily_realized_pnl: float = 0.0,
    checked_at: str | None = None,
) -> dict:
    account_summary = account_summary or []
    broker_values = broker_account_values(account_summary)
    account_equity = infer_account_equity(account_summary, daily_realized_pnl)
    position_exposures = build_position_exposures(positions, account_equity)
    sector_exposures = aggregate_sector_exposure(position_exposures, account_equity)
    sector_summary = sector_intelligence.build_portfolio_summary(positions, account_equity, checked_at=checked_at)

    total_market_value = sum(item.market_value for item in position_exposures)
    total_unrealized = sum(item.unrealized_pnl for item in position_exposures)
    total_open_risk = sum(item.open_risk for item in position_exposures)
    total_exposure_percent = pct(total_market_value, account_equity)
    unrealized_drawdown_percent = pct(abs(min(0.0, total_unrealized)), account_equity)
    daily_realized_pnl_percent = pct(daily_realized_pnl, account_equity)
    daily_drawdown_percent = abs(min(0.0, daily_realized_pnl_percent))

    largest_position = max(position_exposures, key=lambda item: item.exposure_percent, default=None)
    concentration_sector_candidates = [item for item in sector_summary.get("exposure_by_sector", []) if item.get("sector") != sector_intelligence.UNKNOWN]
    largest_sector = max(concentration_sector_candidates or sector_summary.get("exposure_by_sector", []) or sector_exposures, key=lambda item: item["exposure_percent"], default=None)

    account_utilization_percent = total_exposure_percent

    thresholds = {
        "max_total_exposure_percent": configured_threshold("MAX_TOTAL_EXPOSURE_PERCENT", 80.0),
        "max_symbol_exposure_percent": configured_threshold("MAX_SYMBOL_EXPOSURE_PERCENT", 25.0),
        "max_sector_exposure_percent": configured_threshold("MAX_SECTOR_EXPOSURE_PERCENT", 45.0),
        "max_daily_drawdown_percent": configured_threshold("MAX_DAILY_DRAWDOWN_PERCENT", 5.0),
        "max_account_utilization_percent": configured_threshold("MAX_ACCOUNT_UTILIZATION_PERCENT", 90.0),
    }

    alerts: list[dict] = []
    concentration_warnings: list[dict] = []

    total_threshold = thresholds["max_total_exposure_percent"]
    if total_exposure_percent >= total_threshold:
        alerts.append(make_alert("DANGER", "total_exposure_percent", "Total portfolio exposure exceeds the configured limit.", total_exposure_percent, total_threshold, True))
    elif total_exposure_percent >= total_threshold * 0.8:
        alerts.append(make_alert("WARNING", "total_exposure_percent", "Total portfolio exposure is nearing the configured limit.", total_exposure_percent, total_threshold))

    symbol_threshold = thresholds["max_symbol_exposure_percent"]
    largest_position_percent = largest_position.exposure_percent if largest_position else 0.0
    if largest_position and largest_position_percent >= symbol_threshold:
        alert = make_alert("DANGER", "largest_position_percent", f"{largest_position.symbol} exceeds the per-symbol exposure limit.", largest_position_percent, symbol_threshold, True)
        alerts.append(alert)
        concentration_warnings.append(alert)
    elif largest_position and largest_position_percent >= symbol_threshold * 0.8:
        alert = make_alert("WARNING", "largest_position_percent", f"{largest_position.symbol} is nearing the per-symbol exposure limit.", largest_position_percent, symbol_threshold)
        alerts.append(alert)
        concentration_warnings.append(alert)

    sector_threshold = thresholds["max_sector_exposure_percent"]
    largest_sector_percent = safe_float((largest_sector or {}).get("exposure_percent"))
    suppress_unknown_sector_warning = bool(
        largest_sector
        and largest_sector.get("sector") == sector_intelligence.UNKNOWN
        and safe_float(largest_sector.get("average_confidence")) < 0.5
    )
    if largest_sector and not suppress_unknown_sector_warning and largest_sector_percent >= sector_threshold:
        alert = make_alert("DANGER", "sector_exposure_percent", f"{largest_sector['sector']} sector exposure exceeds the configured limit.", largest_sector_percent, sector_threshold, True)
        alerts.append(alert)
        concentration_warnings.append(alert)
    elif largest_sector and not suppress_unknown_sector_warning and largest_sector_percent >= sector_threshold * 0.8:
        alert = make_alert("WARNING", "sector_exposure_percent", f"{largest_sector['sector']} sector exposure is nearing the configured limit.", largest_sector_percent, sector_threshold)
        alerts.append(alert)
        concentration_warnings.append(alert)

    drawdown_threshold = thresholds["max_daily_drawdown_percent"]
    if daily_drawdown_percent >= drawdown_threshold:
        alerts.append(make_alert("DANGER", "daily_drawdown_percent", "Daily realized drawdown exceeds the configured limit.", daily_drawdown_percent, drawdown_threshold, True))
    elif daily_drawdown_percent >= drawdown_threshold * 0.8:
        alerts.append(make_alert("WARNING", "daily_drawdown_percent", "Daily realized drawdown is nearing the configured limit.", daily_drawdown_percent, drawdown_threshold))

    utilization_threshold = thresholds["max_account_utilization_percent"]
    if account_utilization_percent >= utilization_threshold:
        alerts.append(make_alert("DANGER", "account_utilization_percent", "Account utilization exceeds the configured danger limit.", account_utilization_percent, utilization_threshold))
    elif account_utilization_percent >= utilization_threshold * 0.8:
        alerts.append(make_alert("WARNING", "account_utilization_percent", "Account utilization is nearing the configured warning limit.", account_utilization_percent, utilization_threshold))

    blocks_new_buys = any(alert["blocks_new_buys"] for alert in alerts)
    raw_block_reasons = [alert["message"] for alert in alerts if alert["blocks_new_buys"]]
    alert_messages = [alert["message"] for alert in alerts]
    block_reason_summary = summarize_reason_list(
        raw_block_reasons,
        default_text="No portfolio-risk BUY block active.",
    )
    alert_reason_summary = summarize_reason_list(
        alert_messages,
        default_text="No portfolio-risk alerts active.",
    )
    danger = any(alert["severity"] == "DANGER" for alert in alerts)
    warning = any(alert["severity"] == "WARNING" for alert in alerts)

    if blocks_new_buys:
        risk_state = RiskState.BLOCK_NEW_BUYS.value
    elif danger:
        risk_state = RiskState.DANGER.value
    elif warning:
        risk_state = RiskState.WARNING.value
    else:
        risk_state = RiskState.SAFE.value

    return {
        "checked_at": checked_at or now_iso(),
        "risk_state": risk_state,
        "new_buy_risk_status": "BLOCKED" if blocks_new_buys else "ALLOWED",
        "blocks_new_buys": blocks_new_buys,
        "block_reasons": raw_block_reasons,
        "block_reason_summary": block_reason_summary,
        "alert_reason_summary": alert_reason_summary,
        "raw_block_reasons": raw_block_reasons,
        "raw_reasons": raw_block_reasons,
        "full_details": {"block_reasons": raw_block_reasons, "alerts": alerts},
        "account_equity": round(account_equity, 2),
        "effective_equity": round(account_equity, 2),
        "virtual_trading_capital": round(account_equity, 2),
        "risk_calculation_basis": "virtual_trading_capital",
        **broker_values,
        "total_market_value": round(total_market_value, 2),
        "total_portfolio_exposure_percent": total_exposure_percent,
        "exposure_by_symbol": [item.as_dict() for item in sorted(position_exposures, key=lambda item: item.market_value, reverse=True)],
        "exposure_by_sector": sector_summary.get("visible_sector_exposure") or sector_exposures,
        "all_exposure_by_sector": sector_summary.get("exposure_by_sector") or sector_exposures,
        "top_sectors": sector_summary.get("top_sectors", []),
        "exposure_by_industry": sector_summary["exposure_by_industry"],
        "sector_intelligence": sector_summary,
        "top_correlated_groups": sector_summary["top_correlated_groups"],
        "diversification_score": sector_summary["diversification_score"],
        "diversification_quality": sector_summary.get("diversification_quality"),
        "known_sector_percentage": sector_summary.get("known_sector_percentage"),
        "unknown_sector_percentage": sector_summary.get("unknown_sector_percentage"),
        "concentration_percent": sector_summary["concentration_percent"],
        "largest_position_percent": round(largest_position_percent, 4),
        "largest_position": largest_position.as_dict() if largest_position else None,
        "largest_sector": largest_sector,
        "unrealized_pnl": round(total_unrealized, 2),
        "unrealized_drawdown_percent": unrealized_drawdown_percent,
        "daily_realized_pnl": round(daily_realized_pnl, 2),
        "daily_realized_pnl_percent": daily_realized_pnl_percent,
        "daily_drawdown_percent": round(daily_drawdown_percent, 4),
        "total_open_risk": round(total_open_risk, 2),
        "total_open_risk_percent": pct(total_open_risk, account_equity),
        "account_utilization_percent": account_utilization_percent,
        "concentration_warnings": concentration_warnings,
        "alerts": alerts,
        "thresholds": thresholds,
    }


async def get_daily_realized_pnl() -> float:
    start = datetime.combine(datetime.now(timezone.utc).date(), time.min, tzinfo=timezone.utc).isoformat()
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(account_sync.CREATE_EXECUTION_HISTORY)
        async with db.execute(
            """
            SELECT SUM(realized_pnl)
            FROM execution_history
            WHERE COALESCE(time, created_at) >= ?
            """,
            (start,),
        ) as cursor:
            row = await cursor.fetchone()
    return safe_float(row[0] if row else 0.0)


async def _record_state_events(snapshot: dict) -> None:
    previous_state = await database.get_app_state(STATE_KEY, None)
    current_state = str(snapshot.get("risk_state") or RiskState.SAFE.value)

    if previous_state == current_state:
        return

    await database.set_app_state(STATE_KEY, current_state)

    if current_state == RiskState.SAFE.value and previous_state and previous_state != RiskState.SAFE.value:
        event_type = "RISK_RECOVERED"
        decision = "RECOVERED"
        reason = "Portfolio risk recovered to SAFE."
    elif snapshot.get("blocks_new_buys"):
        event_type = "RISK_BLOCK_BUY"
        decision = "BLOCKED"
        reason = "; ".join(snapshot.get("block_reasons") or []) or "Portfolio risk blocks new buys."
    elif current_state in {RiskState.WARNING.value, RiskState.DANGER.value}:
        event_type = "RISK_WARNING"
        decision = current_state
        reason = "; ".join(alert.get("message", "") for alert in snapshot.get("alerts", [])) or "Portfolio risk warning."
    else:
        return

    await database.safe_record_trade_journal_event({
        "event_type": event_type,
        "decision": decision,
        "reason": reason,
        "source_module": "portfolio_risk_engine",
        "risk_percent": snapshot.get("total_portfolio_exposure_percent"),
        "realized_pnl": snapshot.get("daily_realized_pnl"),
        "unrealized_pnl": snapshot.get("unrealized_pnl"),
        "raw_payload": snapshot,
    })


async def refresh_portfolio_risk() -> dict:
    positions = await database.get_open_positions()
    account_summary = await account_sync.get_account_summary()
    daily_realized_pnl = await get_daily_realized_pnl()
    snapshot = evaluate_risk_snapshot(positions, account_summary, daily_realized_pnl)
    await database.set_app_state(LATEST_KEY, json.dumps(snapshot, ensure_ascii=False, default=str))
    await _record_state_events(snapshot)
    return snapshot


async def get_portfolio_risk() -> dict:
    return await refresh_portfolio_risk()


async def get_exposure() -> dict:
    snapshot = await get_portfolio_risk()
    return {
        "checked_at": snapshot["checked_at"],
        "account_equity": snapshot["account_equity"],
        "effective_equity": snapshot["effective_equity"],
        "virtual_trading_capital": snapshot["virtual_trading_capital"],
        "broker_net_liquidation": snapshot["broker_net_liquidation"],
        "broker_cash": snapshot["broker_cash"],
        "broker_buying_power": snapshot["broker_buying_power"],
        "total_portfolio_exposure_percent": snapshot["total_portfolio_exposure_percent"],
        "total_market_value": snapshot["total_market_value"],
        "exposure_by_symbol": snapshot["exposure_by_symbol"],
        "exposure_by_sector": snapshot["exposure_by_sector"],
        "largest_position": snapshot["largest_position"],
        "largest_sector": snapshot["largest_sector"],
    }


async def get_risk_alerts() -> dict:
    snapshot = await get_portfolio_risk()
    return {
        "checked_at": snapshot["checked_at"],
        "risk_state": snapshot["risk_state"],
        "new_buy_risk_status": snapshot["new_buy_risk_status"],
        "blocks_new_buys": snapshot["blocks_new_buys"],
        "block_reasons": snapshot["block_reasons"],
        "alerts": snapshot["alerts"],
        "concentration_warnings": snapshot["concentration_warnings"],
    }


async def require_new_buy_allowed(symbol: str | None = None) -> None:
    snapshot = await get_portfolio_risk()
    if not snapshot.get("blocks_new_buys"):
        return

    reason = "; ".join(snapshot.get("block_reasons") or []) or "Portfolio risk engine blocks new buys."
    await database.safe_record_trade_journal_event({
        "symbol": symbol,
        "event_type": "RISK_BLOCK_BUY",
        "decision": "BLOCKED",
        "reason": reason,
        "source_module": "portfolio_risk_engine.require_new_buy_allowed",
        "risk_percent": snapshot.get("total_portfolio_exposure_percent"),
        "realized_pnl": snapshot.get("daily_realized_pnl"),
        "unrealized_pnl": snapshot.get("unrealized_pnl"),
        "raw_payload": snapshot,
    })
    raise RuntimeError(f"BUY blocked by portfolio risk engine: {reason}")

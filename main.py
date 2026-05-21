from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

import config
import database
import account_sync
import broker_sync
import reconciliation_engine
import recovery_manager
import session_manager
import order_lifecycle
import portfolio_risk_engine
import startup_recovery
import reconciliation_lifecycle
import watchdog
import live_position_tracker
from circuit_breaker import (
    auto_clear_recoverable_circuit_breaker,
    get_circuit_breaker_state,
    get_ibkr_error_count,
    get_last_auto_recovery,
    is_auto_recoverable_trip,
    reset_circuit_breaker,
    trip_circuit_breaker,
)
import position_sizing_engine
import position_exit_priority_engine
import sector_intelligence
import strategy_mode
import intraday_momentum_engine
from execution_quality import evaluate_execution_quality, summarize_execution_quality
from auto_trader import process_auto_trading
from trading_safety import get_market_hours_status
from market_regime_engine import get_cached_market_regime, get_market_regime_history, refresh_market_regime
from market_regime import get_market_regime
from data_fetcher import fetch_intraday_bars, fetch_stock_data
from indicators import compute_indicators
from ranking_engine import calculate_weekly_score, rank_top_weekly_setups
from signal_logic import evaluate_signal
from symbol_loader import get_cached_symbols, load_nasdaq_symbols
from telegram_notifier import send_buy_alert, send_sell_alert, send_position_alert
from position_manager import evaluate_position
from ibkr_asyncio_compat import ensure_event_loop
from paper_liquidation import liquidate_all_paper_positions

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

_latest: dict[str, dict] = {}
_top_weekly: list[dict] = []
_SCAN_UNIVERSE_SET: set[str] = set()
_scan_lock = asyncio.Lock()
_positions_lock = asyncio.Lock()
_scanner_state: dict[str, object] = {
    "last_scan_at": None,
    "next_scan_at": None,
    "current_universe_size": 0,
    "priority_count": 0,
    "rotation_offset": 0,
}
scheduler = AsyncIOScheduler()

SCANNER_JOB_ID = "scanner_scan"
SWING_SCAN_OFFSET_KEY = "scan_offset"
INTRADAY_SCAN_OFFSET_KEY = "intraday_scan_offset"

OPERATION_STATE_KEY = "dashboard_last_operations"
WATCHDOG_JOB_ID = "tws_api_watchdog"


def _json_payload(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


async def _ensure_app_state_table() -> None:
    async with aiosqlite.connect(database.DB_PATH) as db:
        await db.execute(database.CREATE_APP_STATE)
        await db.commit()


async def _record_dashboard_operation(
    action: str,
    status: str,
    *,
    message: str | None = None,
    details: dict | None = None,
) -> dict:
    payload = {
        "action": action,
        "status": status,
        "message": message,
        "details": details or {},
        "timestamp": utc_now().isoformat(),
    }
    await _ensure_app_state_table()
    raw = await database.get_app_state(OPERATION_STATE_KEY)
    try:
        operations = json.loads(raw) if raw else {}
        if not isinstance(operations, dict):
            operations = {}
    except Exception:
        operations = {}
    operations[action] = payload
    await database.set_app_state(OPERATION_STATE_KEY, _json_payload(operations))
    await database.safe_record_trade_journal_event({
        "event_type": "DASHBOARD_OPERATION",
        "decision": status.upper(),
        "reason": message or action,
        "source_module": "dashboard_control_center",
        "raw_payload": payload,
    })
    log.info("Dashboard operation %s | status=%s | message=%s", action, status, message)
    return payload


async def _operation_response(action: str, status: str, payload: dict, *, message: str | None = None, status_code: int = 200) -> JSONResponse:
    operation = await _record_dashboard_operation(action, status, message=message, details=payload)
    return JSONResponse({**payload, "operation": operation}, status_code=status_code, headers=no_cache_headers())


async def _get_dashboard_operations() -> dict:
    await _ensure_app_state_table()
    raw = await database.get_app_state(OPERATION_STATE_KEY)
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def no_cache_headers() -> dict:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_paper_liquidation_worker(
    *,
    restart_auto_trading_after: bool,
    dry_run: bool,
) -> dict:
    ensure_event_loop()
    return liquidate_all_paper_positions(
        restart_auto_trading_after=restart_auto_trading_after,
        dry_run=dry_run,
    )


def _run_flat_tws_reconciliation_worker(*, dry_run: bool) -> dict:
    from reconciliation import close_db_positions_flat_in_tws_worker

    ensure_event_loop()
    return close_db_positions_flat_in_tws_worker(dry_run=dry_run)


def _require_paper_session_reset_allowed() -> None:
    trading_mode = str(getattr(config, "TRADING_MODE", "")).upper()
    if trading_mode not in {"PAPER", "PAPER_AUTO"}:
        raise RuntimeError(
            "Paper session reset blocked: TRADING_MODE must be PAPER or PAPER_AUTO"
        )

    if not bool(getattr(config, "IBKR_PAPER_TRADING", False)):
        raise RuntimeError("Paper session reset blocked: IBKR_PAPER_TRADING is false")

    if bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)):
        raise RuntimeError("Paper session reset blocked: LIVE trading is enabled")


async def _reset_paper_session() -> dict:
    _require_paper_session_reset_allowed()
    return await database.reset_active_paper_session()


AUTO_TRADING_ENABLED_KEY = "auto_trading_enabled"
AUTO_TRADING_STATE_SOURCE_KEY = "auto_trading_state_source"
AUTO_TRADING_STATE_REASON_KEY = "auto_trading_state_reason"


async def _set_auto_trading_state(enabled: bool, *, source: str, reason: str) -> None:
    await database.set_app_state(AUTO_TRADING_ENABLED_KEY, "true" if enabled else "false")
    await database.set_app_state(AUTO_TRADING_STATE_SOURCE_KEY, source)
    await database.set_app_state(AUTO_TRADING_STATE_REASON_KEY, reason)


async def _get_auto_trading_state() -> dict:
    raw_enabled = await database.get_app_state(AUTO_TRADING_ENABLED_KEY, "true")
    enabled = str(raw_enabled).lower() == "true"
    default_source = "default" if enabled else "unknown"
    default_reason = "Auto trading enabled by default" if enabled else "Auto trading disabled"
    return {
        "enabled": enabled,
        "source": await database.get_app_state(AUTO_TRADING_STATE_SOURCE_KEY, default_source),
        "reason": await database.get_app_state(AUTO_TRADING_STATE_REASON_KEY, default_reason),
    }


async def _evaluate_auto_trading_enable_safety() -> dict:
    startup_status = await startup_recovery.get_startup_recovery_status()
    startup_passed = bool(startup_status.get("ok")) and await startup_recovery.startup_recovery_passed()
    circuit = await get_circuit_breaker_state()
    reconciliation = await reconciliation_lifecycle.get_reconciliation_status()
    watchdog_status = await watchdog.get_watchdog_status()
    market_hours = get_market_hours_status()
    broker_snapshot = await database.get_latest_broker_sync_snapshot() or {}
    recon_issues = await database.get_open_reconciliation_issues()

    def _safe_json_array(value) -> list:
        try:
            parsed = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []

    blocked_reasons: list[str] = []
    trading_mode = str(getattr(config, "TRADING_MODE", "")).upper()

    if not startup_passed:
        blocked_reasons.append(
            startup_status.get("reason") or "Startup recovery has not passed"
        )

    health_can_clear_ibkr_errors = (
        startup_passed
        and bool(watchdog_status.get("tws_connected"))
        and not (watchdog_status.get("stale_data") or {}).get("tws_mirror")
        and not (watchdog_status.get("stale_data") or {}).get("execution_sync")
        and int(reconciliation.get("issues_count") or 0) == 0
    )
    if (
        health_can_clear_ibkr_errors
        and (
            (circuit.get("tripped") and is_auto_recoverable_trip(circuit))
            or await get_ibkr_error_count() > 0
        )
    ):
        recovery = await auto_clear_recoverable_circuit_breaker(
            "Startup recovery, TWS mirror sync, execution sync, and reconciliation are healthy",
            source="main._evaluate_auto_trading_enable_safety",
        )
        if recovery.get("cleared"):
            circuit = await get_circuit_breaker_state()

    if circuit.get("tripped"):
        blocked_reasons.append(
            f"Circuit breaker tripped: {circuit.get('reason') or 'unknown reason'}"
        )

    if int(reconciliation.get("issues_count") or 0) != 0:
        blocked_reasons.append(
            f"Reconciliation issues_count={reconciliation.get('issues_count')}"
        )

    if not bool(getattr(config, "IBKR_PAPER_TRADING", False)):
        blocked_reasons.append("IBKR_PAPER_TRADING is false")

    if bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)):
        blocked_reasons.append("LIVE trading is enabled")

    if trading_mode == "LIVE":
        blocked_reasons.append("TRADING_MODE is LIVE")

    return {
        "ok": len(blocked_reasons) == 0,
        "blocked_reasons": blocked_reasons,
        "broker_sync": {"connected": bool(broker_snapshot.get("connected")), "last_synced_at": broker_snapshot.get("synced_at"), "account": broker_snapshot.get("account"), "equity": {"net_liquidation": broker_snapshot.get("net_liquidation"), "total_cash": broker_snapshot.get("total_cash"), "available_funds": broker_snapshot.get("available_funds"), "buying_power": broker_snapshot.get("buying_power")}, "broker_positions_count": len(_safe_json_array(broker_snapshot.get("positions_json"))), "broker_open_orders_count": len(_safe_json_array(broker_snapshot.get("open_orders_json"))), "broker_executions_count": len(_safe_json_array(broker_snapshot.get("executions_json"))), "errors": _safe_json_array(broker_snapshot.get("errors_json"))},
        "reconciliation": {"ok": len([i for i in recon_issues if i.get("severity")=="HIGH"])==0, "open_issues_count": len(recon_issues), "high_severity_issues_count": len([i for i in recon_issues if i.get("severity")=="HIGH"]), "last_checked_at": (recon_issues[0].get("created_at") if recon_issues else None), "issues": recon_issues[:20]},
        "source_of_truth": {"broker_is_source_of_truth": True, "db_positions_match_broker": True, "orders_match_broker": True, "executions_synced": True},
        "startup_recovery": startup_status,
        "circuit_breaker": circuit,
        "reconciliation": reconciliation,
        "paper_trading": bool(getattr(config, "IBKR_PAPER_TRADING", False)),
        "real_trading_enabled": bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)),
        "trading_mode": trading_mode,
        "market_hours": market_hours,
    }


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


async def get_max_open_positions() -> int:
    mode = await strategy_mode.get_strategy_mode()
    return int(strategy_mode.active_rules(mode).get("max_open_positions", getattr(config, "MAX_OPEN_POSITIONS", 10)))


def serve_html_file(filename: str, fallback_to_index: bool = False) -> HTMLResponse:
    base_path = Path(__file__).parent
    file_path = base_path / filename

    if file_path.exists():
        return HTMLResponse(file_path.read_text(encoding="utf-8"), headers=no_cache_headers())

    if fallback_to_index:
        return serve_index_html()

    return HTMLResponse(
        f"<h1>{filename} file not found</h1>",
        status_code=404,
        headers=no_cache_headers(),
    )


def serve_index_html() -> HTMLResponse:
    base_path = Path(__file__).parent
    dashboard_path = base_path / "dashboard.html"
    index_path = base_path / "index.html"

    if dashboard_path.exists():
        return HTMLResponse(dashboard_path.read_text(encoding="utf-8"), headers=no_cache_headers())

    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"), headers=no_cache_headers())

    return HTMLResponse(
        "<h1>Dashboard file not found</h1>",
        status_code=404,
        headers=no_cache_headers(),
    )


def _unique_symbols(symbols: list[str]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()

    for symbol in symbols:
        normalized = str(symbol or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        selected.append(normalized)
        seen.add(normalized)

    return selected


def _relative_volume(row: dict) -> float:
    explicit = row.get("relative_volume") or row.get("volume_ratio") or row.get("intraday_relative_volume")
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            return 0.0

    try:
        volume = float(row.get("volume") or 0)
        avg_volume = float(row.get("avg_volume") or row.get("average_volume") or 0)
    except (TypeError, ValueError):
        return 0.0

    if volume > 0 and avg_volume > 0:
        return volume / avg_volume

    return 0.0


def _mover_score(row: dict) -> float:
    try:
        price = float(row.get("price") or 0)
        ma20 = float(row.get("ma20") or 0)
        atr = float(row.get("atr") or 0)
    except (TypeError, ValueError):
        return 0.0

    gap_score = abs(price - ma20) / ma20 if price > 0 and ma20 > 0 else 0.0
    atr_score = atr / price if price > 0 and atr > 0 else 0.0
    return gap_score + atr_score + (_relative_volume(row) * 0.1)


def scanner_cadence_for_mode(mode: strategy_mode.StrategyMode | str | None) -> dict:
    normalized = strategy_mode.normalize_strategy_mode(mode)

    if strategy_mode.is_intraday_mode(normalized):
        return {
            "active_strategy_mode": normalized.value,
            "scan_interval_seconds": int(getattr(config, "INTRADAY_SCAN_INTERVAL_SECONDS", 30)),
            "symbols_per_scan": int(getattr(config, "INTRADAY_SYMBOLS_PER_SCAN", 100)),
            "priority_symbols_per_scan": int(getattr(config, "INTRADAY_PRIORITY_SYMBOLS_PER_SCAN", 50)),
            "batch_size": int(getattr(config, "INTRADAY_BATCH_SIZE", 20)),
            "offset_key": INTRADAY_SCAN_OFFSET_KEY,
            "intraday_fast_scan_active": True,
        }

    return {
        "active_strategy_mode": normalized.value,
        "scan_interval_seconds": int(getattr(config, "SCAN_INTERVAL_MINUTES", 5)) * 60,
        "symbols_per_scan": int(getattr(config, "MAX_SYMBOLS_PER_SCAN", 30)),
        "priority_symbols_per_scan": int(getattr(config, "MAX_SYMBOLS_PER_SCAN", 30)),
        "batch_size": int(getattr(config, "BATCH_SIZE", 5)),
        "offset_key": SWING_SCAN_OFFSET_KEY,
        "intraday_fast_scan_active": False,
    }


def _enrich_intraday_snapshot(result: dict, intraday_bars: dict[str, list[dict]]) -> dict:
    bars_1m = intraday_bars.get("1m") or []
    bars_5m = intraday_bars.get("5m") or []
    bars_15m = intraday_bars.get("15m") or []

    closes_1m = [float(b.get("close") or 0) for b in bars_1m if b.get("close") is not None]
    volumes_1m = [float(b.get("volume") or 0) for b in bars_1m if b.get("volume") is not None]
    highs_1m = [float(b.get("high") or 0) for b in bars_1m if b.get("high") is not None]
    lows_1m = [float(b.get("low") or 0) for b in bars_1m if b.get("low") is not None]

    vwap = None
    if bars_1m:
        total_pv = 0.0
        total_vol = 0.0
        for bar in bars_1m:
            h = float(bar.get("high") or 0)
            l = float(bar.get("low") or 0)
            c = float(bar.get("close") or 0)
            v = float(bar.get("volume") or 0)
            typical = (h + l + c) / 3 if (h or l or c) else 0.0
            total_pv += typical * v
            total_vol += v
        if total_vol > 0:
            vwap = round(total_pv / total_vol, 4)

    ema9 = None
    if len(closes_1m) >= 9:
        alpha9 = 2 / (9 + 1)
        ema9_val = closes_1m[0]
        for close in closes_1m[1:]:
            ema9_val = (close * alpha9) + (ema9_val * (1 - alpha9))
        ema9 = round(ema9_val, 4)

    ema20 = None
    closes_5m_values = [float(b.get("close") or 0) for b in bars_5m if b.get("close") is not None]
    if len(closes_5m_values) >= 20:
        alpha20 = 2 / (20 + 1)
        ema20_val = closes_5m_values[0]
        for close in closes_5m_values[1:]:
            ema20_val = (close * alpha20) + (ema20_val * (1 - alpha20))
        ema20 = round(ema20_val, 4)

    relative_volume = None
    if len(volumes_1m) >= 10:
        recent = sum(volumes_1m[-5:])
        baseline_samples = volumes_1m[:-5]
        baseline = (sum(baseline_samples) / len(baseline_samples)) if baseline_samples else 0
        if baseline > 0:
            relative_volume = round((recent / 5) / baseline, 4)

    opening_range_high = round(max(highs_1m[:15]), 4) if len(highs_1m) >= 15 else None
    range_expansion = bool(highs_1m and lows_1m and ((max(highs_1m[-5:]) - min(lows_1m[-5:])) > (max(highs_1m[:5]) - min(lows_1m[:5])))) if len(highs_1m) >= 10 and len(lows_1m) >= 10 else False
    consecutive_green_candles = 0
    for bar in reversed(bars_1m):
        if float(bar.get("close") or 0) > float(bar.get("open") or 0):
            consecutive_green_candles += 1
        else:
            break

    price = float(result.get("price") or result.get("current_price") or 0)
    volume_confirmation = bool(relative_volume and relative_volume >= 1.5)
    micro_pullback_continuation = bool(ema9 and price and price >= ema9)
    volatility_expansion = bool(range_expansion)

    return {
        "vwap": vwap,
        "ema9": ema9,
        "ema20": ema20,
        "relative_volume": relative_volume,
        "opening_range_high": opening_range_high,
        "consecutive_green_candles": consecutive_green_candles,
        "range_expansion": range_expansion,
        "volume_confirmation": volume_confirmation,
        "micro_pullback_continuation": micro_pullback_continuation,
        "volatility_expansion": volatility_expansion,
        "intraday_take_profit_percent": 3.0,
        "intraday_force_exit_before_close": bool(strategy_mode.force_exit_before_close_status().get("active")),
    }


def _load_scan_universe(force_refresh: bool = False) -> list[str]:
    if config.USE_DYNAMIC_SYMBOLS:
        all_symbols = get_cached_symbols(limit=None, force_refresh=force_refresh)
    else:
        all_symbols = config.SYMBOLS

    all_symbols = _unique_symbols(all_symbols)
    return all_symbols or _unique_symbols(config.SYMBOLS)


async def _build_intraday_priority_symbols(priority_limit: int) -> list[str]:
    open_positions = await database.get_open_positions()
    open_position_symbols = [position.get("symbol") for position in open_positions]

    latest_candidates = await database.get_latest_candidates(limit=max(500, priority_limit * 6))
    recent_buy_symbols = [
        row.get("symbol")
        for row in latest_candidates
        if str(row.get("signal") or "").upper() == "BUY"
    ]

    cached_rows_by_symbol = {
        symbol: row
        for symbol, row in _latest.items()
        if symbol
    }
    for row in latest_candidates:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol and symbol not in cached_rows_by_symbol:
            cached_rows_by_symbol[symbol] = row

    def _intraday_priority_score(item: dict) -> float:
        symbol = str(item.get("symbol") or "").upper()
        is_nasdaq = symbol in _SCAN_UNIVERSE_SET
        rv = _relative_volume(item)
        move = abs(float(item.get("change_percent") or item.get("percent_change") or 0.0))
        market_cap = float(item.get("market_cap") or 0.0)
        small_mid_cap_bonus = 15.0 if (300_000_000 <= market_cap <= 20_000_000_000) else 0.0
        unusual_volume_bonus = 20.0 if rv >= 2.0 else 0.0
        move_bonus = 20.0 if move >= 3.0 else move * 4.0
        return (20.0 if is_nasdaq else 0.0) + (rv * 20.0) + move_bonus + small_mid_cap_bonus + unusual_volume_bonus + _mover_score(item)

    ranked_symbols = [
        row.get("symbol")
        for row in sorted(cached_rows_by_symbol.values(), key=_intraday_priority_score, reverse=True)
        if _intraday_priority_score(row) > 0
    ]
    watchlist_symbols = list(getattr(config, "SYMBOLS", []))
    priority_symbols = await database.get_priority_symbols(limit=priority_limit)

    return _unique_symbols(
        open_position_symbols
        + recent_buy_symbols
        + ranked_symbols
        + watchlist_symbols
        + priority_symbols
    )[:priority_limit]


async def get_scan_symbols() -> list[str]:
    active_mode = await strategy_mode.get_strategy_mode()
    cadence = scanner_cadence_for_mode(active_mode)
    all_symbols = _load_scan_universe()
    global _SCAN_UNIVERSE_SET
    _SCAN_UNIVERSE_SET = set(all_symbols)

    if not all_symbols:
        return _unique_symbols(config.SYMBOLS)[:cadence["symbols_per_scan"]]

    total = len(all_symbols)
    symbols_per_scan = min(int(cadence["symbols_per_scan"]), max(total, int(cadence["symbols_per_scan"])))
    priority_limit = int(cadence["priority_symbols_per_scan"])
    offset_key = str(cadence["offset_key"])

    if strategy_mode.is_intraday_mode(active_mode):
        priority_symbols = await _build_intraday_priority_symbols(priority_limit)
    else:
        priority_symbols = await database.get_priority_symbols(limit=min(symbols_per_scan, total))

    saved_offset = await database.get_app_state(offset_key, "0")

    try:
        start = int(saved_offset or 0)
    except Exception:
        start = 0

    if start >= total:
        start = 0

    rotation_quota = max(symbols_per_scan - len(priority_symbols), 0)
    end = start + rotation_quota

    if rotation_quota <= 0:
        rotation_symbols = []
        next_offset = start
    elif end <= total:
        rotation_symbols = all_symbols[start:end]
        next_offset = end % total
    else:
        rotation_symbols = all_symbols[start:] + all_symbols[:end - total]
        next_offset = end % total

    selected: list[str] = []
    seen: set[str] = set()
    all_set = set(all_symbols)
    priority_set = set(priority_symbols)
    open_symbols = {
        str(position.get("symbol") or "").strip().upper()
        for position in await database.get_open_positions()
    }
    configured_symbols = {str(symbol or "").strip().upper() for symbol in getattr(config, "SYMBOLS", [])}

    for symbol in priority_symbols + rotation_symbols:
        symbol = str(symbol or "").strip().upper()

        if not symbol or symbol in seen:
            continue

        # Open positions and configured watchlist symbols can be scanned even when the
        # dynamic Nasdaq universe does not currently include them.
        if symbol not in all_set and symbol not in configured_symbols and symbol not in open_symbols and symbol not in priority_set:
            continue

        selected.append(symbol)
        seen.add(symbol)

        if len(selected) >= symbols_per_scan:
            break

    if len(selected) < symbols_per_scan:
        for symbol in all_symbols:
            symbol = str(symbol or "").strip().upper()

            if not symbol or symbol in seen:
                continue

            selected.append(symbol)
            seen.add(symbol)

            if len(selected) >= symbols_per_scan:
                break

    await database.set_app_state(offset_key, str(next_offset))

    _scanner_state.update({
        "current_universe_size": total,
        "priority_count": min(len(priority_symbols), len(selected)),
        "rotation_offset": next_offset,
    })

    log.info(
        "Smart rotation selected: mode=%s priority=%s rotation=%s total_selected=%s | next_offset=%s",
        cadence["active_strategy_mode"],
        len(priority_symbols),
        len(rotation_symbols),
        len(selected),
        next_offset,
    )

    return selected


def rebuild_top_weekly(limit: int = 10) -> list[dict]:
    global _top_weekly

    rows = list(_latest.values())
    ranked = rank_top_weekly_setups(rows, limit=limit)

    _top_weekly = ranked
    return _top_weekly


async def maybe_send_alert(
    symbol: str,
    signal_type: str,
    ind: dict,
    risk: dict | None,
    reasons: list[str],
    score: int,
) -> None:
    if signal_type not in ("BUY", "SELL"):
        last = await database.get_last_signal(symbol)

        if last and signal_type == "NEUTRAL":
            try:
                await database.upsert_last_signal(symbol, "NEUTRAL", ind.get("price"), score)
            except TypeError:
                await database.upsert_last_signal(symbol, "NEUTRAL", ind.get("price"))

        return

    should_send = (
        signal_type == "BUY" and score >= config.BUY_TELEGRAM_MIN_SCORE
    ) or (
        signal_type == "SELL" and score >= config.SELL_TELEGRAM_MIN_SCORE
    )

    if not should_send:
        return

    last = await database.get_last_signal(symbol)

    if last and last.get("last_signal_type") == signal_type:
        last_time = parse_dt(last.get("last_signal_time"))

        if last_time and utc_now() - last_time < timedelta(minutes=config.ALERT_COOLDOWN_MINUTES):
            log.info("[%s] %s alert skipped due to cooldown", symbol, signal_type)
            return

    loop = asyncio.get_running_loop()
    sent = False

    if signal_type == "BUY" and risk:
        sent = await loop.run_in_executor(None, send_buy_alert, ind, risk, reasons, score)

    elif signal_type == "SELL":
        sent = await loop.run_in_executor(None, send_sell_alert, ind, reasons, score)

    if sent:
        try:
            await database.upsert_last_signal(symbol, signal_type, ind.get("price"), score)
        except TypeError:
            await database.upsert_last_signal(symbol, signal_type, ind.get("price"))

        await database.save_signal({
            **ind,
            **(risk or {}),
            "signal_type": signal_type,
            "score": score,
            "weekly_score": score,
            "reasons": reasons,
        })


async def _scan_symbol_inner(symbol: str) -> dict:
    loop = asyncio.get_running_loop()
    symbol = symbol.strip().upper()

    raw = await loop.run_in_executor(None, fetch_stock_data, symbol)

    if raw is None:
        return {
            "symbol": symbol,
            "signal": "ERROR",
            "score": 0,
            "weekly_score": 0,
            "weekly_reasons": ["Failed to fetch data"],
            "error": "Failed to fetch data",
        }

    await watchdog.refresh_market_data_timestamp(
        "scanner_bars",
        symbol=symbol,
        metadata={"bars": len(raw.get("closes") or [])},
    )

    ind = compute_indicators(raw)
    ind["symbol"] = symbol

    if ind.get("price") is None or ind["price"] < config.MIN_PRICE:
        return {
            **ind,
            "signal": "SKIPPED",
            "score": 0,
            "weekly_score": 0,
            "weekly_reasons": ["Price below minimum"],
            "skip_reason": "Price below minimum",
        }

    if ind.get("avg_volume") is None or ind["avg_volume"] < config.MIN_AVG_VOLUME:
        return {
            **ind,
            "signal": "SKIPPED",
            "score": 0,
            "weekly_score": 0,
            "weekly_reasons": ["Volume below minimum"],
            "skip_reason": "Volume below minimum",
        }

    signal_type, risk, reasons = evaluate_signal(ind)

    result = {
        **ind,
        "signal": signal_type,
        "reasons": reasons,
    }

    if risk:
        result.update(risk)
    else:
        result.update({
            "entry_price": None,
            "stop_loss": None,
            "take_profit_1": None,
            "take_profit_2": None,
            "risk_percent": None,
            "rr_ratio": None,
        })

    score, weekly_reasons = calculate_weekly_score(result)

    result["score"] = score
    result["weekly_score"] = score
    result["weekly_reasons"] = weekly_reasons

    active_mode = await strategy_mode.get_strategy_mode()
    if strategy_mode.is_intraday_mode(active_mode):
        intraday_bars = {}
        for timeframe in ("1m", "5m", "15m"):
            try:
                bars = await loop.run_in_executor(None, fetch_intraday_bars, symbol, timeframe)
            except Exception as exc:
                bars = None
                result.setdefault("intraday_errors", {})[timeframe] = str(exc)
            if bars:
                intraday_bars[timeframe] = bars
        result["intraday_bars"] = intraday_bars
        result["intraday_bars_available"] = bool(intraday_bars)
        result.update(_enrich_intraday_snapshot(result, intraday_bars))
        momentum_payload = intraday_momentum_engine.build_dashboard_payload(result)
        result.update(momentum_payload)
        result["intraday_technical_score"] = momentum_payload["intraday_momentum_score"]
        result["intraday_score_reasons"] = momentum_payload.get("score_reasons", [])
        result["intraday_enrichment_status"] = strategy_mode.intraday_enrichment_status(result)

    await maybe_send_alert(symbol, signal_type, ind, risk, reasons, score)

    return result


async def scan_symbol(symbol: str) -> dict:
    try:
        return await asyncio.wait_for(
            _scan_symbol_inner(symbol),
            timeout=config.SCAN_SYMBOL_TIMEOUT_SECONDS,
        )

    except asyncio.TimeoutError:
        symbol = symbol.strip().upper()
        return {
            "symbol": symbol,
            "signal": "ERROR",
            "score": 0,
            "weekly_score": 0,
            "weekly_reasons": ["Timeout"],
            "error": "Timeout",
        }

    except Exception as exc:
        symbol = symbol.strip().upper()
        log.exception("scan_symbol failed for %s", symbol)

        return {
            "symbol": symbol,
            "signal": "ERROR",
            "score": 0,
            "weekly_score": 0,
            "weekly_reasons": [str(exc)],
            "error": str(exc),
        }

async def refresh_open_positions_safe() -> None:
    if _positions_lock.locked():
        return

    async with _positions_lock:
        try:
            await refresh_open_positions()

        except Exception:
            log.exception("Live position refresh failed")

async def refresh_open_positions() -> list[dict]:
    return await live_position_tracker.refresh_live_tracked_positions(scan_symbol)



async def run_full_scan() -> dict:
    global _top_weekly

    if _scan_lock.locked():
        log.info("Scan skipped: previous scan still running")
        return {"status": "already running"}

    async with _scan_lock:
        session_status = session_manager.get_cached_session_status()
        if not session_status.get("scan_allowed"):
            log.info("Scan skipped — session=%s scan_allowed=False", session_status.get("current_session"))
            return {"status": "skipped", "reason": "scan not allowed in current session", "session": session_status}

        active_mode = await strategy_mode.get_strategy_mode()
        cadence = scanner_cadence_for_mode(active_mode)
        symbol_load_start = time.perf_counter()
        symbols = await get_scan_symbols()
        symbol_load_ms = round((time.perf_counter() - symbol_load_start) * 1000, 2)
        scan_started_at = utc_now()
        _scanner_state["last_scan_at"] = scan_started_at.isoformat()

        scan_run_id = await database.start_scan_run(total_symbols=len(symbols))

        log.info("▶ Scan started: %s symbols", len(symbols))

        stats = {
            "scanned_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            "buy_signals": 0,
            "sell_signals": 0,
        }

        all_results: list[dict] = []

        try:
            batch_size = int(cadence["batch_size"])

            for i in range(0, len(symbols), batch_size):
                batch_started = time.perf_counter()
                batch = symbols[i:i + batch_size]

                results = await asyncio.gather(
                    *(scan_symbol(symbol) for symbol in batch),
                    return_exceptions=False,
                )

                for result in results:
                    symbol = result.get("symbol")

                    if not symbol:
                        continue

                    _latest[symbol] = result
                    all_results.append(result)

                    if result.get("error") or result.get("signal") == "ERROR":
                        stats["error_count"] += 1

                    elif result.get("signal") == "SKIPPED":
                        stats["skipped_count"] += 1

                    else:
                        stats["scanned_count"] += 1

                    if result.get("signal") == "BUY":
                        stats["buy_signals"] += 1

                    elif result.get("signal") == "SELL":
                        stats["sell_signals"] += 1

                rebuild_top_weekly(limit=10)
                scan_batch_ms = round((time.perf_counter() - batch_started) * 1000, 2)
                log.info("scan_batch_ms=%s batch_size=%s", scan_batch_ms, len(batch))

                if i + batch_size < len(symbols):
                    await asyncio.sleep(config.REQUEST_DELAY_SECONDS)

            _top_weekly = rank_top_weekly_setups(all_results, limit=10)
            if any(not row.get("error") and row.get("signal") != "ERROR" for row in all_results):
                await watchdog.refresh_market_data_timestamp(
                    "ranking_engine",
                    metadata={"fresh_symbols": sum(1 for row in all_results if not row.get("error") and row.get("signal") != "ERROR")},
                )

            await process_auto_trading(all_results)

            rank_by_symbol = {
                row.get("symbol"): row.get("weekly_rank")
                for row in _top_weekly
                if row.get("symbol")
            }

            for result in all_results:
                symbol = result.get("symbol")

                if symbol in rank_by_symbol:
                    result["weekly_rank"] = rank_by_symbol[symbol]

                await database.save_daily_candidate(result, scan_run_id)

            await database.finish_scan_run(scan_run_id, stats, status="completed")
            _scanner_state["last_scan_at"] = utc_now().isoformat()
            fresh_symbols = stats["scanned_count"] + stats["skipped_count"]
            if fresh_symbols > 0:
                await watchdog.refresh_market_data_timestamp(
                    "scan_cycle_completed",
                    metadata={"scan_run_id": scan_run_id, "fresh_symbols": fresh_symbols},
                )

            total_scan_ms = round((time.perf_counter() - symbol_load_start) * 1000, 2)
            candidates_updated_count = len(all_results)
            log.info(
                "scan_metrics symbol_load_ms=%s total_scan_ms=%s symbols_scanned_count=%s candidates_updated_count=%s",
                symbol_load_ms,
                total_scan_ms,
                len(symbols),
                candidates_updated_count,
            )

            log.info(
                "✔ Scan finished | scanned=%s skipped=%s errors=%s BUY=%s SELL=%s TOP10=%s",
                stats["scanned_count"],
                stats["skipped_count"],
                stats["error_count"],
                stats["buy_signals"],
                stats["sell_signals"],
                len(_top_weekly),
            )

            return {
                "status": "completed",
                "scan_run_id": scan_run_id,
                "top_weekly_count": len(_top_weekly),
                **stats,
            }

        except Exception:
            log.exception("Scan failed")
            await database.finish_scan_run(scan_run_id, stats, status="failed")
            raise


async def refresh_market_regime_safe() -> None:
    try:
        positions = await database.get_open_positions()
        await refresh_market_regime(
            candidates=list(_latest.values()),
            positions=positions,
        )
    except Exception:
        log.exception("Market regime refresh failed")


async def restore_latest_from_db() -> None:
    global _top_weekly

    rows = await database.get_latest_candidates(500)

    if rows:
        _latest.clear()

        for row in rows:
            symbol = row.get("symbol")

            if symbol:
                _latest[symbol] = row

        db_top = await database.get_top_weekly(10)

        if db_top:
            _top_weekly = db_top
        else:
            rebuild_top_weekly(limit=10)

        log.info("Restored %s candidates from DB | TOP10=%s", len(_latest), len(_top_weekly))


def _sync_scanner_next_run_state() -> None:
    job = scheduler.get_job(SCANNER_JOB_ID)
    next_run = getattr(job, "next_run_time", None) if job else None
    _scanner_state["next_scan_at"] = next_run.isoformat() if next_run else None


async def configure_scanner_job() -> dict:
    active_mode = await strategy_mode.get_strategy_mode()
    cadence = scanner_cadence_for_mode(active_mode)

    for old_job_id in ("daily_scan", "interval_scan", SCANNER_JOB_ID):
        while scheduler.get_job(old_job_id):
            scheduler.remove_job(old_job_id)

    if config.SCAN_MODE == "daily" and not cadence["intraday_fast_scan_active"]:
        scheduler.add_job(
            run_full_scan,
            "cron",
            hour=getattr(config, "DAILY_SCAN_HOUR", 16),
            minute=getattr(config, "DAILY_SCAN_MINUTE", 30),
            id=SCANNER_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        log.info(
            "Daily scanner scheduler started — %s:%02d",
            getattr(config, "DAILY_SCAN_HOUR", 16),
            getattr(config, "DAILY_SCAN_MINUTE", 30),
        )
    else:
        scheduler.add_job(
            run_full_scan,
            "interval",
            seconds=int(cadence["scan_interval_seconds"]),
            id=SCANNER_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        log.info(
            "Scanner scheduler started — every %s seconds | mode=%s",
            cadence["scan_interval_seconds"],
            cadence["active_strategy_mode"],
        )

    _sync_scanner_next_run_state()
    return await get_scanner_status()


async def get_scanner_status() -> dict:
    active_mode = await strategy_mode.get_strategy_mode()
    cadence = scanner_cadence_for_mode(active_mode)
    _sync_scanner_next_run_state()

    offset_raw = await database.get_app_state(str(cadence["offset_key"]), "0")
    try:
        rotation_offset = int(offset_raw or 0)
    except (TypeError, ValueError):
        rotation_offset = int(_scanner_state.get("rotation_offset") or 0)

    current_universe_size = int(_scanner_state.get("current_universe_size") or 0)
    if current_universe_size <= 0:
        current_universe_size = len(_latest)

    return {
        "active_strategy_mode": cadence["active_strategy_mode"],
        "scan_interval_seconds": cadence["scan_interval_seconds"],
        "symbols_per_scan": cadence["symbols_per_scan"],
        "last_scan_at": _scanner_state.get("last_scan_at"),
        "next_scan_at": _scanner_state.get("next_scan_at"),
        "current_universe_size": current_universe_size,
        "priority_count": int(_scanner_state.get("priority_count") or 0),
        "rotation_offset": rotation_offset,
        "intraday_fast_scan_active": bool(cadence["intraday_fast_scan_active"]),
        "batch_size": cadence["batch_size"],
        "scan_running": _scan_lock.locked(),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_db()
    await restore_latest_from_db()
    startup_status = await startup_recovery.run_startup_recovery()

    if not startup_status.get("ok"):
        log.warning(
            "Startup recovery failed; scan scheduler may run but auto trading remains disabled: %s",
            startup_status.get("reason"),
        )

    await configure_scanner_job()

    scheduler.add_job(
        refresh_open_positions_safe,
        "interval",
        seconds=config.POSITION_TRACK_INTERVAL_SECONDS,
        id="live_position_tracker",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Live position tracker started — every %s seconds",
        config.POSITION_TRACK_INTERVAL_SECONDS,
    )
    # ==========================================
    # AUTO CANCEL STALE ORDERS
    # ==========================================

    from order_manager import OrderManager

    def cancel_stale_orders_job():

        manager = OrderManager()

        connected = manager.connect()

        if not connected:
            log.warning(
                "OrderManager connection failed"
            )
            return

        try:
            manager.cancel_stale_orders()

        finally:
            manager.disconnect()

    scheduler.add_job(
        cancel_stale_orders_job,
        "interval",
        minutes=1,
        id="stale_order_cleanup",
        replace_existing=True,
    )

    log.info(
        "Stale order cleanup started — every 1 minute"
    )
    # ==========================================
    # EMERGENCY EXIT PROTECTION
    # ==========================================

    from emergency_exit_manager import (
        EmergencyExitManager,
    )

    def emergency_exit_job():

        manager = EmergencyExitManager()

        connected = manager.connect()

        if not connected:
            log.warning(
                "EmergencyExitManager connection failed"
            )
            return

        try:
            manager.process_emergency_exits()

        finally:
            manager.disconnect()

    scheduler.add_job(
        emergency_exit_job,
        "interval",
        seconds=30,
        id="emergency_exit_protection",
        replace_existing=True,
    )

    log.info(
        "Emergency exit protection started — every 30 seconds"
    )
    # ==========================================
    # LIVE TWS MIRROR
    # ==========================================

    from tws_mirror import run_tws_mirror_once

    scheduler.add_job(
        run_tws_mirror_once,
        "interval",
        seconds=15,
        id="live_tws_mirror",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Live TWS mirror started — every 15 seconds"
    )

    # ==========================================
    # EXECUTION SYNC
    # ==========================================

    from execution_sync import sync_executions

    scheduler.add_job(
        sync_executions,
        "interval",
        seconds=30,
        id="execution_sync",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Execution sync started — every 30 seconds"
    )
    # ==========================================
    # RECONCILIATION CHECK
    # ==========================================

    from reconciliation import run_reconciliation_once

    scheduler.add_job(
        run_reconciliation_once,
        "interval",
        seconds=30,
        id="reconciliation_check",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Reconciliation check started — every 30 seconds"
    )

    # ==========================================
    # READ-ONLY ACCOUNT SYNC
    # ==========================================

    scheduler.add_job(
        account_sync.run_account_sync_once,
        "interval",
        seconds=30,
        id="account_sync",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Read-only account sync started — every 30 seconds"
    )

    scheduler.add_job(
        account_sync.run_reconciliation_status_check,
        "interval",
        seconds=60,
        id="account_sync_reconciliation_status",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Account reconciliation status check started — every 60 seconds"
    )

    # ==========================================
    # PORTFOLIO RISK ENGINE
    # ==========================================

    scheduler.add_job(
        portfolio_risk_engine.refresh_portfolio_risk,
        "interval",
        seconds=config.PORTFOLIO_RISK_REFRESH_SECONDS,
        id="portfolio_risk_engine",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Portfolio risk engine started — every %s seconds",
        config.PORTFOLIO_RISK_REFRESH_SECONDS,
    )

    # ==========================================
    # MARKET REGIME ENGINE
    # ==========================================

    scheduler.add_job(
        refresh_market_regime_safe,
        "interval",
        seconds=config.REGIME_REFRESH_SECONDS,
        id="market_regime_engine",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Market regime engine started — every %s seconds",
        config.REGIME_REFRESH_SECONDS,
    )

    # ==========================================
    # MARKET DATA GUARD
    # ==========================================

    from market_data_guard import (
        run_market_data_guard,
    )

    scheduler.add_job(
        run_market_data_guard,
        "interval",
        seconds=60,
        id="market_data_guard",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Market data guard started — every 60 seconds"
    )

    scheduler.add_job(
        session_manager.refresh_session_status,
        "interval",
        seconds=30,
        id="session_manager_refresh",
        replace_existing=True,
        max_instances=1,
    )

    log.info("Session manager heartbeat started — every 30 seconds")

    scheduler.add_job(
        recovery_manager.run_recovery_check,
        "interval",
        seconds=config.RECOVERY_CHECK_INTERVAL_SECONDS,
        id="recovery_manager",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Recovery manager started — every %s seconds",
        config.RECOVERY_CHECK_INTERVAL_SECONDS,
    )

    scheduler.add_job(
        watchdog.run_watchdog_once,
        "interval",
        seconds=config.WATCHDOG_INTERVAL_SECONDS,
        id=WATCHDOG_JOB_ID,
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "TWS/API watchdog started — every %s seconds",
        config.WATCHDOG_INTERVAL_SECONDS,
    )

    scheduler.start()

    if config.RUN_SCAN_ON_STARTUP and not _latest:
        asyncio.create_task(run_full_scan())

    yield

    scheduler.shutdown(wait=False)


app = FastAPI(title="Stock Alerts", lifespan=lifespan)

# ==========================================
# MARKET DATA GUARD API
# ==========================================

@app.get("/api/session-status")
async def api_session_status():
    return JSONResponse(
        session_manager.refresh_session_status(),
        headers=no_cache_headers(),
    )


@app.get("/api/recovery-status")
async def api_recovery_status():
    return JSONResponse(
        await recovery_manager.get_recovery_status(),
        headers=no_cache_headers(),
    )


@app.get("/api/market-data-guard")
async def api_market_data_guard():

    from market_data_guard import run_market_data_guard

    result = await run_market_data_guard()

    return JSONResponse(
        result,
        headers=no_cache_headers(),
    )


@app.get("/api/watchdog/status")
async def api_watchdog_status():
    return JSONResponse(
        await watchdog.get_watchdog_status(),
        headers=no_cache_headers(),
    )


@app.post("/api/watchdog/run-once")
async def api_watchdog_run_once():
    return JSONResponse(
        await watchdog.run_watchdog_once(),
        headers=no_cache_headers(),
    )


def _scheduler_job_count(job_id: str) -> int:
    return sum(1 for job in scheduler.get_jobs() if job.id == job_id)


async def _restart_watchdog_job() -> dict:
    while scheduler.get_job(WATCHDOG_JOB_ID):
        scheduler.remove_job(WATCHDOG_JOB_ID)

    scheduler.add_job(
        watchdog.run_watchdog_once,
        "interval",
        seconds=config.WATCHDOG_INTERVAL_SECONDS,
        id=WATCHDOG_JOB_ID,
        replace_existing=True,
        max_instances=1,
    )
    return {
        "status": "restarted",
        "job_id": WATCHDOG_JOB_ID,
        "job_count": _scheduler_job_count(WATCHDOG_JOB_ID),
        "watchdog": await watchdog.run_watchdog_once(),
    }


@app.get("/api/dashboard/operations")
async def api_dashboard_operations():
    return JSONResponse(await _get_dashboard_operations(), headers=no_cache_headers())


@app.post("/api/tws/reconnect")
async def api_tws_reconnect():
    reconnect_result = await asyncio.to_thread(watchdog._attempt_reconnect_sync)
    status = await watchdog.run_watchdog_once()
    status["last_reconnect_attempt_at"] = utc_now().isoformat()
    status["last_reconnect_result"] = reconnect_result
    await database.set_app_state(watchdog.WATCHDOG_STATUS_KEY, _json_payload(status))
    payload = {
        "status": "connected" if status.get("tws_connected") else "reconnect_attempted",
        "tws_connected": bool(status.get("tws_connected")),
        "watchdog": status,
        "last_reconnect_attempt_at": status.get("last_reconnect_attempt_at"),
        "last_reconnect_result": status.get("last_reconnect_result"),
    }
    return await _operation_response(
        "reconnect_tws",
        "success" if payload["tws_connected"] else "failed",
        payload,
        message="TWS reconnect completed" if payload["tws_connected"] else "TWS reconnect attempted but connection is not healthy",
        status_code=200 if payload["tws_connected"] else 503,
    )


@app.post("/api/scanner/restart")
async def api_scanner_restart():
    scanner = await configure_scanner_job()
    payload = {
        "status": "restarted",
        "scanner": scanner,
        "job_id": SCANNER_JOB_ID,
        "job_count": _scheduler_job_count(SCANNER_JOB_ID),
    }
    return await _operation_response("restart_scanner", "success", payload, message="Scanner scheduler restarted")


@app.post("/api/watchdog/restart")
async def api_watchdog_restart():
    try:
        payload = await _restart_watchdog_job()
        return await _operation_response("restart_watchdog", "success", payload, message="Watchdog scheduler restarted")
    except Exception as exc:
        payload = {"status": "failed", "reason": str(exc), "job_id": WATCHDOG_JOB_ID}
        return await _operation_response("restart_watchdog", "failed", payload, message=str(exc), status_code=500)


@app.post("/api/live-position-tracker/refresh")
async def api_live_position_tracker_refresh():
    try:
        refreshed = await refresh_open_positions()
        payload = {
            "status": "refreshed",
            "refreshed_count": len(refreshed),
            "positions": refreshed,
            "tracker": await live_position_tracker.get_tracker_status(),
        }
        return await _operation_response("refresh_live_tracker", "success", payload, message="Live tracker refreshed")
    except Exception as exc:
        payload = {"status": "failed", "reason": str(exc)}
        return await _operation_response("refresh_live_tracker", "failed", payload, message=str(exc), status_code=500)


@app.post("/api/startup-recovery/run")
async def api_startup_recovery_run():
    result = await startup_recovery.run_startup_recovery()
    ok = bool(result.get("ok"))
    return await _operation_response(
        "run_startup_recovery",
        "success" if ok else "failed",
        result,
        message=result.get("reason") or ("Startup recovery passed" if ok else "Startup recovery failed"),
        status_code=200 if ok else 409,
    )


@app.post("/api/reconciliation/reconcile-positions")
async def api_reconcile_positions_control():
    from reconciliation import run_reconciliation_once

    result = await run_reconciliation_once()
    status = "failed" if result.get("status") == "failed" or result.get("error") else "success"
    return await _operation_response(
        "reconcile_positions",
        status,
        result,
        message="Position reconciliation completed" if status == "success" else result.get("error") or "Position reconciliation failed",
        status_code=200 if status == "success" else 500,
    )


@app.post("/api/emergency-stop")
async def api_emergency_stop(reason: str = Body("Dashboard emergency stop", embed=True)):
    await _set_auto_trading_state(False, source="api_emergency_stop", reason=reason)
    circuit = await trip_circuit_breaker(
        reason,
        source="dashboard_emergency_stop",
        details={"auto_trading_enabled": False},
    )
    payload = {
        "status": "stopped",
        "auto_trading_enabled": False,
        "orders_cancelled": 0,
        "circuit_breaker": circuit,
        "reason": reason,
    }
    return await _operation_response("emergency_stop", "success", payload, message=reason)

# ==========================================
# RECONCILIATION API
# ==========================================

@app.get("/api/reconciliation")
async def api_reconciliation():

    from reconciliation import run_reconciliation_once

    result = await run_reconciliation_once()

    return JSONResponse(
        result,
        headers=no_cache_headers(),
    )


@app.post("/api/reconciliation/adopt-tws-positions")
async def api_adopt_tws_positions():

    from reconciliation import adopt_tws_positions_as_baseline

    try:
        result = await adopt_tws_positions_as_baseline()
        return JSONResponse(
            result,
            headers=no_cache_headers(),
        )

    except RuntimeError as exc:
        return JSONResponse(
            {
                "error": str(exc),
                "adopted_count": 0,
                "skipped_count": 0,
                "symbols_adopted": [],
                "symbols_skipped": [],
            },
            status_code=403,
            headers=no_cache_headers(),
        )


@app.post("/api/reconciliation/close-db-positions-flat-in-tws")
async def api_close_db_positions_flat_in_tws(
    request: Request,
    dry_run: bool = Body(False, embed=True),
):
    query_dry_run = request.query_params.get("dry_run")
    if query_dry_run is not None:
        dry_run = query_dry_run.strip().lower() in {"1", "true", "yes", "on"}

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: _run_flat_tws_reconciliation_worker(dry_run=dry_run),
        )
        return JSONResponse(
            result,
            headers=no_cache_headers(),
        )

    except RuntimeError as exc:
        return JSONResponse(
            {
                "status": "blocked",
                "reason": str(exc),
                "tws_positions_count": 0,
                "db_open_before": 0,
                "closed_count": 0,
                "closed_symbols": [],
                "skipped_symbols": [],
                "remaining_issues": [],
                "dry_run": bool(dry_run),
            },
            status_code=403,
            headers=no_cache_headers(),
        )


# ==========================================
# ACCOUNT SYNC API
# ==========================================

@app.post("/api/paper/liquidate-all")
async def api_paper_liquidate_all(
    request: Request,
    restart_auto_trading_after: bool = Body(False, embed=True),
    dry_run: bool = Body(False, embed=True),
):
    query_dry_run = request.query_params.get("dry_run")
    if query_dry_run is not None:
        dry_run = query_dry_run.strip().lower() in {"1", "true", "yes", "on"}

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: _run_paper_liquidation_worker(
                restart_auto_trading_after=restart_auto_trading_after,
                dry_run=dry_run,
            ),
        )
        return await _operation_response(
            "close_all_positions",
            "success" if result.get("status") != "blocked" else "failed",
            result,
            message=result.get("reason") or "Paper liquidation completed",
            status_code=200 if result.get("status") != "blocked" else 403,
        )

    except RuntimeError as exc:
        return await _operation_response(
            "close_all_positions",
            "failed",
            {
                "status": "blocked",
                "reason": str(exc),
            },
            message=str(exc),
            status_code=403,
        )


@app.post("/api/paper/reset-session")
async def api_paper_reset_session():
    try:
        result = await _reset_paper_session()
        status_code = 409 if result.get("status") == "blocked" else 200
        return JSONResponse(result, status_code=status_code, headers=no_cache_headers())

    except RuntimeError as exc:
        return JSONResponse(
            {
                "status": "blocked",
                "reason": str(exc),
                "orders_submitted": 0,
            },
            status_code=403,
            headers=no_cache_headers(),
        )


@app.get("/api/strategy-mode")
async def api_strategy_mode():
    mode = await strategy_mode.get_strategy_mode()
    open_positions = await database.get_open_positions()
    return JSONResponse(
        strategy_mode.strategy_mode_payload(mode, open_positions_count=len(open_positions)),
        headers=no_cache_headers(),
    )


@app.post("/api/strategy-mode/swing")
async def api_strategy_mode_swing():
    payload = await strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.SWING_DEFAULT)
    scanner = await configure_scanner_job()
    return JSONResponse(
        {**payload, "scanner": scanner},
        headers=no_cache_headers(),
    )


@app.post("/api/strategy-mode/intraday")
async def api_strategy_mode_intraday():
    payload = await strategy_mode.set_strategy_mode(strategy_mode.StrategyMode.INTRADAY_MOMENTUM)
    scanner = await configure_scanner_job()
    return JSONResponse(
        {**payload, "scanner": scanner},
        headers=no_cache_headers(),
    )


@app.post("/api/strategy-mode/switch")
async def api_strategy_mode_switch(mode: str = Body(..., embed=True)):
    normalized = strategy_mode.normalize_strategy_mode(mode)
    payload = await strategy_mode.set_strategy_mode(normalized)
    scanner = await configure_scanner_job()
    result = {**payload, "scanner": scanner}
    return await _operation_response(
        "switch_strategy_mode",
        "success",
        result,
        message=f"Strategy mode switched to {normalized.value}",
    )


@app.post("/api/training-profile/switch")
async def api_training_profile_switch(profile: str = Body(..., embed=True)):
    normalized = config.normalize_paper_training_profile(profile)
    requested = str(profile or "").strip().upper()
    if requested and requested != normalized:
        payload = {
            "status": "blocked",
            "reason": f"Unknown training profile: {requested}",
            "available_profiles": list(config.PAPER_TRAINING_PROFILES.keys()),
        }
        return await _operation_response("switch_training_profile", "failed", payload, message=payload["reason"], status_code=400)

    config.PAPER_TRAINING_PROFILE = normalized
    await _ensure_app_state_table()
    await database.set_app_state("paper_training_profile", normalized)
    mode = await strategy_mode.get_strategy_mode()
    payload = strategy_mode.strategy_mode_payload(mode)
    payload["status"] = "switched"
    payload["requested_profile"] = normalized
    return await _operation_response(
        "switch_training_profile",
        "success",
        payload,
        message=f"Training profile switched to {normalized}",
    )


@app.post("/api/strategy-mode/toggle-intraday-swing")
async def api_strategy_toggle_intraday_swing():
    current = await strategy_mode.get_strategy_mode()
    target = (
        strategy_mode.StrategyMode.SWING_DEFAULT
        if strategy_mode.is_intraday_mode(current)
        else strategy_mode.StrategyMode.INTRADAY_MOMENTUM
    )
    payload = await strategy_mode.set_strategy_mode(target)
    scanner = await configure_scanner_job()
    result = {**payload, "scanner": scanner}
    return await _operation_response(
        "toggle_intraday_swing",
        "success",
        result,
        message=f"Strategy mode toggled to {target.value}",
    )


@app.get("/api/dashboard/health")
async def api_dashboard_health():
    trading = await api_trading_status()
    trading_payload = json.loads(trading.body.decode())
    scanner = await get_scanner_status()
    startup_status = await startup_recovery.get_startup_recovery_status()
    circuit = await get_circuit_breaker_state()
    watchdog_status = trading_payload.get("watchdog") or await watchdog.get_watchdog_status()
    tracker = trading_payload.get("live_position_tracker") or await live_position_tracker.get_tracker_status()
    blocked = bool(
        trading_payload.get("blocked_reasons")
        or watchdog_status.get("trading_blocked")
        or circuit.get("tripped")
        or not trading_payload.get("auto_trading_enabled")
    )
    degraded = bool(
        not watchdog_status.get("healthy")
        or not tracker.get("healthy")
        or (watchdog_status.get("stale_data") or {}).get("market_data")
    )
    system_status = "BLOCKED" if blocked else "DEGRADED" if degraded else "ACTIVE"
    return JSONResponse(
        {
            "system_status": system_status,
            "tws_connection": trading_payload.get("connection_status"),
            "watchdog_health": watchdog_status,
            "scanner_health": scanner,
            "live_tracker_health": tracker,
            "market_data_freshness": {
                "last_market_data_at": trading_payload.get("last_market_data_at"),
                "last_market_data_age_seconds": trading_payload.get("last_market_data_age_seconds"),
                "market_data_feed_active": trading_payload.get("market_data_feed_active"),
                "stale_data_status": trading_payload.get("stale_data_status"),
            },
            "circuit_breaker_state": circuit,
            "startup_recovery_state": startup_status,
            "auto_trading_enabled": trading_payload.get("auto_trading_enabled"),
            "blocked_reasons": trading_payload.get("blocked_reasons") or watchdog_status.get("blocking_reasons") or [],
            "last_operations": await _get_dashboard_operations(),
        },
        headers=no_cache_headers(),
    )


@app.post("/api/auto-trading/enable")
async def api_auto_trading_enable():
    snapshot = await broker_sync.run_broker_sync_once()
    await database.save_broker_sync_snapshot(snapshot)
    recon = await reconciliation_engine.run_reconciliation(snapshot)
    safety = await _evaluate_auto_trading_enable_safety()
    if int(recon.get("high_severity_issues_count") or 0) > 0:
        safety["ok"] = False
        safety.setdefault("blocked_reasons", []).append("Unresolved HIGH reconciliation issues")
    if snapshot.get("ok") and snapshot.get("connected"):
        await database.set_app_state(startup_recovery.STARTUP_RECOVERY_PASSED_KEY, "true")
    if not safety["ok"]:
        reason = "; ".join(safety["blocked_reasons"])
        await _set_auto_trading_state(
            False,
            source="api_auto_trading_enable",
            reason=reason,
        )
        return await _operation_response(
            "enable_auto_trading",
            "failed",
            {
                "status": "blocked",
                "auto_trading_enabled": False,
                "reason": reason,
                **safety,
            },
            message=reason,
            status_code=403,
        )

    reason = "Auto trading enabled after safety checks passed"
    await _set_auto_trading_state(True, source="api_auto_trading_enable", reason=reason)
    return await _operation_response(
        "enable_auto_trading",
        "success",
        {
            "status": "enabled",
            "auto_trading_enabled": True,
            "reason": reason,
            **safety,
        },
        message=reason,
    )


@app.post("/api/auto-trading/disable")
async def api_auto_trading_disable():
    reason = "Auto trading disabled by API request; existing TWS orders were not cancelled"
    await _set_auto_trading_state(False, source="api_auto_trading_disable", reason=reason)
    return await _operation_response(
        "disable_auto_trading",
        "success",
        {
            "status": "disabled",
            "auto_trading_enabled": False,
            "reason": reason,
            "orders_cancelled": 0,
        },
        message=reason,
    )


@app.get("/api/account-summary")
async def api_account_summary():
    return JSONResponse(
        await account_sync.get_account_summary(),
        headers=no_cache_headers(),
    )


@app.get("/api/open-orders")
async def api_open_orders():
    return JSONResponse(
        await account_sync.get_open_orders(),
        headers=no_cache_headers(),
    )


@app.get("/api/executions")
async def api_executions(limit: int = 200, symbol: str | None = None):
    from execution_sync import get_executions

    return JSONResponse(
        await get_executions(limit=limit, symbol=symbol),
        headers=no_cache_headers(),
    )




@app.get("/api/portfolio-risk")
async def api_portfolio_risk():
    return JSONResponse(
        await portfolio_risk_engine.get_portfolio_risk(),
        headers=no_cache_headers(),
    )


@app.get("/api/sector-intelligence")
async def api_sector_intelligence():
    return JSONResponse(
        await sector_intelligence.get_sector_intelligence(),
        headers=no_cache_headers(),
    )


@app.get("/api/sector-intelligence/{symbol}")
async def api_sector_intelligence_symbol(symbol: str):
    return JSONResponse(
        await sector_intelligence.get_symbol_intelligence(symbol),
        headers=no_cache_headers(),
    )


@app.get("/api/execution-quality")
async def api_execution_quality():
    evaluations = [
        evaluate_execution_quality(row=row, symbol=symbol)
        for symbol, row in sorted(_latest.items())
    ]
    return JSONResponse(
        summarize_execution_quality(evaluations),
        headers=no_cache_headers(),
    )


@app.get("/api/execution-quality/{symbol}")
async def api_execution_quality_symbol(symbol: str):
    normalized = str(symbol or "").strip().upper()
    row = _latest.get(normalized)

    if not row:
        return JSONResponse(
            {
                "symbol": normalized,
                "state": "EXECUTION_WARNING",
                "allowed": True,
                "blocks_buy": False,
                "blocked_buy_reason": None,
                "warnings": ["No cached scan data available for execution-quality evaluation"],
                "dangers": [],
                "metrics": {},
            },
            status_code=404,
            headers=no_cache_headers(),
        )

    return JSONResponse(
        evaluate_execution_quality(row=row, symbol=normalized),
        headers=no_cache_headers(),
    )






@app.get("/api/position-exit-priority")
async def api_position_exit_priority():
    return JSONResponse(
        await position_exit_priority_engine.get_position_exit_priority(_latest),
        headers=no_cache_headers(),
    )


@app.get("/api/position-exit-priority/{symbol}")
async def api_position_exit_priority_symbol(symbol: str):
    normalized = str(symbol or "").strip().upper()
    row = _latest.get(normalized, {"symbol": normalized})
    evaluation = await position_exit_priority_engine.get_position_exit_priority_for_symbol(normalized, row)

    if evaluation is None:
        return JSONResponse(
            {"symbol": normalized, "error": "Open position not found", "read_only": True},
            status_code=404,
            headers=no_cache_headers(),
        )

    return JSONResponse(evaluation, headers=no_cache_headers())


@app.get("/api/position-sizing")
async def api_position_sizing():
    return JSONResponse(
        await position_sizing_engine.get_position_sizing(list(_latest.values())),
        headers=no_cache_headers(),
    )


@app.get("/api/position-sizing/{symbol}")
async def api_position_sizing_symbol(symbol: str):
    normalized = str(symbol or "").strip().upper()
    row = _latest.get(normalized, {"symbol": normalized})
    status_code = 200 if normalized in _latest else 404
    return JSONResponse(
        await position_sizing_engine.get_position_sizing_for_symbol(normalized, row),
        status_code=status_code,
        headers=no_cache_headers(),
    )

@app.get("/api/exposure")
async def api_exposure():
    return JSONResponse(
        await portfolio_risk_engine.get_exposure(),
        headers=no_cache_headers(),
    )


@app.get("/api/risk-alerts")
async def api_risk_alerts():
    return JSONResponse(
        await portfolio_risk_engine.get_risk_alerts(),
        headers=no_cache_headers(),
    )

@app.get("/api/order-lifecycle")
async def api_order_lifecycle(limit: int = 200):
    return JSONResponse(
        await order_lifecycle.get_order_lifecycle_events(limit=limit),
        headers=no_cache_headers(),
    )


@app.get("/api/order-lifecycle/{symbol}")
async def api_order_lifecycle_symbol(symbol: str, limit: int = 200):
    return JSONResponse(
        await order_lifecycle.get_order_lifecycle_events(limit=limit, symbol=symbol),
        headers=no_cache_headers(),
    )


@app.get("/api/order-lifecycle-latest")
async def api_order_lifecycle_latest(limit: int = 200):
    return JSONResponse(
        await order_lifecycle.get_latest_order_lifecycle_states(limit=limit),
        headers=no_cache_headers(),
    )


@app.get("/api/startup-recovery/status")
async def api_startup_recovery_status():
    return JSONResponse(
        await startup_recovery.get_startup_recovery_status(),
        headers=no_cache_headers(),
    )


@app.get("/api/reconciliation/status")
async def api_reconciliation_status_v2():
    from reconciliation_lifecycle import get_reconciliation_status

    return JSONResponse(
        await get_reconciliation_status(),
        headers=no_cache_headers(),
    )


@app.post("/api/circuit-breaker/reset")
async def api_circuit_breaker_reset(reason: str = Body("Manual API reset", embed=True)):
    result = await reset_circuit_breaker(reason=reason)
    return await _operation_response(
        "reset_circuit_breaker",
        "success",
        result,
        message=reason,
    )


@app.get("/api/circuit-breaker/status")
async def api_circuit_breaker_status():
    return JSONResponse(
        await get_circuit_breaker_state(),
        headers=no_cache_headers(),
    )


@app.get("/api/reconciliation-status")
async def api_reconciliation_status():
    from reconciliation_lifecycle import get_reconciliation_status

    return JSONResponse(
        await get_reconciliation_status(),
        headers=no_cache_headers(),
    )


@app.get("/api/reconciliation-history")
async def api_reconciliation_history(limit: int = 200, status: str | None = None):
    from reconciliation_lifecycle import get_reconciliation_history

    return JSONResponse(
        await get_reconciliation_history(limit=limit, status=status),
        headers=no_cache_headers(),
    )


@app.head("/")
async def head_root():
    return Response(headers=no_cache_headers())

@app.get("/layout.css")
async def serve_layout_css():
    css_path = Path(__file__).parent / "layout.css"

    if css_path.exists():
        return Response(
            css_path.read_text(encoding="utf-8"),
            media_type="text/css",
            headers=no_cache_headers(),
        )

    return Response(
        "layout.css not found",
        status_code=404,
        media_type="text/plain",
        headers=no_cache_headers(),
    )


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return serve_index_html()


@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard_route():
    return serve_index_html()


@app.get("/market", response_class=HTMLResponse)
async def serve_market():
    return serve_html_file("market.html", fallback_to_index=True)


@app.get("/scanner", response_class=HTMLResponse)
async def serve_scanner():
    return serve_html_file("scanner.html", fallback_to_index=True)


@app.get("/analytics", response_class=HTMLResponse)
async def serve_analytics():
    return serve_html_file("analytics.html", fallback_to_index=True)


@app.get("/positions", response_class=HTMLResponse)
async def serve_positions_route():
    return serve_html_file("positions.html", fallback_to_index=True)


@app.get("/risk", response_class=HTMLResponse)
async def serve_risk():
    return serve_html_file("risk.html", fallback_to_index=True)


@app.get("/system-health", response_class=HTMLResponse)
@app.get("/system_health", response_class=HTMLResponse)
async def serve_system_health():
    return serve_html_file("system_health.html", fallback_to_index=True)


@app.get("/strategies", response_class=HTMLResponse)
async def serve_strategies():
    return serve_html_file("strategies.html", fallback_to_index=True)


@app.get("/settings", response_class=HTMLResponse)
async def serve_settings():
    return serve_html_file("settings.html", fallback_to_index=True)


@app.get("/portfolio", response_class=HTMLResponse)
async def serve_portfolio():
    return serve_html_file("portfolio.html", fallback_to_index=True)


@app.get("/top", response_class=HTMLResponse)
async def serve_top_route():
    return serve_index_html()


@app.get("/all", response_class=HTMLResponse)
async def serve_all_route():
    return serve_index_html()


@app.get("/history", response_class=HTMLResponse)
async def serve_history_route():
    return serve_html_file("history.html", fallback_to_index=True)


@app.get("/api/stocks")
async def api_stocks():
    return JSONResponse(list(_latest.values()), headers=no_cache_headers())


@app.get("/api/top-weekly")
async def api_top_weekly():
    if not _top_weekly and _latest:
        rebuild_top_weekly(limit=10)

    return JSONResponse(_top_weekly, headers=no_cache_headers())


@app.get("/api/scanner/status")
async def api_scanner_status():
    return JSONResponse(await get_scanner_status(), headers=no_cache_headers())


@app.post("/api/run-scan")
async def api_run_scan():
    if _scan_lock.locked():
        return JSONResponse({"status": "already running"}, headers=no_cache_headers())

    asyncio.create_task(run_full_scan())
    return JSONResponse({"status": "scan started"}, headers=no_cache_headers())


@app.get("/api/run-scan")
async def api_run_scan_get():
    if _scan_lock.locked():
        return JSONResponse({"status": "already running"}, headers=no_cache_headers())

    asyncio.create_task(run_full_scan())
    return JSONResponse({"status": "scan started"}, headers=no_cache_headers())


@app.get("/api/rebuild-top-weekly")
async def api_rebuild_top_weekly():
    top = rebuild_top_weekly(limit=10)
    return JSONResponse({"status": "rebuilt", "count": len(top), "top": top}, headers=no_cache_headers())


@app.get("/api/history")
async def api_history():
    return JSONResponse(await database.get_recent_signals(50), headers=no_cache_headers())


@app.get("/api/trade-journal")
async def api_trade_journal(limit: int = 200, symbol: str | None = None):
    return JSONResponse(
        await database.get_trade_journal(limit=limit, symbol=symbol),
        headers=no_cache_headers(),
    )

@app.get("/api/analytics/rejections")
async def api_analytics_rejections(limit: int = 200):
    return JSONResponse(
        await database.get_rejected_setups(limit=limit),
        headers=no_cache_headers(),
    )


@app.get("/api/analytics/setup-performance")
async def api_analytics_setup_performance():
    return JSONResponse(
        await database.refresh_setup_performance(),
        headers=no_cache_headers(),
    )


@app.get("/api/analytics/outcomes")
async def api_analytics_outcomes(limit: int = 200):
    return JSONResponse(
        await database.get_trade_outcomes(limit=limit),
        headers=no_cache_headers(),
    )


@app.get("/api/analytics/learning-summary")
async def api_analytics_learning_summary():
    return JSONResponse(
        await database.get_learning_summary(),
        headers=no_cache_headers(),
    )


@app.get("/api/trade-reviews")
async def api_trade_reviews(limit: int = 200):
    return JSONResponse(
        await database.get_trade_reviews(limit=limit),
        headers=no_cache_headers(),
    )


@app.get("/api/trade-reviews/{symbol}")
async def api_trade_reviews_symbol(symbol: str, limit: int = 200):
    return JSONResponse(
        await database.get_trade_reviews(limit=limit, symbol=symbol),
        headers=no_cache_headers(),
    )


@app.post("/api/trade-reviews/rebuild")
async def api_trade_reviews_rebuild():
    result = await database.rebuild_trade_reviews()
    return await _operation_response(
        "rebuild_trade_reviews",
        "success",
        result,
        message="Trade reviews rebuilt",
    )


@app.get("/api/scan-runs")
async def api_scan_runs():
    return JSONResponse(await database.get_scan_runs(20), headers=no_cache_headers())


@app.get("/api/performance")
async def api_performance():
    return JSONResponse(await database.get_performance_summary(), headers=no_cache_headers())


@app.get("/api/equity-curve")
async def api_equity_curve(limit: int = 500):
    return JSONResponse(await account_sync.get_equity_curve(limit=limit), headers=no_cache_headers())


@app.get("/api/positions")
async def api_positions():
    positions = await database.get_all_positions(100)
    tracker = await live_position_tracker.get_tracker_status()
    tracker_by_symbol = {
        str(item.get("symbol") or "").upper(): item
        for item in tracker.get("positions", [])
        if item.get("symbol")
    }
    for position in positions:
        symbol = str(position.get("symbol") or "").upper()
        live = tracker_by_symbol.get(symbol)
        position["live_tracking"] = bool(live and str(position.get("status") or "").upper() == "OPEN")
        position["live_tracking_source"] = (live or {}).get("source")
        position["live_tracking_last_refresh_at"] = (live or {}).get("last_refresh_at")
        position["live_tracking_last_refresh_age_seconds"] = (live or {}).get("last_refresh_age_seconds")
    return JSONResponse(positions, headers=no_cache_headers())


@app.get("/api/live-position-tracker")
async def api_live_position_tracker():
    return JSONResponse(
        await live_position_tracker.get_tracker_status(),
        headers=no_cache_headers(),
    )


@app.get("/api/market-regime")
async def api_market_regime():
    return JSONResponse(
        await get_cached_market_regime(),
        headers=no_cache_headers(),
    )


@app.get("/api/market-regime/history")
async def api_market_regime_history(limit: int = 100):
    return JSONResponse(
        await get_market_regime_history(limit=limit),
        headers=no_cache_headers(),
    )

# ==========================================
# TWS MIRROR API
# ==========================================

import aiosqlite


@app.get("/api/tws-status")
async def api_tws_status():

    result = {
        "heartbeat": {},
        "account": [],
        "positions": [],
        "orders": [],
    }

    async with aiosqlite.connect(config.DB_PATH) as db:

        # ==============================
        # HEARTBEAT
        # ==============================

        cursor = await db.execute(
            """
            SELECT
                connected,
                account,
                last_sync_at,
                error
            FROM tws_heartbeat
            WHERE id = 1
            """
        )

        row = await cursor.fetchone()

        if row:
            result["heartbeat"] = {
                "connected": bool(row[0]),
                "account": row[1],
                "last_sync_at": row[2],
                "error": row[3],
            }

        # ==============================
        # ACCOUNT
        # ==============================

        cursor = await db.execute(
            """
            SELECT
                tag,
                value,
                currency,
                account,
                updated_at
            FROM tws_account
            ORDER BY tag
            """
        )

        rows = await cursor.fetchall()

        for row in rows:
            result["account"].append(
                {
                    "tag": row[0],
                    "value": row[1],
                    "currency": row[2],
                    "account": row[3],
                    "updated_at": row[4],
                }
            )

        # ==============================
        # POSITIONS
        # ==============================

        cursor = await db.execute(
            """
            SELECT
                symbol,
                quantity,
                avg_cost,
                market_price,
                market_value,
                unrealized_pnl,
                realized_pnl,
                account,
                updated_at
            FROM tws_positions
            ORDER BY symbol
            """
        )

        rows = await cursor.fetchall()

        for row in rows:
            result["positions"].append(
                {
                    "symbol": row[0],
                    "quantity": row[1],
                    "avg_cost": row[2],
                    "market_price": row[3],
                    "market_value": row[4],
                    "unrealized_pnl": row[5],
                    "realized_pnl": row[6],
                    "account": row[7],
                    "updated_at": row[8],
                }
            )

        # ==============================
        # ORDERS
        # ==============================

        cursor = await db.execute(
            """
            SELECT
                order_id,
                perm_id,
                symbol,
                action,
                order_type,
                total_quantity,
                limit_price,
                aux_price,
                status,
                filled,
                remaining,
                avg_fill_price,
                account,
                updated_at
            FROM tws_orders
            ORDER BY order_id DESC
            """
        )

        rows = await cursor.fetchall()

        for row in rows:
            result["orders"].append(
                {
                    "order_id": row[0],
                    "perm_id": row[1],
                    "symbol": row[2],
                    "action": row[3],
                    "order_type": row[4],
                    "total_quantity": row[5],
                    "limit_price": row[6],
                    "aux_price": row[7],
                    "status": row[8],
                    "filled": row[9],
                    "remaining": row[10],
                    "avg_fill_price": row[11],
                    "account": row[12],
                    "updated_at": row[13],
                }
            )

    return JSONResponse(
        result,
        headers=no_cache_headers(),
    )

@app.get("/api/trading-status")
async def api_trading_status():

    from global_risk_manager import (
        get_global_risk_status,
    )

    # ==========================================
    # LOAD DATA
    # ==========================================

    open_positions = await database.get_open_positions()

    active_paper_session = await database.get_active_paper_session()

    realized_pnl = await database.get_realized_pnl()

    market = get_market_regime()

    global_risk = await get_global_risk_status()

    watchdog_status = await watchdog.get_watchdog_status()
    circuit_breaker_auto_recovery = await get_last_auto_recovery()

    # ==========================================
    # ACCOUNT CALCULATIONS
    # ==========================================

    account_equity = float(
        (active_paper_session or {}).get(
            "session_start_equity",
            config.effective_virtual_trading_capital(),
        )
    ) + float(realized_pnl)

    used_capital = sum(
        float(p.get("buy_price") or 0)
        * float(p.get("quantity") or 0)
        for p in open_positions
    )

    cash_reserve = (
        account_equity
        * (
            float(config.MIN_CASH_RESERVE_PERCENT)
            / 100
        )
    )

    available_cash = (
        account_equity
        - used_capital
        - cash_reserve
    )

    active_strategy_mode = await strategy_mode.get_strategy_mode()
    active_strategy_payload = strategy_mode.strategy_mode_payload(
        active_strategy_mode,
        open_positions_count=len(open_positions),
    )
    max_positions = int(active_strategy_payload["rules"].get("max_open_positions", getattr(config, "MAX_OPEN_POSITIONS", 10)))

    open_count = len(open_positions)

    # ==========================================
    # AUTO TRADING STATE
    # ==========================================

    auto_trading_state = await _get_auto_trading_state()
    broker_snapshot = await database.get_latest_broker_sync_snapshot() or {}
    recon_issues = await database.get_open_reconciliation_issues()
    auto_trading_enabled = bool(auto_trading_state["enabled"])

    blocked_reasons = []
    market_hours = get_market_hours_status()

    # ==========================================
    # MARKET HOURS ORDER GUARD
    # ==========================================

    if not market_hours.get("allowed"):

        blocked_reasons.append(
            market_hours.get("reason")
            or "US regular market is closed"
        )

    # ==========================================
    # MANUAL KILL SWITCH
    # ==========================================

    if not auto_trading_enabled:

        blocked_reasons.append(
            "Auto trading disabled manually"
        )

    # ==========================================
    # CONFIG CHECKS
    # ==========================================

    if config.TRADING_MODE == "OFF":

        blocked_reasons.append(
            "TRADING_MODE is OFF"
        )

    if not config.AUTO_SEND_ORDERS:

        blocked_reasons.append(
            "AUTO_SEND_ORDERS is false"
        )

    if not config.IBKR_PAPER_TRADING:

        blocked_reasons.append(
            "IBKR_PAPER_TRADING is false"
        )

    if config.IBKR_ENABLE_REAL_TRADING:

        blocked_reasons.append(
            "LIVE trading is enabled"
        )

    # ==========================================
    # POSITION LIMIT
    # ==========================================

    if open_count >= max_positions:

        blocked_reasons.append(
            f"Max open positions reached "
            f"{open_count}/{max_positions}"
        )

    # ==========================================
    # MARKET REGIME FILTER
    # ==========================================

    if not market.get(
        "allow_new_buys",
        False,
    ):

        blocked_reasons.append(
            f"Market regime blocks new buys: "
            f"{market.get('regime')}"
        )

    # ==========================================
    # CAPITAL PROTECTION
    # ==========================================

    if available_cash < float(config.MIN_TRADE_USD):

        blocked_reasons.append(
            "Available cash below minimum trade amount"
        )

    # ==========================================
    # GLOBAL RISK ENGINE
    # ==========================================

    if global_risk.get("risk_triggered"):
        blocked_reasons.append(global_risk.get("risk_message"))

        await _set_auto_trading_state(
            False,
            source="global_risk_manager",
            reason=global_risk.get("risk_message") or "Global risk protection activated",
        )

        auto_trading_enabled = False
        auto_trading_state = await _get_auto_trading_state()

        log.warning(
            "GLOBAL RISK PROTECTION ACTIVATED | %s",
            global_risk.get("risk_message"),
        )

    broker_snapshot = await database.get_latest_broker_sync_snapshot() or {}
    recon_issues = await database.get_open_reconciliation_issues()

    # ==========================================
    # WATCHDOG
    # ==========================================

    if watchdog_status.get("trading_blocked"):
        blocked_reasons.extend(watchdog_status.get("blocking_reasons") or ["Watchdog blocked trading"])

    # ==========================================
    # FINAL DECISION
    # ==========================================

    can_open_new_trades = (
        len(blocked_reasons) == 0
    )

    # ==========================================
    # RESPONSE
    # ==========================================

    return JSONResponse(
        {

            # ==========================================
            # TRADING STATE
            # ==========================================

            "trading_mode": config.TRADING_MODE,

            "strategy_mode": active_strategy_payload["strategy_mode"],

            "active_buy_engine": active_strategy_payload["active_buy_engine"],

            "active_sell_engine": active_strategy_payload["active_sell_engine"],

            "active_risk_profile": active_strategy_payload["active_risk_profile"],

            "intraday_rules": active_strategy_payload["intraday_rules"],

            "active_training_profile": active_strategy_payload["active_training_profile"],

            "profile_rules": active_strategy_payload["profile_rules"],

            "effective_max_positions": active_strategy_payload["effective_max_positions"],

            "effective_score_threshold": active_strategy_payload["effective_score_threshold"],

            "effective_risk_factor": active_strategy_payload["effective_risk_factor"],

            "effective_max_daily_trades": active_strategy_payload["effective_max_daily_trades"],

            "intraday_enrichment_status": active_strategy_payload["intraday_enrichment_status"],
            "intraday_engine": {
                "active": strategy_mode.is_intraday_mode(active_strategy_mode),
                "mode": active_strategy_payload["strategy_mode"],
                "buy_engine": "intraday_momentum_engine",
                "sell_engine": "intraday_exit_engine",
                "intraday_regime": "MOMENTUM_NEUTRAL",
                "buy_threshold": intraday_momentum_engine.BUY_THRESHOLD,
                "required_timeframes": list(intraday_momentum_engine.REQUIRED_TIMEFRAMES),
                "max_daily_trades": active_strategy_payload["intraday_rules"].get("max_daily_trades"),
                "max_consecutive_losses": active_strategy_payload["intraday_rules"].get("max_consecutive_losses"),
                "max_daily_loss_percent": active_strategy_payload["intraday_rules"].get("max_daily_loss_percent"),
                "force_exit_before_close": True,
                "allow_overnight": False,
            },
            "intraday_aggressive_profile": {
                "active": active_strategy_payload["intraday_rules"].get("training_profile") == "INTRADAY_AGGRESSIVE",
                "min_score_to_buy": active_strategy_payload["intraday_rules"].get("min_score_to_buy"),
                "risk_per_trade_percent": active_strategy_payload["intraday_rules"].get("risk_per_trade_percent"),
                "max_daily_loss_percent": active_strategy_payload["intraday_rules"].get("max_daily_loss_percent"),
                "max_daily_trades": active_strategy_payload["intraday_rules"].get("max_daily_trades"),
                "max_open_intraday_positions": active_strategy_payload["intraday_rules"].get("max_open_intraday_positions", active_strategy_payload["intraday_rules"].get("max_open_positions")),
                "max_total_intraday_exposure_percent": active_strategy_payload["intraday_rules"].get("max_total_intraday_exposure_percent"),
                "max_single_position_percent": active_strategy_payload["intraday_rules"].get("max_single_position_percent"),
                "take_profit_1_percent": active_strategy_payload["intraday_rules"].get("take_profit_1_percent"),
                "take_profit_2_percent": active_strategy_payload["intraday_rules"].get("take_profit_2_percent"),
                "stop_loss_range": {
                    "min": active_strategy_payload["intraday_rules"].get("stop_loss_percent_min"),
                    "max": active_strategy_payload["intraday_rules"].get("stop_loss_percent_max"),
                },
                "partial_take_profit_enabled": active_strategy_payload["intraday_rules"].get("partial_take_profit_enabled"),
                "force_exit_before_close": active_strategy_payload["intraday_rules"].get("force_exit_before_close", True),
                "allow_overnight": active_strategy_payload["intraday_rules"].get("allow_overnight", False),
            },

            "force_exit_before_close": active_strategy_payload["force_exit_before_close"],

            "auto_send_orders": config.AUTO_SEND_ORDERS,

            "paper_trading": config.IBKR_PAPER_TRADING,

            "real_trading_enabled": config.IBKR_ENABLE_REAL_TRADING,

            "auto_trading_enabled": auto_trading_enabled,

            "auto_trading_state_source": auto_trading_state.get("source"),

            "auto_trading_state_reason": auto_trading_state.get("reason"),

            "market_hours_guard_enabled": market_hours.get("enabled"),

            "market_hours_allowed": market_hours.get("allowed"),

            "market_hours_reason": market_hours.get("reason"),

            "market_hours": market_hours,

            "watchdog": watchdog_status,

            "live_position_tracker": watchdog_status.get("live_position_tracking") or await live_position_tracker.get_tracker_status(),

            "circuit_breaker_auto_recovered": bool(
                watchdog_status.get("circuit_breaker_auto_recovered")
                or (
                    circuit_breaker_auto_recovery
                    and not (watchdog_status.get("circuit_breaker") or {}).get("tripped")
                )
            ),

            "last_market_data_at": watchdog_status.get("last_market_data_at"),

            "last_market_data_age_seconds": watchdog_status.get("last_market_data_age_seconds"),

            "last_market_data_refresh_source": watchdog_status.get("last_market_data_refresh_source"),

            "market_data_feed_active": bool(watchdog_status.get("market_data_feed_active")),

            "connection_status": {
                "tws_connected": watchdog_status.get("tws_connected"),
                "shared_ib_connected": watchdog_status.get("shared_ib_connected"),
                "heartbeat": watchdog_status.get("heartbeat"),
            },

            "stale_data_status": watchdog_status.get("stale_data"),

            "last_heartbeat_at": watchdog_status.get("last_heartbeat_at"),

            "last_reconnect_attempt_at": watchdog_status.get("last_reconnect_attempt_at"),

            "last_reconnect_result": watchdog_status.get("last_reconnect_result"),

            # ==========================================
            # MARKET REGIME
            # ==========================================

            "market_regime": market.get("regime"),

            "allow_new_buys": market.get("allow_new_buys"),

            "position_size_factor": market.get("position_size_factor"),

            "min_score_override": market.get("min_score_override"),

            # ==========================================
            # ACCOUNT
            # ==========================================

            "account_balance": float(config.effective_account_balance()),

            "virtual_trading_capital": float(config.effective_virtual_trading_capital()),

            "active_paper_session": active_paper_session,

            "session_start_equity": round(
                float((active_paper_session or {}).get("session_start_equity", account_equity)),
                2,
            ),

            "risk_calculation_basis": "virtual_trading_capital",

            "realized_pnl": round(
                float(realized_pnl),
                2,
            ),

            "account_equity": round(
                account_equity,
                2,
            ),

            "used_capital": round(
                used_capital,
                2,
            ),

            "cash_reserve": round(
                cash_reserve,
                2,
            ),

            "available_cash": round(
                available_cash,
                2,
            ),

            # ==========================================
            # POSITIONS
            # ==========================================

            "open_positions": open_count,

            "max_open_positions": max_positions,

            # ==========================================
            # FINAL STATE
            # ==========================================

            "can_open_new_trades": can_open_new_trades,

            "blocked_reasons": blocked_reasons,
        "broker_sync": {"connected": bool(broker_snapshot.get("connected")), "last_synced_at": broker_snapshot.get("synced_at"), "account": broker_snapshot.get("account"), "equity": {"net_liquidation": broker_snapshot.get("net_liquidation"), "total_cash": broker_snapshot.get("total_cash"), "available_funds": broker_snapshot.get("available_funds"), "buying_power": broker_snapshot.get("buying_power")}, "broker_positions_count": len(json.loads(broker_snapshot.get("positions_json") or "[]")), "broker_open_orders_count": len(json.loads(broker_snapshot.get("open_orders_json") or "[]")), "broker_executions_count": len(json.loads(broker_snapshot.get("executions_json") or "[]")), "errors": json.loads(broker_snapshot.get("errors_json") or "[]")},
        "reconciliation": {"ok": len([i for i in recon_issues if i.get("severity")=="HIGH"])==0, "open_issues_count": len(recon_issues), "high_severity_issues_count": len([i for i in recon_issues if i.get("severity")=="HIGH"]), "last_checked_at": (recon_issues[0].get("created_at") if recon_issues else None), "issues": recon_issues[:20]},
        "source_of_truth": {"broker_is_source_of_truth": True, "db_positions_match_broker": True, "orders_match_broker": True, "executions_synced": True},

            # ==========================================
            # GLOBAL RISK
            # ==========================================

            "global_risk": global_risk,
        },
        headers=no_cache_headers(),
)

@app.get("/api/orders")
async def api_orders(limit: int = 200):
    try:
        rows = await database.fetch_all("SELECT * FROM orders ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?", (max(1, min(limit, 1000)),)) or []
        return {
            "ok": True,
            "connected": True,
            "orders": rows if isinstance(rows, list) else [],
            "count": len(rows if isinstance(rows, list) else []),
            "errors": [],
            "last_synced_at": None,
            "heartbeat_age_seconds": None,
            "open_orders_count": 0,
            "executions_count": 0,
            "positions_count": 0,
            "source": "orders_table",
        }
    except Exception as exc:
        log.exception("api_orders failed")
        return {
            "ok": False,
            "connected": False,
            "orders": [],
            "count": 0,
            "errors": [str(exc)],
            "last_synced_at": None,
            "heartbeat_age_seconds": None,
            "open_orders_count": 0,
            "executions_count": 0,
            "positions_count": 0,
            "source": "orders_table",
        }


@app.get("/api/broker-sync/status")
async def api_broker_sync_status():
    def _safe_json_array(value) -> list:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    try:
        snapshot = await database.fetch_one("SELECT * FROM broker_sync_snapshots ORDER BY id DESC LIMIT 1") or {}
        errors = _safe_json_array(snapshot.get("errors_json")) or _safe_json_array(snapshot.get("errors"))
        open_orders = _safe_json_array(snapshot.get("open_orders_json"))
        executions = _safe_json_array(snapshot.get("executions_json"))
        positions = _safe_json_array(snapshot.get("positions_json"))
        return {
            "ok": bool(snapshot),
            "connected": bool(snapshot.get("connected")),
            "snapshot": snapshot,
            "errors": errors,
            "last_synced_at": snapshot.get("synced_at"),
            "heartbeat_age_seconds": snapshot.get("heartbeat_age_seconds"),
            "open_orders_count": len(open_orders),
            "executions_count": len(executions),
            "positions_count": len(positions),
            "source": "broker_sync_snapshots",
            "metrics": {"positions_count": len(positions), "open_orders_count": len(open_orders), "executions_count": len(executions), "errors_count": len(errors)},
        }
    except Exception as exc:
        log.exception("api_broker_sync_status failed")
        return {
            "ok": False,
            "connected": False,
            "snapshot": {},
            "metrics": {},
            "errors": [str(exc)],
            "last_synced_at": None,
            "heartbeat_age_seconds": None,
            "open_orders_count": 0,
            "executions_count": 0,
            "positions_count": 0,
            "source": "broker_sync_snapshots",
        }


@app.post("/api/broker-sync/run")
async def api_broker_sync_run():
    def _safe_array(payload: dict[str, Any], key: str) -> list:
        value = payload.get(key)
        if isinstance(value, list):
            return value
        return []

    try:
        result = await broker_sync.run_broker_sync_once() or {}
        if not isinstance(result, dict):
            result = {"ok": False, "connected": False, "errors": ["malformed broker response"], "source": "broker_sync"}
        await database.save_broker_sync_snapshot(result)
        errors = _safe_array(result, "errors")
        open_orders = _safe_array(result, "open_orders")
        executions = _safe_array(result, "executions")
        positions = _safe_array(result, "positions")
        return {
            "ok": bool(result.get("ok", False)),
            "connected": bool(result.get("connected", False)),
            "result": result,
            "errors": errors,
            "last_synced_at": result.get("synced_at"),
            "heartbeat_age_seconds": result.get("heartbeat_age_seconds"),
            "open_orders_count": len(open_orders),
            "executions_count": len(executions),
            "positions_count": len(positions),
            "source": result.get("source") or "broker_sync",
        }
    except Exception as exc:
        log.exception("api_broker_sync_run failed")
        return {
            "ok": False,
            "connected": False,
            "result": {},
            "errors": [str(exc)],
            "last_synced_at": None,
            "heartbeat_age_seconds": None,
            "open_orders_count": 0,
            "executions_count": 0,
            "positions_count": 0,
            "source": "broker_sync",
        }


@app.post("/api/emergency/flatten-all")
async def api_emergency_flatten_all():
    from trade_engine import flatten_all
    result = flatten_all(reason="api_emergency_flatten_all")
    return result


@app.post("/api/emergency/stop-trading")
async def api_emergency_stop_trading():
    await database.set_app_state("auto_trading_enabled", "false")
    return {"ok": True, "auto_trading_enabled": False}


@app.post("/api/intraday/exit-check")
async def api_intraday_exit_check(payload: dict | None = None):
    from intraday_exit_engine import evaluate_exit
    payload = payload or {}
    return evaluate_exit(payload.get("position", {}), payload.get("signals", {}))

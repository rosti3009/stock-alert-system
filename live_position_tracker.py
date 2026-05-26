from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import config
import database
import strategy_mode
import watchdog
from data_fetcher import fetch_intraday_bars
from position_manager import evaluate_position
from telegram_notifier import send_position_alert

log = logging.getLogger(__name__)

LIVE_POSITION_TRACKER_STATE_KEY = "live_position_tracker_status"
LIVE_POSITION_TRACKER_SOURCE = "live_position_tracker"
ALWAYS_REFRESH_POSITION_SOURCES = {
    "TWS_RECONCILIATION_RECOVERY",
    "TWS_BASELINE_ADOPTED",
}

_refresh_lock = asyncio.Lock()

ScanCallable = Callable[[str], Awaitable[dict[str, Any]]]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def configured_interval_seconds() -> int:
    try:
        return max(1, int(getattr(config, "POSITION_TRACK_INTERVAL_SECONDS", 15)))
    except Exception:
        return 15


def configured_swing_interval_seconds() -> int:
    try:
        default = max(configured_interval_seconds(), 60)
        return max(configured_interval_seconds(), int(getattr(config, "SWING_POSITION_TRACK_INTERVAL_SECONDS", default)))
    except Exception:
        return max(configured_interval_seconds(), 60)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _age_seconds(value: Any) -> int | None:
    dt = _parse_dt(value)
    if not dt:
        return None
    return max(0, int((now_utc() - dt).total_seconds()))


def _json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


async def get_tracker_status() -> dict[str, Any]:
    raw = await database.get_app_state(LIVE_POSITION_TRACKER_STATE_KEY)
    if not raw:
        return _empty_status()
    try:
        status = json.loads(raw)
    except Exception:
        return _empty_status(error="Live position tracker status is unreadable")
    status["last_refresh_age_seconds"] = _age_seconds(status.get("last_refresh_at"))
    for item in status.get("positions", []):
        item["last_refresh_age_seconds"] = _age_seconds(item.get("last_refresh_at"))
    return status


def get_tracker_status_sync() -> dict[str, Any]:
    import sqlite3
    from contextlib import closing

    try:
        with closing(sqlite3.connect(config.DB_PATH)) as db:
            db.execute(database.CREATE_APP_STATE)
            row = db.execute(
                "SELECT value FROM app_state WHERE key = ?",
                (LIVE_POSITION_TRACKER_STATE_KEY,),
            ).fetchone()
    except Exception as exc:
        return _empty_status(error=f"Live position tracker status unavailable: {exc}")

    if not row:
        return _empty_status()
    try:
        status = json.loads(row[0])
    except Exception:
        return _empty_status(error="Live position tracker status is unreadable")
    status["last_refresh_age_seconds"] = _age_seconds(status.get("last_refresh_at"))
    for item in status.get("positions", []):
        item["last_refresh_age_seconds"] = _age_seconds(item.get("last_refresh_at"))
    return status


def _empty_status(error: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": LIVE_POSITION_TRACKER_SOURCE,
        "tracked_symbols": [],
        "positions": [],
        "open_position_count": 0,
        "tracked_count": 0,
        "last_refresh_at": None,
        "last_refresh_age_seconds": None,
        "interval_seconds": configured_interval_seconds(),
        "swing_interval_seconds": configured_swing_interval_seconds(),
        "healthy": True,
        "running": False,
    }
    if error:
        payload["healthy"] = False
        payload["error"] = error
    return payload


def _position_source(position: dict[str, Any]) -> str:
    return str(position.get("source") or "").strip().upper()


def _is_reconciliation_managed_position(position: dict[str, Any]) -> bool:
    return _position_source(position) in ALWAYS_REFRESH_POSITION_SOURCES


def _is_position_intraday(position: dict[str, Any], active_mode: strategy_mode.StrategyMode | str) -> bool:
    return strategy_mode.is_intraday_mode(position.get("strategy_mode") or active_mode)


def _should_refresh_position(position: dict[str, Any], previous_by_symbol: dict[str, dict[str, Any]], active_mode: strategy_mode.StrategyMode | str) -> bool:
    if _is_reconciliation_managed_position(position):
        return True
    if _is_position_intraday(position, active_mode):
        return True
    previous = previous_by_symbol.get(str(position.get("symbol") or "").upper()) or {}
    age = _age_seconds(previous.get("last_refresh_at"))
    return age is None or age >= configured_swing_interval_seconds()


def _live_metadata(position: dict[str, Any], scan_result: dict[str, Any], bars_1m: list[dict] | None, bars_5m: list[dict] | None, refreshed_at: str) -> dict[str, Any]:
    symbol = str(position.get("symbol") or "").upper()
    bid = scan_result.get("bid") or scan_result.get("best_bid")
    ask = scan_result.get("ask") or scan_result.get("best_ask")
    vwap = scan_result.get("vwap") or scan_result.get("intraday_vwap")
    momentum = scan_result.get("momentum_percent") or scan_result.get("change_percent") or scan_result.get("rsi")
    return {
        "symbol": symbol,
        "status": "OPEN",
        "source": LIVE_POSITION_TRACKER_SOURCE,
        "position_source": position.get("source"),
        "last_refresh_at": refreshed_at,
        "refresh_source": LIVE_POSITION_TRACKER_SOURCE,
        "live_tracking": True,
        "live_tracking_source": LIVE_POSITION_TRACKER_SOURCE,
        "live_tracking_last_refresh_at": refreshed_at,
        "quote_refreshed": scan_result.get("price") is not None or scan_result.get("current_price") is not None,
        "bid": bid,
        "ask": ask,
        "bid_ask_refreshed": bid is not None or ask is not None,
        "vwap": vwap,
        "vwap_refreshed": vwap is not None,
        "bars_1m_refreshed": bool(bars_1m),
        "bars_5m_refreshed": bool(bars_5m),
        "momentum": momentum,
        "momentum_refreshed": momentum is not None,
        "signal": scan_result.get("signal"),
        "action": position.get("action"),
        "current_price": position.get("current_price"),
        "profit_amount": position.get("profit_amount"),
        "profit_percent": position.get("profit_percent"),
    }


def _merge_previous_status(previous: dict[str, Any], open_positions: list[dict[str, Any]], active_mode: strategy_mode.StrategyMode | str) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    previous_by_symbol = {
        str(item.get("symbol") or "").upper(): item
        for item in previous.get("positions", [])
        if item.get("symbol")
    }
    open_symbols = {str(p.get("symbol") or "").upper() for p in open_positions if p.get("symbol")}
    retained = [item for symbol, item in previous_by_symbol.items() if symbol in open_symbols]
    return retained, previous_by_symbol


async def _resolve_open_symbols_from_sources(open_positions: list[dict[str, Any]]) -> list[str]:
    symbols = {
        str(item.get("symbol") or "").strip().upper()
        for item in open_positions
        if item.get("symbol")
    }

    snapshot = await database.get_latest_broker_sync_snapshot() or {}
    for position in snapshot.get("positions") or []:
        symbol = str(position.get("symbol") or "").strip().upper()
        qty = position.get("position")
        try:
            has_qty = float(qty) != 0.0
        except Exception:
            has_qty = bool(qty)
        if symbol and has_qty:
            symbols.add(symbol)

    return sorted(symbol for symbol in symbols if symbol)


async def _record_transition_event(position: dict[str, Any], scan_result: dict[str, Any], position_update: dict[str, Any], previous_action: str | None) -> None:
    new_action = position_update.get("action")
    journal_event_by_action = {
        "STOP_LOSS_HIT": "STOP_LOSS_TRIGGERED",
        "TAKE_PROFIT_1": "TP1_TRIGGERED",
        "TAKE_PROFIT_2": "TP2_TRIGGERED",
        "TRAILING_STOP_UPDATED": "TRAILING_STOP_UPDATED",
        "SELL_SIGNAL": "SELL_SIGNAL_DETECTED",
        "INTRADAY_STOP_LOSS_HIT": "INTRADAY_STOP_LOSS_TRIGGERED",
        "INTRADAY_TAKE_PROFIT_FAST": "INTRADAY_TP_FAST_TRIGGERED",
        "INTRADAY_TAKE_PROFIT_FINAL": "INTRADAY_TP_FINAL_TRIGGERED",
        "INTRADAY_TRAILING_STOP_UPDATED": "INTRADAY_TRAILING_STOP_UPDATED",
        "INTRADAY_SELL_SIGNAL": "INTRADAY_SELL_SIGNAL_DETECTED",
        "INTRADAY_FORCE_EXIT": "INTRADAY_FORCE_EXIT_TRIGGERED",
        "INTRADAY_TIME_EXIT": "INTRADAY_TIME_EXIT_TRIGGERED",
    }
    event_type = journal_event_by_action.get(new_action)
    if not event_type or new_action == previous_action:
        return
    await database.safe_record_trade_journal_event({
        "symbol": position.get("symbol"),
        "event_type": event_type,
        "decision": new_action,
        "reason": position_update.get("reason"),
        "source_module": "live_position_tracker.refresh_live_tracked_positions",
        "signal_score": scan_result.get("score"),
        "weekly_score": scan_result.get("weekly_score"),
        "price": position_update.get("current_price"),
        "quantity": position_update.get("sell_quantity") or position.get("quantity"),
        "stop_loss": position_update.get("stop_loss"),
        "take_profit_1": position_update.get("take_profit_1"),
        "take_profit_2": position_update.get("take_profit_2"),
        "risk_percent": scan_result.get("risk_percent"),
        "realized_pnl": position_update.get("profit_amount") if position_update.get("status") == "CLOSED" else None,
        "unrealized_pnl": position_update.get("profit_amount") if position_update.get("status") != "CLOSED" else None,
        "raw_payload": {"position": position, "scan_result": scan_result, "position_update": position_update},
    })


async def _refresh_one_position(position: dict[str, Any], scan_symbol: ScanCallable, active_mode: strategy_mode.StrategyMode | str, refreshed_at: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    symbol = str(position.get("symbol") or "").strip().upper()
    if not symbol:
        return None, None

    scan_result = await scan_symbol(symbol)
    bars_1m = fetch_intraday_bars(symbol, "1m")
    bars_5m = fetch_intraday_bars(symbol, "5m")

    if scan_result.get("error") or scan_result.get("signal") == "ERROR":
        updated = await database.update_position(symbol, {
            "action": "ERROR",
            "reason": scan_result.get("error", "Failed to update live tracked position"),
            "updated_at": refreshed_at,
        })
        metadata = _live_metadata(updated or position, scan_result, bars_1m, bars_5m, refreshed_at)
        metadata["error"] = scan_result.get("error") or "scan error"
        return updated, metadata

    enriched_scan = dict(scan_result)
    enriched_scan["strategy_mode"] = str(active_mode)
    enriched_scan["intraday_bars"] = {"1m": bars_1m or [], "5m": bars_5m or []}
    enriched_scan["intraday_bars_available"] = bool(bars_1m or bars_5m)

    position_update = evaluate_position(position, enriched_scan, mode=str(active_mode))
    previous_action = position.get("action")
    await _record_transition_event(position, enriched_scan, position_update, previous_action)

    new_status = position_update.get("status", "OPEN")
    updated = await database.update_position(symbol, {
        "current_price": position_update.get("current_price"),
        "profit_amount": position_update.get("profit_amount"),
        "profit_percent": position_update.get("profit_percent"),
        "stop_loss": position_update.get("stop_loss"),
        "take_profit_1": position_update.get("take_profit_1"),
        "take_profit_2": position_update.get("take_profit_2"),
        "status": new_status,
        "action": position_update.get("action"),
        "reason": position_update.get("reason"),
        "updated_at": refreshed_at,
        "closed_at": refreshed_at if new_status == "CLOSED" else position.get("closed_at"),
    })

    updated_or_position = updated or {**position, **position_update}
    metadata = _live_metadata(updated_or_position, enriched_scan, bars_1m, bars_5m, refreshed_at)
    metadata["status"] = new_status
    metadata["action"] = position_update.get("action")
    metadata["exit_engine"] = position_update.get("exit_engine")
    metadata["trailing_stop_logic_refreshed"] = True
    metadata["exit_rules_refreshed"] = True
    metadata["pnl_refreshed"] = position_update.get("profit_amount") is not None or position_update.get("profit_percent") is not None

    important_actions = {
        "STOP_LOSS_HIT", "SELL_SIGNAL", "TAKE_PROFIT_1", "TAKE_PROFIT_2", "MOVE_STOP_TO_BREAKEVEN",
        "TRAILING_STOP_UPDATED", "EXIT_WARNING", "WARNING", "WATCH_PROFIT", "INTRADAY_STOP_LOSS_HIT",
        "INTRADAY_TAKE_PROFIT_FAST", "INTRADAY_TAKE_PROFIT_FINAL", "INTRADAY_TRAILING_STOP_UPDATED",
        "INTRADAY_SELL_SIGNAL", "INTRADAY_FORCE_EXIT", "INTRADAY_TIME_EXIT",
    }
    if position_update.get("action") in important_actions and position_update.get("action") != previous_action and updated:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, send_position_alert, updated)
        except Exception as exc:
            log.warning("[%s] Telegram live position alert failed: %s", symbol, exc)

    return updated, metadata


async def refresh_live_tracked_positions(scan_symbol: ScanCallable) -> list[dict[str, Any]]:
    if _refresh_lock.locked():
        log.info("Live position tracker refresh skipped — already running")
        return []

    async with _refresh_lock:
        started_at = now_iso()
        previous = await get_tracker_status()
        open_positions = await database.get_open_positions()
        active_mode = await strategy_mode.get_strategy_mode()
        retained, previous_by_symbol = _merge_previous_status(previous, open_positions, active_mode)
        metadata_by_symbol = {str(item.get("symbol") or "").upper(): item for item in retained}
        refreshed_positions: list[dict[str, Any]] = []
        errors: list[str] = []

        for position in open_positions:
            symbol = str(position.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            if not _should_refresh_position(position, previous_by_symbol, active_mode):
                continue
            try:
                updated, metadata = await _refresh_one_position(position, scan_symbol, active_mode, started_at)
            except Exception as exc:
                log.exception("Live position tracker failed for %s", symbol)
                errors.append(f"{symbol}: {exc}")
                metadata = {
                    "symbol": symbol,
                    "status": "OPEN",
                    "source": LIVE_POSITION_TRACKER_SOURCE,
                    "last_refresh_at": previous_by_symbol.get(symbol, {}).get("last_refresh_at"),
                    "refresh_source": LIVE_POSITION_TRACKER_SOURCE,
                    "error": str(exc),
                }
                updated = None

            if metadata and metadata.get("status") == "OPEN":
                metadata_by_symbol[symbol] = metadata
            else:
                metadata_by_symbol.pop(symbol, None)
            if updated:
                refreshed_positions.append(updated)

        latest_open_positions = await database.get_open_positions()
        latest_open_symbols = set(await _resolve_open_symbols_from_sources(latest_open_positions))
        if latest_open_symbols and not metadata_by_symbol:
            for symbol in latest_open_symbols:
                metadata_by_symbol[symbol] = {
                    "symbol": symbol,
                    "status": "OPEN",
                    "source": LIVE_POSITION_TRACKER_SOURCE,
                    "position_source": "RECONCILED",
                    "last_refresh_at": started_at,
                    "refresh_source": "live_position_tracker_rebuild",
                    "live_tracking": True,
                    "live_tracking_source": LIVE_POSITION_TRACKER_SOURCE,
                    "live_tracking_last_refresh_at": started_at,
                    "reconciled": True,
                }
        positions_payload = [item for symbol, item in sorted(metadata_by_symbol.items()) if symbol in latest_open_symbols]
        healthy = not errors
        last_refresh_at = started_at if latest_open_symbols else None
        status = {
            "source": LIVE_POSITION_TRACKER_SOURCE,
            "last_refresh_at": last_refresh_at,
            "last_refresh_started_at": started_at,
            "last_refresh_completed_at": now_iso(),
            "interval_seconds": configured_interval_seconds(),
            "swing_interval_seconds": configured_swing_interval_seconds(),
            "open_position_count": len(latest_open_positions),
            "tracked_count": len(positions_payload),
            "tracked_symbols": [item["symbol"] for item in positions_payload],
            "positions": positions_payload,
            "healthy": healthy,
            "running": False,
            "errors": errors,
        }
        await database.set_app_state(LIVE_POSITION_TRACKER_STATE_KEY, _json(status))

        if latest_open_positions:
            await watchdog.refresh_market_data_timestamp(
                LIVE_POSITION_TRACKER_SOURCE,
                metadata={"tracked_symbols": status["tracked_symbols"], "tracked_count": status["tracked_count"]},
            )

        return refreshed_positions


async def prune_closed_positions() -> dict[str, Any]:
    """Synchronize tracker state with DB positions after manual/adopted close paths."""
    status = await get_tracker_status()
    open_positions = await database.get_open_positions()
    open_symbols = {str(p.get("symbol") or "").upper() for p in open_positions if p.get("symbol")}
    positions = [item for item in status.get("positions", []) if str(item.get("symbol") or "").upper() in open_symbols]
    status.update({
        "positions": positions,
        "tracked_symbols": [item.get("symbol") for item in positions if item.get("symbol")],
        "tracked_count": len(positions),
        "open_position_count": len(open_positions),
    })
    if not open_positions:
        status["last_refresh_at"] = None
    await database.set_app_state(LIVE_POSITION_TRACKER_STATE_KEY, _json(status))
    return status

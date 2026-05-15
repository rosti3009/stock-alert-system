from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

import config
import database
import account_sync
import recovery_manager
import session_manager
import order_lifecycle
import portfolio_risk_engine
import startup_recovery
import reconciliation_lifecycle
import watchdog
from circuit_breaker import (
    auto_clear_recoverable_circuit_breaker,
    get_circuit_breaker_state,
    get_ibkr_error_count,
    get_last_auto_recovery,
    is_auto_recoverable_trip,
    reset_circuit_breaker,
)
import position_sizing_engine
import position_exit_priority_engine
import sector_intelligence
from execution_quality import evaluate_execution_quality, summarize_execution_quality
from auto_trader import process_auto_trading
from trading_safety import get_market_hours_status
from market_regime_engine import get_cached_market_regime, get_market_regime_history, refresh_market_regime
from market_regime import get_market_regime
from data_fetcher import fetch_stock_data
from indicators import compute_indicators
from ranking_engine import calculate_weekly_score, rank_top_weekly_setups
from signal_logic import evaluate_signal
from symbol_loader import load_nasdaq_symbols
from telegram_notifier import send_buy_alert, send_sell_alert, send_position_alert
from position_manager import evaluate_position
from ibkr_asyncio_compat import ensure_event_loop
from paper_liquidation import liquidate_all_paper_positions

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

_latest: dict[str, dict] = {}
_top_weekly: list[dict] = []
_scan_lock = asyncio.Lock()
_positions_lock = asyncio.Lock()
scheduler = AsyncIOScheduler()


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


def get_max_open_positions() -> int:
    return int(getattr(config, "MAX_OPEN_POSITIONS", 10))


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


async def get_scan_symbols() -> list[str]:
    if config.USE_DYNAMIC_SYMBOLS:
        all_symbols = load_nasdaq_symbols(limit=None)
    else:
        all_symbols = config.SYMBOLS

    if not all_symbols:
        return config.SYMBOLS[:config.MAX_SYMBOLS_PER_SCAN]

    all_symbols = [
        str(symbol).strip().upper()
        for symbol in all_symbols
        if symbol
    ]

    if not all_symbols:
        return config.SYMBOLS[:config.MAX_SYMBOLS_PER_SCAN]

    total = len(all_symbols)
    batch_size = min(config.MAX_SYMBOLS_PER_SCAN, total)

    priority_symbols = await database.get_priority_symbols(limit=batch_size)

    saved_offset = await database.get_app_state("scan_offset", "0")

    try:
        start = int(saved_offset or 0)
    except Exception:
        start = 0

    if start >= total:
        start = 0

    end = start + batch_size

    if end <= total:
        rotation_symbols = all_symbols[start:end]
    else:
        rotation_symbols = all_symbols[start:] + all_symbols[:end - total]

    selected: list[str] = []
    seen: set[str] = set()
    all_set = set(all_symbols)

    for symbol in priority_symbols + rotation_symbols:
        symbol = str(symbol).strip().upper()

        if not symbol:
            continue

        if symbol in seen:
            continue

        if symbol not in all_set:
            continue

        selected.append(symbol)
        seen.add(symbol)

        if len(selected) >= batch_size:
            break

    if len(selected) < batch_size:
        for symbol in all_symbols:
            symbol = str(symbol).strip().upper()

            if symbol in seen:
                continue

            selected.append(symbol)
            seen.add(symbol)

            if len(selected) >= batch_size:
                break

    next_offset = end % total

    await database.set_app_state("scan_offset", str(next_offset))

    log.info(
        "Smart rotation selected: priority=%s rotation=%s total_selected=%s | next_offset=%s",
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
            log.exception("Fast position refresh failed")

async def refresh_open_positions() -> list[dict]:
    positions = await database.get_open_positions()
    updated_positions = []

    if not positions:
        return []

    log.info("Refreshing %s open positions", len(positions))

    important_actions = {
        "STOP_LOSS_HIT",
        "SELL_SIGNAL",
        "TAKE_PROFIT_1",
        "TAKE_PROFIT_2",
        "MOVE_STOP_TO_BREAKEVEN",
        "TRAILING_STOP_UPDATED",
        "EXIT_WARNING",
        "WARNING",
        "WATCH_PROFIT",
    }

    for position in positions:
        symbol = position.get("symbol")

        if not symbol:
            continue

        scan_result = await scan_symbol(symbol)

        if scan_result.get("error") or scan_result.get("signal") == "ERROR":
            await database.update_position(symbol, {
                "action": "ERROR",
                "reason": scan_result.get("error", "Failed to update position"),
                "updated_at": database.now_iso(),
            })
            continue

        position_update = evaluate_position(position, scan_result)

        previous_action = position.get("action")
        new_action = position_update.get("action")
        new_status = position_update.get("status", "OPEN")

        journal_event_by_action = {
            "STOP_LOSS_HIT": "STOP_LOSS_TRIGGERED",
            "TAKE_PROFIT_1": "TP1_TRIGGERED",
            "TAKE_PROFIT_2": "TP2_TRIGGERED",
            "TRAILING_STOP_UPDATED": "TRAILING_STOP_UPDATED",
            "SELL_SIGNAL": "SELL_SIGNAL_DETECTED",
        }

        journal_event_type = journal_event_by_action.get(new_action)

        if journal_event_type and new_action != previous_action:
            await database.safe_record_trade_journal_event({
                "symbol": symbol,
                "event_type": journal_event_type,
                "decision": new_action,
                "reason": position_update.get("reason"),
                "source_module": "main.refresh_open_positions",
                "signal_score": scan_result.get("score"),
                "weekly_score": scan_result.get("weekly_score"),
                "price": position_update.get("current_price"),
                "quantity": position_update.get("sell_quantity") or position.get("quantity"),
                "stop_loss": position_update.get("stop_loss"),
                "take_profit_1": position_update.get("take_profit_1"),
                "take_profit_2": position_update.get("take_profit_2"),
                "risk_percent": scan_result.get("risk_percent"),
                "realized_pnl": (
                    position_update.get("profit_amount")
                    if new_status == "CLOSED"
                    else None
                ),
                "unrealized_pnl": (
                    position_update.get("profit_amount")
                    if new_status != "CLOSED"
                    else None
                ),
                "raw_payload": {
                    "position": position,
                    "scan_result": scan_result,
                    "position_update": position_update,
                },
            })

        updated = await database.update_position(symbol, {
            "current_price": position_update.get("current_price"),
            "profit_amount": position_update.get("profit_amount"),
            "profit_percent": position_update.get("profit_percent"),
            "stop_loss": position_update.get("stop_loss"),
            "take_profit_1": position_update.get("take_profit_1"),
            "take_profit_2": position_update.get("take_profit_2"),
            "status": new_status,
            "action": new_action,
            "reason": position_update.get("reason"),
            "updated_at": database.now_iso(),
            "closed_at": database.now_iso() if new_status == "CLOSED" else position.get("closed_at"),
        })

        if updated:
            updated_positions.append(updated)

            should_alert = (
                new_action in important_actions
                and new_action != previous_action
            )

            if should_alert:
                try:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, send_position_alert, updated)
                    log.info("[%s] Position Telegram alert sent: %s", symbol, new_action)
                except Exception as exc:
                    log.warning("[%s] Telegram position alert failed: %s", symbol, exc)

    return updated_positions


async def run_full_scan() -> dict:
    global _top_weekly

    if _scan_lock.locked():
        log.info("Scan request ignored — already running")
        return {"status": "already running"}

    async with _scan_lock:
        session_status = session_manager.get_cached_session_status()
        if not session_status.get("scan_allowed"):
            log.info("Scan skipped — session=%s scan_allowed=False", session_status.get("current_session"))
            return {"status": "skipped", "reason": "scan not allowed in current session", "session": session_status}

        symbols = await get_scan_symbols()

        open_positions = await database.get_open_positions()
        open_symbols = {p["symbol"] for p in open_positions if p.get("symbol")}

        for symbol in open_symbols:
            if symbol not in symbols:
                symbols.append(symbol)

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
            for i in range(0, len(symbols), config.BATCH_SIZE):
                batch = symbols[i:i + config.BATCH_SIZE]

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

                if i + config.BATCH_SIZE < len(symbols):
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

            await refresh_open_positions()

            await database.finish_scan_run(scan_run_id, stats, status="completed")
            fresh_symbols = stats["scanned_count"] + stats["skipped_count"]
            if fresh_symbols > 0:
                await watchdog.refresh_market_data_timestamp(
                    "scan_cycle_completed",
                    metadata={"scan_run_id": scan_run_id, "fresh_symbols": fresh_symbols},
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

    if config.SCAN_MODE == "daily":
        scheduler.add_job(
            run_full_scan,
            "cron",
            hour=getattr(config, "DAILY_SCAN_HOUR", 16),
            minute=getattr(config, "DAILY_SCAN_MINUTE", 30),
            id="daily_scan",
            replace_existing=True,
        )

        log.info(
            "Daily scheduler started — %s:%02d",
            getattr(config, "DAILY_SCAN_HOUR", 16),
            getattr(config, "DAILY_SCAN_MINUTE", 30),
        )

    else:
        scheduler.add_job(
            run_full_scan,
            "interval",
            minutes=config.SCAN_INTERVAL_MINUTES,
            id="interval_scan",
            replace_existing=True,
        )

        log.info(
            "Interval scheduler started — every %s min",
            config.SCAN_INTERVAL_MINUTES,
        )

    scheduler.add_job(
        refresh_open_positions_safe,
        "interval",
        seconds=30,
        id="fast_positions_refresh",
        replace_existing=True,
    )

    log.info("Fast positions refresh started — every 30 seconds")
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
        id="tws_api_watchdog",
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
        return JSONResponse(
            result,
            headers=no_cache_headers(),
        )

    except RuntimeError as exc:
        return JSONResponse(
            {
                "status": "blocked",
                "reason": str(exc),
            },
            status_code=403,
            headers=no_cache_headers(),
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


@app.post("/api/auto-trading/enable")
async def api_auto_trading_enable():
    safety = await _evaluate_auto_trading_enable_safety()
    if not safety["ok"]:
        reason = "; ".join(safety["blocked_reasons"])
        await _set_auto_trading_state(
            False,
            source="api_auto_trading_enable",
            reason=reason,
        )
        return JSONResponse(
            {
                "status": "blocked",
                "auto_trading_enabled": False,
                "reason": reason,
                **safety,
            },
            status_code=403,
            headers=no_cache_headers(),
        )

    reason = "Auto trading enabled after safety checks passed"
    await _set_auto_trading_state(True, source="api_auto_trading_enable", reason=reason)
    return JSONResponse(
        {
            "status": "enabled",
            "auto_trading_enabled": True,
            "reason": reason,
            **safety,
        },
        headers=no_cache_headers(),
    )


@app.post("/api/auto-trading/disable")
async def api_auto_trading_disable():
    reason = "Auto trading disabled by API request; existing TWS orders were not cancelled"
    await _set_auto_trading_state(False, source="api_auto_trading_disable", reason=reason)
    return JSONResponse(
        {
            "status": "disabled",
            "auto_trading_enabled": False,
            "reason": reason,
            "orders_cancelled": 0,
        },
        headers=no_cache_headers(),
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
    return JSONResponse(
        await reset_circuit_breaker(reason=reason),
        headers=no_cache_headers(),
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
    return JSONResponse(await database.get_all_positions(100), headers=no_cache_headers())


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
            getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0),
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

    max_positions = int(
        getattr(
            config,
            "MAX_OPEN_POSITIONS",
            10,
        )
    )

    open_count = len(open_positions)

    # ==========================================
    # AUTO TRADING STATE
    # ==========================================

    auto_trading_state = await _get_auto_trading_state()
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

        blocked_reasons.append(
            global_risk.get("risk_message")
        )

        # AUTO DISABLE TRADING

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

            "circuit_breaker_auto_recovered": bool(
                watchdog_status.get("circuit_breaker_auto_recovered")
                or (
                    circuit_breaker_auto_recovery
                    and not (watchdog_status.get("circuit_breaker") or {}).get("tripped")
                )
            ),

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

            "account_balance": float(config.ACCOUNT_BALANCE),

            "virtual_trading_capital": float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0)),

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

            # ==========================================
            # GLOBAL RISK
            # ==========================================

            "global_risk": global_risk,
        },
        headers=no_cache_headers(),
)

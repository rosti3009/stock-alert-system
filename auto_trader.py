from __future__ import annotations

import asyncio
import logging

from ibkr_asyncio_compat import ensure_event_loop

ensure_event_loop()

from ib_insync import IB, Stock, LimitOrder
from trade_protection import validate_buy_before_order
from recovery_manager import require_buy_allowed
from trading_safety import (
    get_market_hours_status,
    require_market_hours_order_send_allowed,
    require_paper_auto_trading_allowed,
)

import config
import database
import order_lifecycle
import portfolio_risk_engine
import strategy_mode
import intraday_momentum_engine
from execution_quality import evaluate_execution_quality
from position_sizing_engine import (
    PositionSizingInput,
    evaluate_position_sizing,
    record_position_sizing_event,
)
from market_regime import get_market_regime

log = logging.getLogger(__name__)

AUTO_TRADING_ENABLED = True
PAPER_TRADING_ENABLED = True
MIN_SCORE_TO_BUY = 80

BUY_CLIENT_ID_OFFSET = 100

FILLED_STATUSES = {
    "Filled",
}

OPEN_ORDER_STATUSES = {
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
}

_buy_lock = asyncio.Lock()


def _base_journal_event(
    row: dict,
    event_type: str,
    decision: str,
    reason: str,
    market: dict | None = None,
    extra: dict | None = None,
) -> dict:
    payload = dict(row or {})

    if extra:
        payload.update(extra)

    return {
        "symbol": str(payload.get("symbol") or "").strip().upper(),
        "event_type": event_type,
        "decision": decision,
        "reason": reason,
        "source_module": "auto_trader",
        "signal_score": payload.get("score"),
        "weekly_score": payload.get("weekly_score"),
        "market_regime": (market or {}).get("regime"),
        "price": payload.get("price") or payload.get("entry_price"),
        "quantity": payload.get("quantity"),
        "stop_loss": payload.get("stop_loss"),
        "take_profit_1": payload.get("take_profit_1"),
        "take_profit_2": payload.get("take_profit_2"),
        "risk_percent": payload.get("risk_percent"),
        "realized_pnl": payload.get("realized_pnl"),
        "unrealized_pnl": payload.get("unrealized_pnl"),
        "raw_payload": payload,
    }


async def _journal_buy_decision(
    row: dict,
    event_type: str,
    decision: str,
    reason: str,
    market: dict | None = None,
    extra: dict | None = None,
) -> None:
    event = _base_journal_event(
        row=row,
        event_type=event_type,
        decision=decision,
        reason=reason,
        market=market,
        extra=extra,
    )
    strategy_value = (market or {}).get("strategy_mode")
    if hasattr(strategy_value, "value"):
        strategy_value = strategy_value.value
    analytics_payload = {
        **(row or {}),
        **(extra or {}),
        "event_type": event_type,
        "decision": decision,
        "reason": reason,
        "rejection_reason": reason,
        "entry_reason": reason,
        "market_regime": event.get("market_regime"),
        "strategy_mode": strategy_value,
        "timestamp": event.get("timestamp") or database.now_iso(),
    }
    await database.safe_record_trade_journal_event(event)
    try:
        if decision == "ACCEPTED" or event_type == "BUY_CANDIDATE_ACCEPTED":
            await database.record_trade_decision(analytics_payload)
        elif decision in {"REJECTED", "BLOCKED"} or event_type in {
            "BUY_CANDIDATE_REJECTED",
            "INTRADAY_BUY_BLOCKED",
            "EXECUTION_BLOCK_BUY",
            "POSITION_SIZE_BLOCKED",
            "RISK_BLOCK_BUY",
            "BUY_BLOCKED_BY_SAFETY_GATE",
        }:
            await database.record_rejected_setup(analytics_payload)
    except Exception as exc:
        log.warning("Trading intelligence analytics insert failed: %s", exc)


def is_us_regular_market_open() -> bool:
    return bool(get_market_hours_status().get("allowed"))


def calculate_position_size(
    row: dict,
    open_positions: list[dict],
    account_equity: float,
    size_factor: float = 1.0,
) -> dict | None:
    entry_price = float(row.get("price") or row.get("entry_price") or 0)
    stop_loss = float(row.get("stop_loss") or 0)

    if entry_price <= 0:
        return None

    if stop_loss <= 0 or stop_loss >= entry_price:
        stop_loss = entry_price * 0.92

    broker_account_equity = float(account_equity)
    balance = float(config.effective_virtual_trading_capital())
    reserve = balance * (float(config.MIN_CASH_RESERVE_PERCENT) / 100)

    used = sum(
        float(p.get("buy_price") or 0) * float(p.get("quantity") or 0)
        for p in open_positions
        if (p.get("status") or "OPEN") == "OPEN"
    )

    available = balance - reserve - used

    if available <= float(config.MIN_TRADE_USD):
        return None

    risk_amount = (
        balance
        * (float(config.RISK_PER_TRADE_PERCENT) / 100)
        * float(size_factor)
    )

    risk_per_share = entry_price - stop_loss

    if risk_per_share <= 0:
        return None

    max_position = (
        balance
        * (float(config.MAX_POSITION_PERCENT) / 100)
        * float(size_factor)
    )

    qty_by_risk = risk_amount / risk_per_share
    qty_by_cash = available / entry_price
    qty_by_cap = max_position / entry_price

    quantity = min(qty_by_risk, qty_by_cash, qty_by_cap)

    if not config.ALLOW_FRACTIONAL_SHARES:
        quantity = int(quantity)

    quantity = float(quantity)

    if quantity <= 0:
        return None

    position_size = quantity * entry_price

    if position_size < float(config.MIN_TRADE_USD):
        return None

    return {
        "quantity": round(quantity, 6),
        "entry_price": round(entry_price, 4),
        "stop_loss": round(stop_loss, 4),
        "position_size": round(position_size, 2),
        "risk": round(quantity * risk_per_share, 2),
        "account_equity": round(balance, 2),
        "effective_equity": round(balance, 2),
        "virtual_trading_capital": round(balance, 2),
        "broker_account_equity": round(broker_account_equity, 2),
        "risk_calculation_basis": "virtual_trading_capital",
        "used": round(used, 2),
        "reserve": round(reserve, 2),
        "available": round(available, 2),
        "size_factor": round(size_factor, 2),
    }


def execute_limit_buy_sync(
    symbol: str,
    quantity: float,
    limit_price: float,
    execution_payload: dict | None = None,
) -> dict:
    symbol = str(symbol).strip().upper()
    require_paper_auto_trading_allowed("AUTO BUY")
    require_buy_allowed("auto_trader")
    require_market_hours_order_send_allowed("AUTO BUY")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ib = IB()

    try:
        client_id = int(config.IBKR_CLIENT_ID) + BUY_CLIENT_ID_OFFSET

        order_lifecycle.safe_record_order_lifecycle_event_sync({
            "symbol": symbol,
            "side": "BUY",
            "quantity": quantity,
            "price": limit_price,
            "client_id": client_id,
            "source_module": "auto_trader.execute_limit_buy_sync",
            "state": order_lifecycle.OrderState.CREATED,
            "reason": "AUTO BUY limit order created locally before TWS submission",
            "raw_payload": {"symbol": symbol, "quantity": quantity, "limit_price": limit_price},
        })

        ib.connect(
            config.IBKR_HOST,
            int(config.IBKR_PORT),
            clientId=client_id,
            timeout=10,
        )

        ib.reqMarketDataType(
            int(getattr(config, "IBKR_MARKET_DATA_TYPE", 1))
        )
        protection = validate_buy_before_order(
            ib=ib,
            symbol=symbol,
            limit_price=float(limit_price),
            row=execution_payload or {},
        )

        if not protection.get("allowed"):
            order_lifecycle.safe_record_order_lifecycle_event_sync({
                "symbol": symbol,
                "side": "BUY",
                "quantity": quantity,
                "price": limit_price,
                "client_id": client_id,
                "source_module": "auto_trader.execute_limit_buy_sync",
                "state": order_lifecycle.OrderState.REJECTED,
                "reason": protection.get("reason") or "Quote protection blocked order before submission",
                "raw_payload": protection,
            })
            database.safe_record_trade_journal_event_sync({
                "symbol": symbol,
                "event_type": "EXECUTION_BLOCK_BUY" if protection.get("execution_quality", {}).get("blocks_buy") else "BUY_BLOCKED_BY_SAFETY_GATE",
                "decision": "BLOCKED",
                "reason": protection.get("reason") or "Quote protection blocked order before submission",
                "source_module": "auto_trader.execute_limit_buy_sync",
                "price": limit_price,
                "quantity": quantity,
                "raw_payload": protection,
            })
            return {
                "symbol": symbol,
                "order_id": None,
                "action": "BUY",
                "order_type": "LMT",
                "limit_price": float(limit_price),
                "status": "BLOCKED_PROTECTION",
                "filled": 0.0,
                "remaining": 0.0,
                "avg_fill_price": 0.0,
                "reason": protection.get("reason"),
                "quote": protection.get("quote"),
                "execution_quality": protection.get("execution_quality"),
            }

        ib.reqAllOpenOrders()
        ib.sleep(1)

        for trade in ib.openTrades():
            existing_symbol = getattr(trade.contract, "symbol", "").upper()
            action = str(trade.order.action or "").upper()
            status = str(trade.orderStatus.status or "")

            if (
                existing_symbol == symbol
                and action == "BUY"
                and status in OPEN_ORDER_STATUSES
            ):
                order_lifecycle.safe_record_order_lifecycle_event_sync({
                    "symbol": symbol,
                    "side": "BUY",
                    "quantity": quantity,
                    "price": limit_price,
                    "order_id": trade.order.orderId,
                    "perm_id": getattr(trade.order, "permId", None),
                    "client_id": client_id,
                    "source_module": "auto_trader.execute_limit_buy_sync",
                    "state": order_lifecycle.OrderState.ACKNOWLEDGED,
                    "reason": "Existing pending BUY order found; no duplicate order submitted",
                    "raw_payload": {"status": status, "filled": trade.orderStatus.filled, "remaining": trade.orderStatus.remaining},
                })
                return {
                    "symbol": symbol,
                    "order_id": trade.order.orderId,
                    "action": "BUY",
                    "order_type": "LMT",
                    "limit_price": limit_price,
                    "status": "PENDING_EXISTS",
                    "filled": float(trade.orderStatus.filled or 0),
                    "remaining": float(trade.orderStatus.remaining or 0),
                    "avg_fill_price": float(trade.orderStatus.avgFillPrice or 0),
                }

        contract = Stock(
            symbol,
            "SMART",
            "USD",
        )

        ib.qualifyContracts(contract)

        order = LimitOrder(
            "BUY",
            float(quantity),
            float(limit_price),
            tif="DAY",
        )

        trade = ib.placeOrder(
            contract,
            order,
        )

        order_lifecycle.safe_record_order_lifecycle_event_sync({
            "symbol": symbol,
            "side": "BUY",
            "quantity": quantity,
            "price": limit_price,
            "order_id": trade.order.orderId,
            "perm_id": getattr(trade.order, "permId", None),
            "client_id": client_id,
            "source_module": "auto_trader.execute_limit_buy_sync",
            "state": order_lifecycle.OrderState.SUBMITTED,
            "reason": "AUTO BUY limit order submitted to TWS",
            "raw_payload": {"order": getattr(trade.order, "__dict__", {})},
        })

        ib.sleep(3)

        result = {
            "symbol": symbol,
            "order_id": trade.order.orderId,
            "perm_id": getattr(trade.order, "permId", None),
            "action": "BUY",
            "order_type": "LMT",
            "limit_price": float(limit_price),
            "status": str(trade.orderStatus.status or ""),
            "filled": float(trade.orderStatus.filled or 0),
            "remaining": float(trade.orderStatus.remaining or 0),
            "avg_fill_price": float(trade.orderStatus.avgFillPrice or 0),
        }

        order_lifecycle.safe_record_order_lifecycle_event_sync({
            "symbol": symbol,
            "side": "BUY",
            "quantity": quantity,
            "price": limit_price,
            "order_id": result["order_id"],
            "perm_id": result["perm_id"],
            "client_id": client_id,
            "source_module": "auto_trader.execute_limit_buy_sync",
            "state": order_lifecycle.map_ibkr_status_to_state(result["status"], result["filled"], result["remaining"]),
            "reason": f"TWS order status after submission: {result['status']}",
            "raw_payload": result,
        })

        return result

    except Exception as exc:
        order_lifecycle.safe_record_order_lifecycle_event_sync({
            "symbol": symbol,
            "side": "BUY",
            "quantity": quantity,
            "price": limit_price,
            "source_module": "auto_trader.execute_limit_buy_sync",
            "state": order_lifecycle.OrderState.FAILED,
            "reason": str(exc),
            "raw_payload": {"symbol": symbol, "quantity": quantity, "limit_price": limit_price, "error": str(exc)},
        })
        raise

    finally:
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass

        try:
            loop.close()
        except Exception:
            pass


async def process_auto_trading(scan_results: list[dict]) -> None:
    if not AUTO_TRADING_ENABLED:
        return

    persisted_auto_trading = await database.get_app_state("auto_trading_enabled", "true")
    if str(persisted_auto_trading).lower() != "true":
        log.info("AUTO TRADER disabled by persisted app state")
        return

    from circuit_breaker import get_circuit_breaker_state
    from startup_recovery import startup_recovery_passed

    circuit = await get_circuit_breaker_state()
    if circuit.get("tripped"):
        log.warning("AUTO TRADER blocked by circuit breaker: %s", circuit.get("reason"))
        return

    if not await startup_recovery_passed():
        log.warning("AUTO TRADER blocked: startup recovery has not passed")
        return

    from watchdog import get_watchdog_status

    watchdog_status = await get_watchdog_status()
    if watchdog_status.get("trading_blocked"):
        reason = "; ".join(watchdog_status.get("blocking_reasons") or ["Watchdog blocked trading"])
        log.warning("AUTO TRADER blocked by watchdog: %s", reason)
        await database.safe_record_trade_journal_event({
            "event_type": "AUTO_TRADING_BLOCKED_BY_WATCHDOG",
            "decision": "BLOCKED",
            "reason": reason,
            "source_module": "auto_trader.process_auto_trading",
            "raw_payload": watchdog_status,
        })
        return

    if not PAPER_TRADING_ENABLED:
        log.warning("Real trading is disabled. PAPER_TRADING_ENABLED must stay True.")
        return

    market_hours = get_market_hours_status()
    market_is_open = bool(market_hours.get("allowed"))

    if not market_is_open:
        log.warning(
            "AUTO BUY BLOCKED — %s. SELL management remains active.",
            market_hours.get("reason"),
        )

    market = get_market_regime()
    active_strategy_mode = await strategy_mode.get_strategy_mode()
    active_rules = strategy_mode.active_rules(active_strategy_mode)

    regime = market.get("regime")
    allow_new_buys = market.get("allow_new_buys", True)
    min_score_override = int(market.get("min_score_override", MIN_SCORE_TO_BUY))
    size_factor = float(market.get("position_size_factor", 1.0))
    if strategy_mode.is_intraday_mode(active_strategy_mode):
        min_score_override = int(active_rules["min_score_to_buy"])
        size_factor *= float(active_rules["position_size_factor"])

    open_positions = await database.get_open_positions()

    open_symbols = {
        str(p.get("symbol", "")).upper()
        for p in open_positions
        if p.get("symbol")
    }

    realized_pnl = await database.get_realized_pnl()
    account_equity = float(config.effective_virtual_trading_capital())

    current_open_count = len(open_positions)
    max_positions = int(active_rules.get("max_open_positions", getattr(config, "MAX_OPEN_POSITIONS", 10)))

    log.info(
        "AUTO TRADER | regime=%s | allow_buys=%s | min_score=%s | equity=$%s | open=%s/%s",
        regime,
        allow_new_buys,
        min_score_override,
        round(account_equity, 2),
        current_open_count,
        max_positions,
    )

    for row in scan_results:
        symbol = str(row.get("symbol", "")).strip().upper()
        signal = row.get("signal")
        intraday_mode_active = strategy_mode.is_intraday_mode(active_strategy_mode)
        if intraday_mode_active:
            intraday_entry = intraday_momentum_engine.detect_intraday_entry_setup(row)
            row = {**row, **intraday_entry, "intraday_score_reasons": intraday_entry.get("score_reasons", [])}
            score = int(
                row.get("aggressive_score")
                or row.get("intraday_aggressive_score")
                or intraday_entry.get("intraday_momentum_score")
                or row.get("intraday_momentum_score")
                or 0
            )
            if row.get("regime_override_active"):
                await database.safe_record_trade_journal_event({
                    "symbol": symbol or "UNKNOWN",
                    "event_type": "REGIME_OVERRIDE_ACTIVATED",
                    "decision": "OVERRIDE",
                    "reason": "HIGH_MOMENTUM_EXCEPTION",
                    "source_module": "auto_trader",
                    "raw_payload": row,
                })
        else:
            score = int(row.get("weekly_score") or row.get("score") or 0)

        if not symbol:
            continue

        aggressive_entry_allowed = bool(row.get("aggressive_entry_allowed"))
        intraday_entry_allowed = bool(row.get("intraday_entry_allowed"))
        legacy_entry_allowed = bool(row.get("entry_allowed"))
        intraday_candidate = aggressive_entry_allowed or intraday_entry_allowed or legacy_entry_allowed or (signal == "BUY")

        if intraday_mode_active and intraday_candidate:
            await database.safe_record_trade_journal_event({
                "symbol": symbol,
                "event_type": "AUTO_TRADER_INTRADAY_CANDIDATE_DETECTED",
                "decision": "CANDIDATE",
                "reason": "intraday_candidate_detected",
                "source_module": "auto_trader",
                "raw_payload": {
                    "symbol": symbol,
                    "aggressive_entry_allowed": aggressive_entry_allowed,
                    "intraday_entry_allowed": intraday_entry_allowed,
                    "entry_allowed": legacy_entry_allowed,
                    "aggressive_score": row.get("aggressive_score"),
                    "intraday_aggressive_score": row.get("intraday_aggressive_score"),
                    "intraday_momentum_score": row.get("intraday_momentum_score"),
                    "rejection_reasons": row.get("rejection_reasons") or [],
                },
            })
        elif intraday_mode_active:
            await database.safe_record_trade_journal_event({
                "symbol": symbol,
                "event_type": "AUTO_TRADER_INTRADAY_CANDIDATE_SKIPPED",
                "decision": "SKIPPED",
                "reason": "no_aggressive_or_intraday_entry_allowed",
                "source_module": "auto_trader",
                "raw_payload": {
                    "symbol": symbol,
                    "aggressive_entry_allowed": aggressive_entry_allowed,
                    "intraday_entry_allowed": intraday_entry_allowed,
                    "entry_allowed": legacy_entry_allowed,
                    "signal": signal,
                    "rejection_reasons": row.get("rejection_reasons") or [],
                },
            })

        is_buy_candidate = intraday_candidate if intraday_mode_active else (signal == "BUY")

        if is_buy_candidate:
            if not market_is_open:
                reason = market_hours.get("reason") or "US regular market is closed"
                log.info("AUTO BUY skipped for %s — %s", symbol, reason)
                await _journal_buy_decision(
                    row,
                    "BUY_BLOCKED_BY_SAFETY_GATE",
                    "BLOCKED",
                    reason,
                    market,
                    {"market_hours": market_hours},
                )
                continue

            if not allow_new_buys:
                log.info("AUTO BUY blocked for %s — market regime is %s", symbol, regime)
                await _journal_buy_decision(
                    row,
                    "BUY_CANDIDATE_REJECTED",
                    "REJECTED",
                    f"Market regime blocks new buys: {regime}",
                    market,
                )
                continue

            if (not strategy_mode.is_intraday_mode(active_strategy_mode)) and score < min_score_override:
                log.info(
                    "AUTO BUY skipped for %s — score too low (%s < %s)",
                    symbol,
                    score,
                    min_score_override,
                )
                await _journal_buy_decision(
                    row,
                    "BUY_CANDIDATE_REJECTED",
                    "REJECTED",
                    f"Score too low ({score} < {min_score_override})",
                    market,
                )
                continue

            if intraday_mode_active:
                if not (aggressive_entry_allowed or intraday_entry_allowed or legacy_entry_allowed):
                    reason = "; ".join(row.get("rejection_reasons") or ["Intraday BUY blocked"])
                    await database.safe_record_trade_journal_event({
                        "symbol": symbol,
                        "event_type": "AGGRESSIVE_REJECTION",
                        "decision": "BLOCKED",
                        "reason": reason,
                        "source_module": "auto_trader",
                        "raw_payload": row,
                    })
                    log.info("AUTO BUY skipped for %s — %s", symbol, reason)
                    await _journal_buy_decision(
                        row,
                        "INTRADAY_BUY_BLOCKED",
                        "BLOCKED",
                        reason,
                        market,
                        {"intraday_decision": row},
                    )
                    continue

            if symbol in open_symbols:
                log.info("AUTO BUY skipped for %s — position already open", symbol)
                await _journal_buy_decision(
                    row,
                    "BUY_CANDIDATE_REJECTED",
                    "REJECTED",
                    "Position already open",
                    market,
                )
                continue

            if current_open_count >= max_positions:
                log.info("AUTO BUY skipped for %s — max positions reached", symbol)
                await _journal_buy_decision(
                    row,
                    "BUY_CANDIDATE_REJECTED",
                    "REJECTED",
                    f"Max positions reached {current_open_count}/{max_positions}",
                    market,
                )
                continue

            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_ACCEPTED",
                "ACCEPTED",
                "BUY candidate passed auto-trading filters",
                market,
            )
            await database.safe_record_trade_journal_event({
                "symbol": symbol,
                "event_type": "AGGRESSIVE_ENTRY_ACCEPTED",
                "decision": "ACCEPTED",
                "reason": "; ".join(row.get("score_reasons") or ["intraday_accepted"]),
                "source_module": "auto_trader",
                "raw_payload": row,
            })

            opened = await auto_open_position(
                row=row,
                open_positions=open_positions,
                account_equity=account_equity,
                size_factor=size_factor,
                market={**market, "strategy_mode": active_strategy_mode.value},
            )

            if opened:
                current_open_count += 1
                open_symbols.add(symbol)
                open_positions = await database.get_open_positions()
                realized_pnl = await database.get_realized_pnl()
                account_equity = float(config.effective_virtual_trading_capital())

        elif signal == "SELL":
            if symbol in open_symbols:
                await database.safe_record_trade_journal_event({
                    "symbol": symbol,
                    "event_type": "SELL_SIGNAL_DETECTED",
                    "decision": "CLOSE",
                    "reason": "SELL signal",
                    "source_module": "auto_trader",
                    "signal_score": row.get("score"),
                    "weekly_score": row.get("weekly_score"),
                    "market_regime": regime,
                    "price": row.get("price"),
                    "raw_payload": row,
                })

                closed = await auto_close_position(symbol, "SELL signal")

                if closed:
                    open_positions = await database.get_open_positions()

                    open_symbols = {
                        str(p.get("symbol", "")).upper()
                        for p in open_positions
                        if p.get("symbol")
                    }

                    current_open_count = len(open_positions)
                    realized_pnl = await database.get_realized_pnl()
                    account_equity = float(config.effective_virtual_trading_capital())


async def auto_open_position(
    row: dict,
    open_positions: list[dict],
    account_equity: float,
    size_factor: float = 1.0,
    market: dict | None = None,
) -> bool:
    async with _buy_lock:
        symbol = str(row.get("symbol", "")).strip().upper()

        limit_price = float(row.get("price") or row.get("entry_price") or 0)
        execution_quality = evaluate_execution_quality(
            row=row,
            quote=None,
            limit_price=limit_price,
            symbol=symbol,
        )
        for exec_event in execution_quality.get("journal_events") or []:
            await _journal_buy_decision(
                row,
                exec_event,
                "INFO",
                "Execution volume metrics fallback/normalization applied",
                market,
                {"execution_quality": execution_quality},
            )
        portfolio_risk = await portfolio_risk_engine.get_portfolio_risk()
        sizing = evaluate_position_sizing(PositionSizingInput(
            row=row,
            open_positions=open_positions,
            account_equity=account_equity,
            market_regime=market or {},
            execution_quality=execution_quality,
            portfolio_risk=portfolio_risk,
            size_factor=size_factor,
        ))
        await record_position_sizing_event(symbol, sizing)

        if not symbol or not sizing:
            log.info("AUTO BUY skipped for %s — sizing failed", symbol)
            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_REJECTED",
                "REJECTED",
                "Position sizing failed",
                market,
            )
            return False

        if execution_quality.get("blocks_buy"):
            reason = execution_quality.get("blocked_buy_reason") or "Execution quality blocked BUY"
            log.warning("AUTO BUY blocked for %s — %s", symbol, reason)
            log.warning(
                "AUTO BUY EXECUTION PAYLOAD SNAPSHOT | symbol=%s | missing_fields=%s | payload=%s",
                symbol,
                execution_quality.get("missing_fields"),
                execution_quality.get("payload_snapshot"),
            )
            await database.set_app_state(
                f"execution_quality_state:{symbol}",
                execution_quality.get("state"),
            )
            await _journal_buy_decision(
                row,
                "EXECUTION_BLOCK_BUY",
                "BLOCKED",
                reason,
                market,
                {
                    "sizing": sizing,
                    "execution_quality": execution_quality,
                    "portfolio_risk": portfolio_risk,
                },
            )
            return False

        if sizing.get("blocks_buy"):
            reason = "; ".join(sizing.get("block_reasons") or []) or "Position sizing blocked BUY"
            log.warning("AUTO BUY blocked for %s — %s", symbol, reason)
            await _journal_buy_decision(
                row,
                "POSITION_SIZE_BLOCKED",
                "BLOCKED",
                reason,
                market,
                {"sizing": sizing, "execution_quality": execution_quality, "portfolio_risk": portfolio_risk},
            )
            return False

        quantity = float(sizing.get("recommended_share_quantity", 0) or 0)

        if quantity <= 0:
            log.info("AUTO BUY skipped for %s — invalid quantity", symbol)
            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_REJECTED",
                "REJECTED",
                "Invalid quantity",
                market,
                {"quantity": quantity, "sizing": sizing},
            )
            return False

        limit_price = float(sizing["entry_price"])
        execution_quality = evaluate_execution_quality(
            row={**row, "quantity": quantity},
            quote=None,
            limit_price=limit_price,
            symbol=symbol,
        )
        for exec_event in execution_quality.get("journal_events") or []:
            await _journal_buy_decision(
                row,
                exec_event,
                "INFO",
                "Execution volume metrics fallback/normalization applied",
                market,
                {"quantity": quantity, "limit_price": limit_price, "execution_quality": execution_quality},
            )
        sizing["execution_quality_context"] = execution_quality
        previous_execution_state = await database.get_app_state(
            f"execution_quality_state:{symbol}",
            "",
        )
        await database.set_app_state(
            f"execution_quality_state:{symbol}",
            execution_quality.get("state"),
        )

        if execution_quality.get("blocks_buy"):
            reason = execution_quality.get("blocked_buy_reason") or "Execution quality blocked BUY"
            log.warning("AUTO BUY blocked for %s — %s", symbol, reason)
            log.warning(
                "AUTO BUY EXECUTION PAYLOAD SNAPSHOT | symbol=%s | missing_fields=%s | payload=%s",
                symbol,
                execution_quality.get("missing_fields"),
                execution_quality.get("payload_snapshot"),
            )
            await _journal_buy_decision(
                row,
                "EXECUTION_BLOCK_BUY",
                "BLOCKED",
                reason,
                market,
                {
                    "quantity": quantity,
                    "limit_price": limit_price,
                    "sizing": sizing,
                    "execution_quality": execution_quality,
                },
            )
            return False

        if execution_quality.get("state") in {"EXECUTION_WARNING", "EXECUTION_DANGER"}:
            await _journal_buy_decision(
                row,
                "EXECUTION_WARNING",
                "WARNING",
                "; ".join(execution_quality.get("warnings") or execution_quality.get("dangers") or ["Execution quality warning"]),
                market,
                {
                    "quantity": quantity,
                    "limit_price": limit_price,
                    "sizing": sizing,
                    "execution_quality": execution_quality,
                },
            )
        elif previous_execution_state == "EXECUTION_BLOCK_BUY":
            await _journal_buy_decision(
                row,
                "EXECUTION_RECOVERED",
                "RECOVERED",
                "Execution quality recovered; BUY may continue through remaining safety gates",
                market,
                {
                    "quantity": quantity,
                    "limit_price": limit_price,
                    "sizing": sizing,
                    "execution_quality": execution_quality,
                },
            )

        try:
            await portfolio_risk_engine.require_new_buy_allowed(symbol)
        except RuntimeError as exc:
            log.warning("AUTO BUY blocked for %s — %s", symbol, exc)
            await _journal_buy_decision(
                row,
                "RISK_BLOCK_BUY",
                "BLOCKED",
                str(exc),
                market,
                {"quantity": quantity, "sizing": sizing, "portfolio_risk": portfolio_risk},
            )
            return False

        if not config.AUTO_SEND_ORDERS:
            log.warning("AUTO BUY blocked for %s — AUTO_SEND_ORDERS is false", symbol)
            await _journal_buy_decision(
                row,
                "BUY_BLOCKED_BY_SAFETY_GATE",
                "BLOCKED",
                "AUTO_SEND_ORDERS is false",
                market,
                {"quantity": quantity, "sizing": sizing},
            )
            return False

        try:
            order_result = await asyncio.to_thread(
                execute_limit_buy_sync,
                symbol,
                quantity,
                limit_price,
                row,
            )

        except Exception as e:
            log.exception("AUTO BUY execution failed for %s | %s", symbol, e)
            event_type = (
                "BUY_BLOCKED_BY_SAFETY_GATE"
                if "blocked:" in str(e)
                else "BUY_CANDIDATE_REJECTED"
            )
            await _journal_buy_decision(
                row,
                event_type,
                "BLOCKED" if event_type == "BUY_BLOCKED_BY_SAFETY_GATE" else "REJECTED",
                str(e),
                market,
                {"quantity": quantity, "limit_price": limit_price, "sizing": sizing},
            )
            return False

        if not order_result:
            log.warning("AUTO BUY blocked for %s — empty TWS result", symbol)
            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_REJECTED",
                "REJECTED",
                "Empty TWS result",
                market,
                {"quantity": quantity, "limit_price": limit_price, "sizing": sizing},
            )
            return False

        order_status = str(order_result.get("status") or "")
        filled = float(order_result.get("filled") or 0)
        avg_fill_price = float(order_result.get("avg_fill_price") or 0)

        log.warning(
            "AUTO BUY TWS RESULT | %s | status=%s | filled=%s | avg=%s",
            symbol,
            order_status,
            filled,
            avg_fill_price,
        )

        if order_status == "PENDING_EXISTS":
            log.warning(
                "AUTO BUY skipped for %s — pending BUY order already exists",
                symbol,
            )
            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_REJECTED",
                "REJECTED",
                "Pending BUY order already exists",
                market,
                {"quantity": quantity, "order_result": order_result, "sizing": sizing},
            )
            return False

        if order_result.get("order_id"):
            await _journal_buy_decision(
                row,
                "BUY_ORDER_SUBMITTED",
                "SUBMITTED",
                f"TWS order status: {order_status}",
                market,
                {"quantity": quantity, "order_result": order_result, "sizing": sizing},
            )

        if order_status not in FILLED_STATUSES or filled <= 0:
            log.warning(
                "AUTO BUY NOT SAVED TO DB | %s | order accepted but not filled yet | status=%s",
                symbol,
                order_status,
            )
            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_REJECTED",
                "REJECTED",
                f"Order accepted but not filled: {order_status}",
                market,
                {"quantity": quantity, "order_result": order_result, "sizing": sizing},
            )
            return False

        real_entry_price = avg_fill_price if avg_fill_price > 0 else limit_price

        payload = {
            "symbol": symbol,
            "buy_price": real_entry_price,
            "entry_price": real_entry_price,
            "quantity": round(filled, 6),
            "current_price": real_entry_price,
            "stop_loss": sizing["stop_loss"],
            "take_profit_1": row.get("take_profit_1"),
            "take_profit_2": row.get("take_profit_2"),
            "reason": "AUTO BUY FILLED IN TWS",
            "notes": (
                f"AUTO TRADE FILLED | "
                f"order_id={order_result.get('order_id')} "
                f"| status={order_status} "
                f"| filled={filled} "
                f"| avg_fill={real_entry_price} "
                f"| equity=${sizing['account_equity']} "
                f"| available=${sizing['available']} "
                f"| risk=${sizing['risk']}"
            ),
        }

        try:
            await database.add_position(
                payload,
                max_open_positions=int(strategy_mode.active_rules((market or {}).get("strategy_mode")).get("max_open_positions", getattr(config, "MAX_OPEN_POSITIONS", 10))),
            )

            log.info(
                "AUTO BUY SAVED TO DB AFTER FILL | %s | qty=%s | entry=%s",
                symbol,
                filled,
                real_entry_price,
            )

            await _journal_buy_decision(
                row,
                "BUY_ORDER_FILLED",
                "FILLED",
                "AUTO BUY filled in TWS and saved to DB",
                market,
                {
                    "quantity": filled,
                    "price": real_entry_price,
                    "order_result": order_result,
                    "position_payload": payload,
                    "sizing": sizing,
                },
            )

            return True

        except Exception as e:
            log.warning("AUTO BUY filled but DB save failed for %s: %s", symbol, e)
            await _journal_buy_decision(
                row,
                "BUY_CANDIDATE_REJECTED",
                "REJECTED",
                f"Order filled but DB save failed: {e}",
                market,
                {
                    "quantity": filled,
                    "price": real_entry_price,
                    "order_result": order_result,
                    "position_payload": payload,
                    "sizing": sizing,
                },
            )
            return False


async def auto_close_position(symbol: str, reason: str) -> bool:
    from watchdog import get_watchdog_status

    watchdog_status = await get_watchdog_status()
    if watchdog_status.get("trading_blocked"):
        block_reason = "; ".join(watchdog_status.get("blocking_reasons") or ["Watchdog blocked trading"])
        log.warning("AUTO SELL/CLOSE blocked for %s by watchdog: %s", symbol, block_reason)
        await database.safe_record_trade_journal_event({
            "symbol": symbol,
            "event_type": "SELL_BLOCKED_BY_WATCHDOG",
            "decision": "BLOCKED",
            "reason": block_reason,
            "source_module": "auto_trader.auto_close_position",
            "raw_payload": {"requested_reason": reason, "watchdog": watchdog_status},
        })
        return False

    try:
        await database.close_position(
            symbol,
            reason=f"AUTO: {reason}",
        )

        log.info("AUTO CLOSE: %s | %s", symbol, reason)
        return True

    except Exception as e:
        log.warning("AUTO CLOSE failed for %s: %s", e)
        return False

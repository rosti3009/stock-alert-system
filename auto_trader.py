from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from ib_insync import IB, Stock, LimitOrder
from trade_protection import validate_buy_before_order

import config
import database
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


def is_us_regular_market_open() -> bool:
    now = datetime.now(ZoneInfo("America/New_York"))

    if now.weekday() >= 5:
        return False

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now <= market_close


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

    balance = float(account_equity)
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
        "used": round(used, 2),
        "reserve": round(reserve, 2),
        "available": round(available, 2),
        "size_factor": round(size_factor, 2),
    }


def execute_limit_buy_sync(
    symbol: str,
    quantity: float,
    limit_price: float,
) -> dict:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ib = IB()

    try:
        client_id = int(config.IBKR_CLIENT_ID) + BUY_CLIENT_ID_OFFSET

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
        )

        if not protection.get("allowed"):
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

        ib.sleep(3)

        return {
            "symbol": symbol,
            "order_id": trade.order.orderId,
            "action": "BUY",
            "order_type": "LMT",
            "limit_price": float(limit_price),
            "status": str(trade.orderStatus.status or ""),
            "filled": float(trade.orderStatus.filled or 0),
            "remaining": float(trade.orderStatus.remaining or 0),
            "avg_fill_price": float(trade.orderStatus.avgFillPrice or 0),
        }

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

    if not PAPER_TRADING_ENABLED:
        log.warning("Real trading is disabled. PAPER_TRADING_ENABLED must stay True.")
        return

    market_is_open = is_us_regular_market_open()

    if not market_is_open:
        log.warning(
            "AUTO BUY BLOCKED — US regular market is closed. SELL management remains active."
        )

    market = get_market_regime()

    regime = market.get("regime")
    allow_new_buys = market.get("allow_new_buys", True)
    min_score_override = int(market.get("min_score_override", MIN_SCORE_TO_BUY))
    size_factor = float(market.get("position_size_factor", 1.0))

    open_positions = await database.get_open_positions()

    open_symbols = {
        str(p.get("symbol", "")).upper()
        for p in open_positions
        if p.get("symbol")
    }

    realized_pnl = await database.get_realized_pnl()
    account_equity = float(config.ACCOUNT_BALANCE) + float(realized_pnl)

    current_open_count = len(open_positions)
    max_positions = int(getattr(config, "MAX_OPEN_POSITIONS", 10))

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
        score = int(row.get("weekly_score") or row.get("score") or 0)

        if not symbol:
            continue

        if signal == "BUY":
            if not market_is_open:
                log.info("AUTO BUY skipped for %s — US market closed", symbol)
                continue

            if not allow_new_buys:
                log.info("AUTO BUY blocked for %s — market regime is %s", symbol, regime)
                continue

            if score < min_score_override:
                log.info(
                    "AUTO BUY skipped for %s — score too low (%s < %s)",
                    symbol,
                    score,
                    min_score_override,
                )
                continue

            if symbol in open_symbols:
                log.info("AUTO BUY skipped for %s — position already open", symbol)
                continue

            if current_open_count >= max_positions:
                log.info("AUTO BUY skipped for %s — max positions reached", symbol)
                continue

            opened = await auto_open_position(
                row=row,
                open_positions=open_positions,
                account_equity=account_equity,
                size_factor=size_factor,
            )

            if opened:
                current_open_count += 1
                open_symbols.add(symbol)
                open_positions = await database.get_open_positions()
                realized_pnl = await database.get_realized_pnl()
                account_equity = float(config.ACCOUNT_BALANCE) + float(realized_pnl)

        elif signal == "SELL":
            if symbol in open_symbols:
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
                    account_equity = float(config.ACCOUNT_BALANCE) + float(realized_pnl)


async def auto_open_position(
    row: dict,
    open_positions: list[dict],
    account_equity: float,
    size_factor: float = 1.0,
) -> bool:
    async with _buy_lock:
        symbol = str(row.get("symbol", "")).strip().upper()

        sizing = calculate_position_size(
            row=row,
            open_positions=open_positions,
            account_equity=account_equity,
            size_factor=size_factor,
        )

        if not symbol or not sizing:
            log.info("AUTO BUY skipped for %s — sizing failed", symbol)
            return False

        quantity = float(sizing.get("quantity", 0) or 0)

        if quantity <= 0:
            log.info("AUTO BUY skipped for %s — invalid quantity", symbol)
            return False

        limit_price = float(sizing["entry_price"])

        if not config.AUTO_SEND_ORDERS:
            log.warning("AUTO BUY blocked for %s — AUTO_SEND_ORDERS is false", symbol)
            return False

        try:
            order_result = await asyncio.to_thread(
                execute_limit_buy_sync,
                symbol,
                quantity,
                limit_price,
            )

        except Exception as e:
            log.exception("AUTO BUY execution failed for %s | %s", symbol, e)
            return False

        if not order_result:
            log.warning("AUTO BUY blocked for %s — empty TWS result", symbol)
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
            return False

        if order_status not in FILLED_STATUSES or filled <= 0:
            log.warning(
                "AUTO BUY NOT SAVED TO DB | %s | order accepted but not filled yet | status=%s",
                symbol,
                order_status,
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
                max_open_positions=int(getattr(config, "MAX_OPEN_POSITIONS", 10)),
            )

            log.info(
                "AUTO BUY SAVED TO DB AFTER FILL | %s | qty=%s | entry=%s",
                symbol,
                filled,
                real_entry_price,
            )

            return True

        except Exception as e:
            log.warning("AUTO BUY filled but DB save failed for %s: %s", symbol, e)
            return False


async def auto_close_position(symbol: str, reason: str) -> bool:
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
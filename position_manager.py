from __future__ import annotations

from datetime import datetime, timezone

import config
import strategy_mode


def evaluate_position(position: dict, market: dict, mode: str | None = None) -> dict:
    buy_price = float(position.get("buy_price", 0) or 0)
    quantity = float(position.get("quantity", 0) or 0)

    current_price = float(
        market.get("price")
        or position.get("current_price")
        or buy_price
    )

    stop_loss = float(position.get("stop_loss") or 0)
    take_profit_1 = float(position.get("take_profit_1") or 0)
    take_profit_2 = float(position.get("take_profit_2") or 0)

    highest_price = float(
        position.get("highest_price")
        or current_price
    )

    if quantity <= 0:
        quantity = 1

    if current_price > highest_price:
        highest_price = current_price

    profit_per_share = current_price - buy_price
    profit_amount = profit_per_share * quantity

    profit_percent = (
        (profit_per_share / buy_price) * 100
        if buy_price > 0
        else 0
    )

    action = "HOLD"
    reason = ""
    status = "OPEN"
    partial_sell = False
    sell_quantity = 0

    active_mode = mode or market.get("strategy_mode") or position.get("strategy_mode")
    if strategy_mode.is_intraday_mode(active_mode):
        return evaluate_intraday_position(
            position=position,
            market=market,
            buy_price=buy_price,
            quantity=quantity,
            current_price=current_price,
            stop_loss=stop_loss,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
            highest_price=highest_price,
            profit_amount=profit_amount,
            profit_percent=profit_percent,
        )

    # =========================
    # HARD STOP LOSS
    # =========================
    if stop_loss > 0 and current_price <= stop_loss:
        return {
            "current_price": current_price,
            "highest_price": round(highest_price, 4),
            "profit_amount": round(profit_amount, 2),
            "profit_percent": round(profit_percent, 2),
            "stop_loss": stop_loss,
            "take_profit_1": take_profit_1,
            "take_profit_2": take_profit_2,
            "status": "CLOSED",
            "action": "STOP_LOSS_HIT",
            "reason": "Stop loss triggered",
            "partial_sell": False,
            "sell_quantity": quantity,
        }

    # =========================
    # BREAK EVEN
    # =========================
    if profit_percent >= 3:
        new_stop = buy_price

        if new_stop > stop_loss:
            stop_loss = new_stop
            action = "MOVE_STOP_TO_BREAKEVEN"
            reason = "Stop moved to break even"

    # =========================
    # TAKE PROFIT 1
    # =========================
    if take_profit_1 > 0 and current_price >= take_profit_1:
        action = "TAKE_PROFIT_1"
        reason = "First target reached"
        partial_sell = True
        sell_quantity = round(quantity * 0.5, 6)
        stop_loss = max(stop_loss, buy_price)

    # =========================
    # TAKE PROFIT 2
    # =========================
    if take_profit_2 > 0 and current_price >= take_profit_2:
        return {
            "current_price": current_price,
            "highest_price": round(highest_price, 4),
            "profit_amount": round(profit_amount, 2),
            "profit_percent": round(profit_percent, 2),
            "stop_loss": stop_loss,
            "take_profit_1": take_profit_1,
            "take_profit_2": take_profit_2,
            "status": "CLOSED",
            "action": "TAKE_PROFIT_2",
            "reason": "Final target reached",
            "partial_sell": False,
            "sell_quantity": quantity,
        }

    # =========================
    # TRAILING STOP
    # =========================
    if profit_percent >= 5:
        trailing_stop = highest_price * 0.97

        if trailing_stop > stop_loss:
            stop_loss = trailing_stop
            action = "TRAILING_STOP_UPDATED"
            reason = "Trailing stop 3%"

    if profit_percent >= 10:
        trailing_stop = highest_price * 0.94

        if trailing_stop > stop_loss:
            stop_loss = trailing_stop
            action = "TRAILING_STOP_UPDATED"
            reason = "Trailing stop 6%"

    # =========================
    # SELL SIGNAL
    # =========================
    if market.get("signal") == "SELL":
        return {
            "current_price": current_price,
            "highest_price": round(highest_price, 4),
            "profit_amount": round(profit_amount, 2),
            "profit_percent": round(profit_percent, 2),
            "stop_loss": stop_loss,
            "take_profit_1": take_profit_1,
            "take_profit_2": take_profit_2,
            "status": "CLOSE_REQUESTED",
            "action": "SELL_SIGNAL",
            "reason": "Sell signal detected",
            "partial_sell": False,
            "sell_quantity": quantity,
        }

    # =========================
    # WARNING ZONES
    # =========================
    if profit_percent < -5 and action == "HOLD":
        action = "WARNING"
        reason = "Position under pressure"

    if profit_percent > 15 and action == "HOLD":
        action = "WATCH_PROFIT"
        reason = "Strong profit zone"

    return {
        "current_price": current_price,
        "highest_price": round(highest_price, 4),
        "profit_amount": round(profit_amount, 2),
        "profit_percent": round(profit_percent, 2),
        "stop_loss": round(stop_loss, 4) if stop_loss else stop_loss,
        "take_profit_1": take_profit_1,
        "take_profit_2": take_profit_2,
        "status": status,
        "action": action,
        "reason": reason,
        "partial_sell": partial_sell,
        "sell_quantity": sell_quantity,
    }

def evaluate_intraday_position(
    *,
    position: dict,
    market: dict,
    buy_price: float,
    quantity: float,
    current_price: float,
    stop_loss: float,
    take_profit_1: float,
    take_profit_2: float,
    highest_price: float,
    profit_amount: float,
    profit_percent: float,
) -> dict:
    """Tighter intraday exit engine: fast stops/targets, break-even, time, and EOD flat rule."""

    rules = strategy_mode.intraday_rules()
    if stop_loss <= 0 and buy_price > 0:
        stop_loss = buy_price * 0.985
    if take_profit_1 <= 0 and buy_price > 0:
        take_profit_1 = buy_price * 1.015
    if take_profit_2 <= 0 and buy_price > 0:
        take_profit_2 = buy_price * 1.03

    def payload(action: str, reason: str, status: str = "OPEN", partial: bool = False, sell_qty: float = 0) -> dict:
        return {
            "current_price": current_price,
            "highest_price": round(highest_price, 4),
            "profit_amount": round(profit_amount, 2),
            "profit_percent": round(profit_percent, 2),
            "stop_loss": round(stop_loss, 4) if stop_loss else stop_loss,
            "take_profit_1": round(take_profit_1, 4) if take_profit_1 else take_profit_1,
            "take_profit_2": round(take_profit_2, 4) if take_profit_2 else take_profit_2,
            "status": status,
            "action": action,
            "reason": reason,
            "partial_sell": partial,
            "sell_quantity": sell_qty,
            "exit_engine": "intraday_exit",
        }

    force_exit = strategy_mode.force_exit_before_close_status()
    if force_exit.get("active"):
        return payload("INTRADAY_FORCE_EXIT", "Intraday force close window before market close", "CLOSED", False, quantity)

    if stop_loss > 0 and current_price <= stop_loss:
        return payload("INTRADAY_STOP_LOSS_HIT", "Intraday tight stop loss triggered", "CLOSED", False, quantity)

    if profit_percent >= float(getattr(config, "INTRADAY_BREAK_EVEN_PROFIT_PERCENT", 1.0)):
        new_stop = buy_price
        if new_stop > stop_loss:
            stop_loss = new_stop
            return payload("INTRADAY_MOVE_STOP_TO_BREAKEVEN", "Intraday break-even protection activated")

    if take_profit_2 > 0 and current_price >= take_profit_2:
        return payload("INTRADAY_TAKE_PROFIT_FINAL", "Intraday final target reached", "CLOSED", False, quantity)

    if take_profit_1 > 0 and current_price >= take_profit_1:
        stop_loss = max(stop_loss, buy_price)
        return payload("INTRADAY_TAKE_PROFIT_FAST", "Intraday fast target reached", "OPEN", True, round(quantity * 0.5, 6))

    if profit_percent >= 2.0:
        trailing_stop = highest_price * 0.99
        if trailing_stop > stop_loss:
            stop_loss = trailing_stop
            return payload("INTRADAY_TRAILING_STOP_UPDATED", "Intraday trailing stop tightened")

    entered_at = position.get("opened_at") or position.get("created_at") or position.get("buy_time")
    age_minutes = _age_minutes(entered_at)
    if age_minutes is not None and age_minutes >= int(getattr(config, "INTRADAY_TIME_EXIT_MINUTES", 30)) and profit_percent <= 0.25:
        return payload("INTRADAY_TIME_EXIT", "Intraday time exit: trade did not move", "CLOSED", False, quantity)

    if market.get("signal") == "SELL":
        return payload("INTRADAY_SELL_SIGNAL", "Intraday sell signal detected", "CLOSE_REQUESTED", False, quantity)

    if not rules["allow_overnight"]:
        return payload("INTRADAY_HOLD", "Intraday position monitored; overnight holding disabled")

    return payload("HOLD", "")


def _age_minutes(value: object) -> float | None:
    if not value:
        return None
    try:
        entered = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if entered.tzinfo is None:
            entered = entered.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - entered.astimezone(timezone.utc)).total_seconds() / 60
    except Exception:
        return None

from __future__ import annotations


def evaluate_position(position: dict, market: dict) -> dict:
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
            "status": "CLOSED",
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
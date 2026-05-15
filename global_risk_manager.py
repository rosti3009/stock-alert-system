import logging

import config
import database


log = logging.getLogger(__name__)


# ==========================================
# GLOBAL RISK ENGINE
# ==========================================

async def get_global_risk_status():

    realized_pnl = float(
        await database.get_realized_pnl()
    )

    open_positions = await database.get_open_positions()

    unrealized_pnl = sum(
        float(p.get("profit_amount") or 0)
        for p in open_positions
    )

    account_balance = float(
        config.effective_virtual_trading_capital()
    )

    total_pnl = (
        realized_pnl
        + unrealized_pnl
    )

    equity = (
        account_balance
        + total_pnl
    )

    daily_drawdown_percent = 0

    if account_balance > 0:

        daily_drawdown_percent = (
            abs(total_pnl)
            / account_balance
        ) * 100

    risk_triggered = False

    risk_message = ""

    # ==========================================
    # GLOBAL LOSS PROTECTION
    # ==========================================

    if (
        config.ENABLE_GLOBAL_RISK_PROTECTION
        and total_pnl < 0
        and daily_drawdown_percent
        >= float(config.MAX_DAILY_LOSS_PERCENT)
    ):

        risk_triggered = True

        risk_message = (
            f"MAX DAILY LOSS HIT "
            f"({daily_drawdown_percent:.2f}%)"
        )

    # ==========================================
    # FINAL RESPONSE
    # ==========================================

    return {
        "enabled": config.ENABLE_GLOBAL_RISK_PROTECTION,
        "risk_triggered": risk_triggered,
        "risk_message": risk_message,
        "account_balance": round(account_balance, 2),
        "virtual_trading_capital": round(account_balance, 2),
        "risk_calculation_basis": "virtual_trading_capital",
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "total_pnl": round(total_pnl, 2),
        "equity": round(equity, 2),
        "daily_drawdown_percent": round(
            daily_drawdown_percent,
            2,
        ),
        "max_daily_loss_percent": float(
            config.MAX_DAILY_LOSS_PERCENT
        ),
    }


# ==========================================
# SHOULD BLOCK NEW BUYS
# ==========================================

async def should_block_new_trades():

    risk = await get_global_risk_status()

    return risk.get("risk_triggered", False)
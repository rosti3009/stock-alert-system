from __future__ import annotations

import config

PAPER_TWS_PORT = 7497


def require_paper_auto_trading_allowed(action: str = "Trading") -> None:
    """
    Enforce the shared paper-only execution gate for any path that can
    submit, cancel, or replace IBKR/TWS orders.

    This intentionally blocks live trading. Paper execution is allowed only
    when the app is explicitly configured for automated paper orders.
    """
    if str(config.TRADING_MODE or "").upper() == "OFF":
        raise RuntimeError(f"{action} blocked: TRADING_MODE is OFF")

    if not config.AUTO_SEND_ORDERS:
        raise RuntimeError(f"{action} blocked: AUTO_SEND_ORDERS is false")

    if not config.IBKR_PAPER_TRADING:
        raise RuntimeError(f"{action} blocked: IBKR_PAPER_TRADING is false")

    if config.IBKR_ENABLE_REAL_TRADING:
        raise RuntimeError(f"{action} blocked: LIVE trading is enabled")

    if int(config.IBKR_PORT) != PAPER_TWS_PORT:
        raise RuntimeError(
            f"{action} blocked: IBKR port is not Paper port {PAPER_TWS_PORT}"
        )

from __future__ import annotations

import config
import database

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

    require_circuit_breaker_clear(action)


def _circuit_breaker_is_tripped_sync() -> tuple[bool, str | None]:
    import json
    import sqlite3

    try:
        with sqlite3.connect(database.DB_PATH) as db:
            row = db.execute(
                "SELECT value FROM app_state WHERE key = ?",
                ("circuit_breaker_state",),
            ).fetchone()
        if not row:
            return False, None
        state = json.loads(row[0])
        return bool(state.get("tripped")), state.get("reason")
    except Exception as exc:
        if "no such table: app_state" in str(exc):
            return False, None
        return True, f"Circuit breaker state check failed: {exc}"


def require_circuit_breaker_clear(action: str = "Trading") -> None:
    tripped, reason = _circuit_breaker_is_tripped_sync()
    if tripped:
        raise RuntimeError(f"{action} blocked: circuit breaker tripped: {reason}")

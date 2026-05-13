from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import config
import database

PAPER_TWS_PORT = 7497


def _parse_market_time(value: str, name: str) -> time:
    try:
        return datetime.strptime(str(value).strip(), "%H:%M").time()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must use HH:MM format") from exc


def get_market_hours_status(now: datetime | None = None) -> dict:
    """
    Return the configured regular-session order guard status.

    Scanning can run regardless of this status. Automated order submission must
    require this status to be allowed before it talks to TWS/IBKR.
    """
    enabled = bool(getattr(config, "ENABLE_MARKET_HOURS_GUARD", True))
    timezone_name = str(getattr(config, "MARKET_TIMEZONE", "America/New_York"))
    open_time_value = str(getattr(config, "MARKET_OPEN_TIME", "09:30"))
    close_time_value = str(getattr(config, "MARKET_CLOSE_TIME", "16:00"))

    base_status = {
        "enabled": enabled,
        "timezone": timezone_name,
        "open_time": open_time_value,
        "close_time": close_time_value,
        "allowed": True,
        "reason": "Market hours guard disabled",
        "is_weekend": False,
        "local_time": None,
    }

    if not enabled:
        return base_status

    try:
        market_tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return {
            **base_status,
            "allowed": False,
            "reason": f"Invalid market timezone: {timezone_name}",
        }

    try:
        market_open_time = _parse_market_time(open_time_value, "MARKET_OPEN_TIME")
        market_close_time = _parse_market_time(close_time_value, "MARKET_CLOSE_TIME")
    except ValueError as exc:
        return {
            **base_status,
            "allowed": False,
            "reason": str(exc),
        }

    if market_open_time >= market_close_time:
        return {
            **base_status,
            "allowed": False,
            "reason": "MARKET_OPEN_TIME must be before MARKET_CLOSE_TIME",
        }

    if now is None:
        market_now = datetime.now(market_tz)
    elif now.tzinfo is None:
        market_now = now.replace(tzinfo=market_tz)
    else:
        market_now = now.astimezone(market_tz)

    local_time = market_now.time()
    local_time_label = market_now.strftime("%Y-%m-%d %H:%M:%S %Z")
    open_label = market_open_time.strftime("%H:%M")
    close_label = market_close_time.strftime("%H:%M")

    if market_now.weekday() >= 5:
        return {
            **base_status,
            "allowed": False,
            "reason": (
                f"US regular market is closed on weekends "
                f"({local_time_label}; session {open_label}-{close_label} {timezone_name})"
            ),
            "is_weekend": True,
            "local_time": local_time_label,
        }

    if local_time < market_open_time:
        return {
            **base_status,
            "allowed": False,
            "reason": (
                f"US regular market is not open yet "
                f"({local_time_label}; opens {open_label} {timezone_name})"
            ),
            "local_time": local_time_label,
        }

    if local_time > market_close_time:
        return {
            **base_status,
            "allowed": False,
            "reason": (
                f"US regular market is closed after regular session "
                f"({local_time_label}; closed {close_label} {timezone_name})"
            ),
            "local_time": local_time_label,
        }

    return {
        **base_status,
        "allowed": True,
        "reason": (
            f"US regular market is open "
            f"({local_time_label}; session {open_label}-{close_label} {timezone_name})"
        ),
        "local_time": local_time_label,
    }


def require_market_hours_order_send_allowed(action: str = "Trading") -> None:
    status = get_market_hours_status()
    if not status.get("allowed"):
        raise RuntimeError(f"{action} blocked: {status.get('reason')}")


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

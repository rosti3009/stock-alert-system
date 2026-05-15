from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import aiosqlite

import config
import database

log = logging.getLogger(__name__)

CIRCUIT_BREAKER_STATE_KEY = "circuit_breaker_state"
IBKR_ERROR_COUNT_KEY = "circuit_breaker_ibkr_error_count"
IBKR_LAST_ERROR_KEY = "circuit_breaker_ibkr_last_error"
IBKR_THRESHOLD_STATE_KEY = "circuit_breaker_ibkr_threshold_state"
IBKR_ERROR_STATE_KEYS = (
    IBKR_ERROR_COUNT_KEY,
    IBKR_LAST_ERROR_KEY,
    IBKR_THRESHOLD_STATE_KEY,
)
CIRCUIT_BREAKER_AUTO_RECOVERY_KEY = "circuit_breaker_last_auto_recovery"
RECOVERABLE_IBKR_SOURCES = ("ibkr", "tws_mirror", "account_sync", "execution_sync")
DEFAULT_MAX_IBKR_ERRORS = 3
DEFAULT_MAX_DRAWDOWN_PERCENT = 20.0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_payload(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


async def trip_circuit_breaker(reason: str, *, source: str = "system", details: dict | None = None) -> dict:
    """Disable auto trading and persist a user-visible circuit-breaker reason."""
    state = {
        "tripped": True,
        "reason": str(reason or "Circuit breaker tripped"),
        "source": source,
        "details": details or {},
        "tripped_at": now_iso(),
    }
    await database.set_app_state(CIRCUIT_BREAKER_STATE_KEY, _json_payload(state))
    await database.set_app_state("auto_trading_enabled", "false")
    await database.safe_record_trade_journal_event({
        "event_type": "CIRCUIT_BREAKER_TRIPPED",
        "decision": "AUTO_TRADING_DISABLED",
        "reason": state["reason"],
        "source_module": source,
        "raw_payload": state,
    })
    return state


async def get_circuit_breaker_state() -> dict:
    raw = await database.get_app_state(CIRCUIT_BREAKER_STATE_KEY)
    if raw:
        try:
            state = json.loads(raw)
            state.setdefault("tripped", False)
            return state
        except Exception:
            return {
                "tripped": True,
                "reason": "Circuit breaker state is unreadable",
                "source": "circuit_breaker.get_circuit_breaker_state",
                "details": {"raw": raw},
                "tripped_at": now_iso(),
            }
    return {
        "tripped": False,
        "reason": None,
        "source": None,
        "details": {},
        "tripped_at": None,
    }


async def reset_circuit_breaker(reason: str = "Manual circuit breaker reset") -> dict:
    await database.delete_app_states([CIRCUIT_BREAKER_STATE_KEY, *IBKR_ERROR_STATE_KEYS])
    await database.safe_record_trade_journal_event({
        "event_type": "CIRCUIT_BREAKER_RESET",
        "decision": "RESET",
        "reason": reason,
        "source_module": "circuit_breaker.reset_circuit_breaker",
    })
    return await get_circuit_breaker_state()


def _recoverable_source(source: str | None) -> bool:
    value = str(source or "").lower()
    return value == "watchdog" or any(value.startswith(prefix) for prefix in RECOVERABLE_IBKR_SOURCES)


def _recoverable_reason(reason: str | None) -> bool:
    value = str(reason or "").lower()
    return "repeated ibkr error" in value or "prolonged tws/api disconnect" in value


def is_auto_recoverable_trip(state: dict) -> bool:
    return bool(state.get("tripped")) and (_recoverable_source(state.get("source")) or _recoverable_reason(state.get("reason")))


async def get_ibkr_error_count() -> int:
    raw = await database.get_app_state(IBKR_ERROR_COUNT_KEY, "0")
    try:
        return int(raw or 0)
    except Exception:
        return 0


async def get_last_auto_recovery() -> dict | None:
    raw = await database.get_app_state(CIRCUIT_BREAKER_AUTO_RECOVERY_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def auto_clear_recoverable_circuit_breaker(
    reason: str,
    *,
    source: str = "circuit_breaker.auto_clear_recoverable_circuit_breaker",
    force: bool = False,
) -> dict:
    state = await get_circuit_breaker_state()
    previous_error_count = await get_ibkr_error_count()
    if not state.get("tripped") and previous_error_count <= 0:
        return {"cleared": False, "state": state, "previous_error_count": previous_error_count}
    if state.get("tripped") and not force and not is_auto_recoverable_trip(state):
        return {"cleared": False, "state": state, "previous_error_count": previous_error_count}

    payload = {
        "cleared": True,
        "reason": reason,
        "source": source,
        "previous_error_count": previous_error_count,
        "previous_circuit_breaker": state,
        "cleared_at": now_iso(),
    }
    await database.delete_app_states([CIRCUIT_BREAKER_STATE_KEY, *IBKR_ERROR_STATE_KEYS])
    await database.set_app_state(CIRCUIT_BREAKER_AUTO_RECOVERY_KEY, _json_payload(payload))
    await database.safe_record_trade_journal_event({
        "event_type": "CIRCUIT_BREAKER_AUTO_RECOVERED",
        "decision": "AUTO_RECOVERED",
        "reason": reason,
        "source_module": source,
        "raw_payload": payload,
    })
    log.warning(
        "Circuit breaker auto-cleared | reason=%s previous_error_count=%s previous_source=%s",
        reason,
        previous_error_count,
        state.get("source"),
    )
    payload["state"] = await get_circuit_breaker_state()
    return payload


async def record_ibkr_error(error: str, *, source: str = "ibkr") -> dict:
    max_errors = int(getattr(config, "CIRCUIT_BREAKER_MAX_IBKR_ERRORS", DEFAULT_MAX_IBKR_ERRORS))
    raw = await database.get_app_state(IBKR_ERROR_COUNT_KEY, "0")
    try:
        count = int(raw or 0) + 1
    except Exception:
        count = 1
    threshold_state = {"error_count": count, "max_errors": max_errors, "threshold_reached": count >= max_errors}
    await database.set_app_state(IBKR_ERROR_COUNT_KEY, str(count))
    await database.set_app_state(IBKR_LAST_ERROR_KEY, str(error))
    await database.set_app_state(IBKR_THRESHOLD_STATE_KEY, _json_payload(threshold_state))
    if count >= max_errors:
        return await trip_circuit_breaker(
            f"Repeated IBKR errors ({count}/{max_errors}): {error}",
            source=source,
            details={**threshold_state, "last_error": str(error)},
        )
    return await get_circuit_breaker_state()


async def reset_ibkr_error_count() -> None:
    await database.delete_app_states(IBKR_ERROR_STATE_KEYS)


async def record_order_reject(reason: str, *, source: str = "order_lifecycle", details: dict | None = None) -> dict:
    return await trip_circuit_breaker(
        f"Order rejected: {reason}",
        source=source,
        details=details,
    )


async def validate_buying_power(value: float | None, *, source: str = "startup_recovery") -> dict:
    try:
        buying_power = float(value)
    except Exception:
        buying_power = -1.0
    if buying_power <= 0:
        return await trip_circuit_breaker(
            f"Invalid buying power: {value}",
            source=source,
            details={"buying_power": value},
        )
    return await get_circuit_breaker_state()


async def validate_equity(value: float | None, *, source: str = "startup_recovery") -> dict:
    try:
        equity = float(value)
    except Exception:
        equity = -1.0
    if equity <= 0:
        return await trip_circuit_breaker(
            f"Invalid account equity: {value}",
            source=source,
            details={"equity": value},
        )
    return await get_circuit_breaker_state()


async def validate_drawdown(*, source: str = "circuit_breaker.validate_drawdown") -> dict:
    max_drawdown = float(getattr(config, "CIRCUIT_BREAKER_MAX_DRAWDOWN_PERCENT", DEFAULT_MAX_DRAWDOWN_PERCENT))
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT net_liquidation
            FROM equity_curve
            WHERE net_liquidation IS NOT NULL AND net_liquidation > 0
            ORDER BY timestamp ASC, id ASC
            """
        ) as cursor:
            rows = await cursor.fetchall()
    values = [float(row["net_liquidation"]) for row in rows]
    if len(values) < 2:
        return await get_circuit_breaker_state()
    peak = max(values)
    current = values[-1]
    drawdown = ((peak - current) / peak) * 100 if peak > 0 else 0.0
    if drawdown >= max_drawdown:
        return await trip_circuit_breaker(
            f"Excessive drawdown: {drawdown:.2f}% >= {max_drawdown:.2f}%",
            source=source,
            details={"peak_equity": peak, "current_equity": current, "drawdown_percent": drawdown},
        )
    return await get_circuit_breaker_state()

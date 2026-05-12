from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Awaitable, Callable

import aiosqlite

import config
import database

log = logging.getLogger(__name__)

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
RECOVERY = "RECOVERY"
BLOCK_BUY = "BLOCK_BUY"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"

RECOVERY_STATUS_KEY = "recovery_manager_status"
ACCOUNT_SYNC_SUCCESS_KEY = "account_sync_last_success_at"
ACCOUNT_SYNC_ERROR_KEY = "account_sync_last_error"
EXECUTION_SYNC_SUCCESS_KEY = "execution_sync_last_success_at"
EXECUTION_SYNC_ERROR_KEY = "execution_sync_last_error"
RECONCILIATION_SUCCESS_KEY = "reconciliation_last_success_at"
RECONCILIATION_ERROR_KEY = "reconciliation_last_error"

DEFAULT_HEARTBEAT_STALE_SECONDS = 45
DEFAULT_SYNC_STALE_SECONDS = 120

ResyncRunner = Callable[[], Awaitable[dict]]

CREATE_RECOVERY_STATE = """
CREATE TABLE IF NOT EXISTS recovery_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    connection_status TEXT,
    last_heartbeat_at TEXT,
    last_successful_sync_at TEXT,
    recovery_mode TEXT,
    recovery_reason TEXT,
    trading_allowed_after_recovery INTEGER DEFAULT 1,
    updated_at TEXT
)
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def seconds_since(value: str | None) -> float | None:
    dt = parse_dt(value)
    if not dt:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds()


def _json_payload(data) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"repr": repr(data)}, ensure_ascii=False)


async def init_recovery_db() -> None:
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(CREATE_RECOVERY_STATE)
        await db.execute(
            """
            INSERT OR IGNORE INTO recovery_state (
                id, connection_status, last_heartbeat_at, last_successful_sync_at,
                recovery_mode, recovery_reason, trading_allowed_after_recovery, updated_at
            )
            VALUES (1, 'UNKNOWN', NULL, NULL, 'HEALTHY', 'Initial state', 1, ?)
            """,
            (now_iso(),),
        )
        await db.commit()


async def get_recovery_state() -> dict:
    await init_recovery_db()

    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM recovery_state WHERE id = 1") as cursor:
            row = await cursor.fetchone()

    state = dict(row) if row else {}
    state["trading_allowed_after_recovery"] = bool(state.get("trading_allowed_after_recovery"))
    return state


async def set_recovery_state(
    *,
    connection_status: str | None = None,
    last_heartbeat_at: str | None = None,
    last_successful_sync_at: str | None = None,
    recovery_mode: str | None = None,
    recovery_reason: str | None = None,
    trading_allowed_after_recovery: bool | None = None,
) -> dict:
    await init_recovery_db()
    current = await get_recovery_state()

    next_state = {
        "connection_status": connection_status if connection_status is not None else current.get("connection_status"),
        "last_heartbeat_at": last_heartbeat_at if last_heartbeat_at is not None else current.get("last_heartbeat_at"),
        "last_successful_sync_at": (
            last_successful_sync_at
            if last_successful_sync_at is not None
            else current.get("last_successful_sync_at")
        ),
        "recovery_mode": recovery_mode if recovery_mode is not None else current.get("recovery_mode"),
        "recovery_reason": recovery_reason if recovery_reason is not None else current.get("recovery_reason"),
        "trading_allowed_after_recovery": (
            int(bool(trading_allowed_after_recovery))
            if trading_allowed_after_recovery is not None
            else int(bool(current.get("trading_allowed_after_recovery")))
        ),
        "updated_at": now_iso(),
    }

    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO recovery_state (
                id, connection_status, last_heartbeat_at, last_successful_sync_at,
                recovery_mode, recovery_reason, trading_allowed_after_recovery, updated_at
            )
            VALUES (1, :connection_status, :last_heartbeat_at, :last_successful_sync_at,
                    :recovery_mode, :recovery_reason, :trading_allowed_after_recovery, :updated_at)
            ON CONFLICT(id) DO UPDATE SET
                connection_status = excluded.connection_status,
                last_heartbeat_at = excluded.last_heartbeat_at,
                last_successful_sync_at = excluded.last_successful_sync_at,
                recovery_mode = excluded.recovery_mode,
                recovery_reason = excluded.recovery_reason,
                trading_allowed_after_recovery = excluded.trading_allowed_after_recovery,
                updated_at = excluded.updated_at
            """,
            next_state,
        )
        await db.commit()

    next_state["trading_allowed_after_recovery"] = bool(next_state["trading_allowed_after_recovery"])
    await database.set_app_state(RECOVERY_STATUS_KEY, _json_payload(next_state))
    return next_state


async def record_recovery_event(event_type: str, reason: str, payload: dict | None = None) -> None:
    await database.safe_record_trade_journal_event({
        "event_type": event_type,
        "decision": event_type,
        "reason": reason,
        "source_module": "recovery_manager",
        "raw_payload": payload or {},
    })


def record_buy_blocked_sync(symbol: str | None, reason: str, payload: dict | None = None) -> None:
    database.safe_record_trade_journal_event_sync({
        "symbol": symbol,
        "event_type": "BUY_BLOCKED_RECOVERY",
        "decision": "BLOCKED",
        "reason": reason,
        "source_module": "recovery_manager",
        "raw_payload": payload or {},
    })


async def record_buy_blocked(symbol: str | None, reason: str, payload: dict | None = None) -> None:
    await database.safe_record_trade_journal_event({
        "symbol": symbol,
        "event_type": "BUY_BLOCKED_RECOVERY",
        "decision": "BLOCKED",
        "reason": reason,
        "source_module": "recovery_manager",
        "raw_payload": payload or {},
    })


async def _load_tws_heartbeat() -> dict:
    try:
        async with aiosqlite.connect(config.DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT connected, account, last_sync_at, error
                FROM tws_heartbeat
                WHERE id = 1
                """
            ) as cursor:
                row = await cursor.fetchone()
    except sqlite3.OperationalError:
        return {
            "connected": False,
            "account": None,
            "last_sync_at": None,
            "error": "tws_heartbeat table missing",
        }

    if not row:
        return {
            "connected": False,
            "account": None,
            "last_sync_at": None,
            "error": "No TWS heartbeat has been recorded",
        }

    return {
        "connected": bool(row["connected"]),
        "account": row["account"],
        "last_sync_at": row["last_sync_at"],
        "error": row["error"],
    }


async def mark_account_sync_result(snapshot: dict) -> None:
    synced_at = snapshot.get("synced_at") or now_iso()
    error = snapshot.get("error")

    if error or not snapshot.get("connected"):
        await database.set_app_state(ACCOUNT_SYNC_ERROR_KEY, str(error or "Account sync disconnected"))
    else:
        await database.set_app_state(ACCOUNT_SYNC_ERROR_KEY, "")
        await database.set_app_state(ACCOUNT_SYNC_SUCCESS_KEY, synced_at)


async def mark_execution_sync_result(success: bool, synced_at: str | None = None, error: str | None = None) -> None:
    if success:
        await database.set_app_state(EXECUTION_SYNC_ERROR_KEY, "")
        await database.set_app_state(EXECUTION_SYNC_SUCCESS_KEY, synced_at or now_iso())
    else:
        await database.set_app_state(EXECUTION_SYNC_ERROR_KEY, str(error or "Execution sync failed"))


async def mark_reconciliation_result(result: dict) -> None:
    checked_at = result.get("checked_at") or now_iso()
    if result.get("error"):
        await database.set_app_state(RECONCILIATION_ERROR_KEY, str(result.get("error")))
        return

    await database.set_app_state(RECONCILIATION_ERROR_KEY, "")
    await database.set_app_state(RECONCILIATION_SUCCESS_KEY, checked_at)


async def _sync_failure_reasons() -> list[str]:
    stale_seconds = int(getattr(config, "RECOVERY_SYNC_STALE_SECONDS", DEFAULT_SYNC_STALE_SECONDS))
    checks = [
        ("account sync", ACCOUNT_SYNC_SUCCESS_KEY, ACCOUNT_SYNC_ERROR_KEY),
        ("execution sync", EXECUTION_SYNC_SUCCESS_KEY, EXECUTION_SYNC_ERROR_KEY),
        ("reconciliation", RECONCILIATION_SUCCESS_KEY, RECONCILIATION_ERROR_KEY),
    ]

    reasons = []

    for label, success_key, error_key in checks:
        error = await database.get_app_state(error_key, "")
        last_success = await database.get_app_state(success_key)
        age = seconds_since(last_success)

        if error:
            reasons.append(f"{label} failed: {error}")
        elif last_success and age is not None and age > stale_seconds:
            reasons.append(f"{label} stale for {round(age)} seconds")

    return reasons


async def run_full_resync() -> dict:
    from account_sync import run_account_sync_once, run_reconciliation_status_check
    from execution_sync import sync_executions
    from reconciliation import run_reconciliation_once
    from tws_mirror import run_tws_mirror_once

    results = {
        "tws_mirror": await run_tws_mirror_once(),
        "account_sync": await run_account_sync_once(),
        "execution_sync": await sync_executions(),
        "reconciliation": await run_reconciliation_once(),
        "reconciliation_status": await run_reconciliation_status_check(),
    }

    await mark_reconciliation_result(results["reconciliation"])
    return results


class RecoveryManager:
    def __init__(self, resync_runner: ResyncRunner | None = None):
        self.resync_runner = resync_runner or run_full_resync

    async def check_once(self) -> dict:
        await init_recovery_db()
        current = await get_recovery_state()
        heartbeat = await _load_tws_heartbeat()
        stale_seconds = int(getattr(config, "RECOVERY_HEARTBEAT_STALE_SECONDS", DEFAULT_HEARTBEAT_STALE_SECONDS))
        heartbeat_age = seconds_since(heartbeat.get("last_sync_at"))
        heartbeat_stale = heartbeat_age is None or heartbeat_age > stale_seconds
        was_recovery = current.get("recovery_mode") in {RECOVERY, BLOCK_BUY}
        was_healthy = current.get("recovery_mode") == HEALTHY

        if (not heartbeat.get("connected")) or heartbeat_stale:
            reason = heartbeat.get("error") or "TWS heartbeat stale or disconnected"
            if heartbeat_age is not None:
                reason = f"{reason}; heartbeat age={round(heartbeat_age)}s"

            state = await set_recovery_state(
                connection_status="DISCONNECTED" if not heartbeat.get("connected") else "STALE",
                last_heartbeat_at=heartbeat.get("last_sync_at"),
                recovery_mode=RECOVERY,
                recovery_reason=reason,
                trading_allowed_after_recovery=False,
            )

            if was_healthy:
                await record_recovery_event("CONNECTION_LOST", reason, {"heartbeat": heartbeat})
                await record_recovery_event("RECOVERY_STARTED", reason, {"heartbeat": heartbeat})

            return {"state": state, "heartbeat": heartbeat, "resync": None}

        sync_reasons = await _sync_failure_reasons()

        if sync_reasons and not was_recovery:
            reason = "; ".join(sync_reasons)
            state = await set_recovery_state(
                connection_status="DEGRADED",
                last_heartbeat_at=heartbeat.get("last_sync_at"),
                recovery_mode=DEGRADED,
                recovery_reason=reason,
                trading_allowed_after_recovery=False,
            )
            return {"state": state, "heartbeat": heartbeat, "resync": None}

        if current.get("recovery_mode") in {HEALTHY, DEGRADED} and not sync_reasons:
            state = await set_recovery_state(
                connection_status="CONNECTED",
                last_heartbeat_at=heartbeat.get("last_sync_at"),
                recovery_mode=HEALTHY,
                recovery_reason="Heartbeat and sync checks healthy",
                trading_allowed_after_recovery=True,
            )
            return {"state": state, "heartbeat": heartbeat, "resync": None}

        await record_recovery_event(
            "CONNECTION_RESTORED",
            "TWS heartbeat restored; starting broker state resync",
            {"heartbeat": heartbeat},
        )

        resync_result = await self.resync_runner()
        reconciliation_status = resync_result.get("reconciliation_status") or resync_result.get("reconciliation") or {}
        mismatches = reconciliation_status.get("mismatches") or reconciliation_status.get("issues") or []
        component_errors = []

        tws_result = resync_result.get("tws_mirror") or {}
        if tws_result and (tws_result.get("error") or not tws_result.get("connected", True)):
            component_errors.append(f"TWS mirror failed: {tws_result.get('error') or 'disconnected'}")

        account_result = resync_result.get("account_sync") or {}
        if account_result and (account_result.get("error") or not account_result.get("connected", True)):
            component_errors.append(f"account sync failed: {account_result.get('error') or 'disconnected'}")

        execution_result = resync_result.get("execution_sync") or {}
        if execution_result and not execution_result.get("ok", True):
            component_errors.append(f"execution sync failed: {execution_result.get('error') or 'unknown error'}")

        reconciliation_result = resync_result.get("reconciliation") or {}
        if reconciliation_result and reconciliation_result.get("error"):
            component_errors.append(f"reconciliation failed: {reconciliation_result.get('error')}")

        ok = bool(reconciliation_status.get("ok")) and not mismatches and not component_errors

        if ok:
            sync_time = now_iso()
            state = await set_recovery_state(
                connection_status="CONNECTED",
                last_heartbeat_at=heartbeat.get("last_sync_at"),
                last_successful_sync_at=sync_time,
                recovery_mode=HEALTHY,
                recovery_reason="Full broker state resync completed successfully",
                trading_allowed_after_recovery=True,
            )
            await record_recovery_event("RECOVERY_COMPLETED", state["recovery_reason"], resync_result)
            return {"state": state, "heartbeat": heartbeat, "resync": resync_result}

        if component_errors:
            reason = "; ".join(component_errors)
            state = await set_recovery_state(
                connection_status="CONNECTED",
                last_heartbeat_at=heartbeat.get("last_sync_at"),
                recovery_mode=RECOVERY,
                recovery_reason=reason,
                trading_allowed_after_recovery=False,
            )
            return {"state": state, "heartbeat": heartbeat, "resync": resync_result}

        reason = "Reconciliation mismatches remain after recovery resync"
        state = await set_recovery_state(
            connection_status="CONNECTED",
            last_heartbeat_at=heartbeat.get("last_sync_at"),
            recovery_mode=MANUAL_REVIEW_REQUIRED,
            recovery_reason=reason,
            trading_allowed_after_recovery=False,
        )
        await record_recovery_event("MANUAL_REVIEW_REQUIRED", reason, resync_result)
        return {"state": state, "heartbeat": heartbeat, "resync": resync_result}


async def run_recovery_check_once() -> dict:
    return await RecoveryManager().check_once()


async def get_recovery_status() -> dict:
    state = await get_recovery_state()
    return {
        "state": state,
        "healthy": state.get("recovery_mode") == HEALTHY,
        "buy_orders_allowed": state.get("recovery_mode") == HEALTHY and bool(state.get("trading_allowed_after_recovery")),
    }


async def require_recovery_healthy_for_buy(symbol: str | None = None, payload: dict | None = None) -> None:
    status = await get_recovery_status()
    if status["buy_orders_allowed"]:
        return

    state = status["state"]
    if state.get("recovery_mode") == DEGRADED:
        state = await set_recovery_state(
            recovery_mode=BLOCK_BUY,
            recovery_reason=state.get("recovery_reason") or "BUY orders blocked while recovery state is degraded",
            trading_allowed_after_recovery=False,
        )

    reason = f"BUY blocked while recovery state is {state.get('recovery_mode')}: {state.get('recovery_reason')}"
    await record_buy_blocked(symbol, reason, payload or state)
    raise RuntimeError(reason)


def require_recovery_healthy_for_buy_sync(symbol: str | None = None, payload: dict | None = None) -> None:
    with sqlite3.connect(config.DB_PATH) as db:
        db.execute(CREATE_RECOVERY_STATE)
        db.execute(
            """
            INSERT OR IGNORE INTO recovery_state (
                id, connection_status, last_heartbeat_at, last_successful_sync_at,
                recovery_mode, recovery_reason, trading_allowed_after_recovery, updated_at
            )
            VALUES (1, 'UNKNOWN', NULL, NULL, 'HEALTHY', 'Initial state', 1, ?)
            """,
            (now_iso(),),
        )
        db.commit()
        row = db.execute(
            """
            SELECT recovery_mode, recovery_reason, trading_allowed_after_recovery
            FROM recovery_state
            WHERE id = 1
            """
        ).fetchone()

    if not row:
        return

    mode, reason_text, allowed = row
    if mode == HEALTHY and bool(allowed):
        return

    if mode == DEGRADED:
        mode = BLOCK_BUY
        with sqlite3.connect(config.DB_PATH) as db:
            db.execute(
                """
                UPDATE recovery_state
                SET recovery_mode = ?,
                    recovery_reason = COALESCE(recovery_reason, ?),
                    trading_allowed_after_recovery = 0,
                    updated_at = ?
                WHERE id = 1
                """,
                (BLOCK_BUY, "BUY orders blocked while recovery state is degraded", now_iso()),
            )
            db.commit()

    reason = f"BUY blocked while recovery state is {mode}: {reason_text}"
    record_buy_blocked_sync(symbol, reason, payload or {"recovery_mode": mode, "recovery_reason": reason_text})
    raise RuntimeError(reason)

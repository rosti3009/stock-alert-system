from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import aiosqlite

import config
import database
from broker_freshness import evaluate_broker_freshness

log = logging.getLogger(__name__)

RECOVERY_STATUS_KEY = "recovery_status"
RECOVERY_STATE_KEY = "recovery_state"


class RecoveryState(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    RECOVERY = "RECOVERY"
    BLOCK_BUY = "BLOCK_BUY"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"


BUY_BLOCKING_STATES = {
    RecoveryState.RECOVERY.value,
    RecoveryState.BLOCK_BUY.value,
    RecoveryState.MANUAL_REVIEW_REQUIRED.value,
}


@dataclass
class RecoveryIssue:
    issue_type: str
    severity: str
    message: str
    source: str
    symbol: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_type": self.issue_type,
            "severity": self.severity,
            "message": self.message,
            "source": self.source,
            "symbol": self.symbol,
            "details": self.details,
        }


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def heartbeat_degraded_seconds() -> int:
    return int(getattr(config, "RECOVERY_HEARTBEAT_DEGRADED_SECONDS", 60))


def heartbeat_block_buy_seconds() -> int:
    return int(getattr(config, "RECOVERY_HEARTBEAT_BLOCK_BUY_SECONDS", 120))


def position_stale_seconds() -> int:
    return int(getattr(config, "RECOVERY_POSITION_STALE_SECONDS", 180))


def _json_payload(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"repr": repr(data)}, ensure_ascii=False)


def _state_from_issues(issues: list[RecoveryIssue]) -> RecoveryState:
    issue_types = {issue.issue_type for issue in issues}
    severities = {issue.severity.upper() for issue in issues}

    if "RECONCILIATION_HIGH" in issue_types:
        return RecoveryState.MANUAL_REVIEW_REQUIRED

    if "NO_TWS_HEARTBEAT" in issue_types or "TWS_DISCONNECTED" in issue_types:
        return RecoveryState.BLOCK_BUY

    if "TWS_HEARTBEAT_BLOCK_STALE" in issue_types:
        return RecoveryState.BLOCK_BUY

    if "TWS_HEARTBEAT_RECOVERY_STALE" in issue_types:
        return RecoveryState.RECOVERY

    if "HIGH" in severities or "CRITICAL" in severities:
        return RecoveryState.BLOCK_BUY

    if issues:
        return RecoveryState.DEGRADED

    return RecoveryState.HEALTHY


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


async def _collect_heartbeat_issues(db: aiosqlite.Connection) -> tuple[dict[str, Any], list[RecoveryIssue]]:
    issues: list[RecoveryIssue] = []
    heartbeat = {
        "found": False,
        "connected": False,
        "account": None,
        "last_sync_at": None,
        "age_seconds": None,
        "error": None,
    }

    if not await _table_exists(db, "tws_heartbeat"):
        issues.append(RecoveryIssue(
            issue_type="NO_TWS_HEARTBEAT",
            severity="CRITICAL",
            message="No TWS heartbeat table found",
            source="tws_heartbeat",
        ))
        return heartbeat, issues

    async with db.execute(
        """
        SELECT connected, account, last_sync_at, error
        FROM tws_heartbeat
        WHERE id = 1
        """
    ) as cursor:
        row = await cursor.fetchone()

    if not row:
        issues.append(RecoveryIssue(
            issue_type="NO_TWS_HEARTBEAT",
            severity="CRITICAL",
            message="No TWS heartbeat found",
            source="tws_heartbeat",
        ))
        return heartbeat, issues

    connected = bool(row[0])
    last_sync_at = row[2]
    error = row[3]
    last_sync_dt = parse_dt(last_sync_at)
    age_seconds = None

    if last_sync_dt:
        age_seconds = int((now_utc() - last_sync_dt).total_seconds())

    heartbeat.update({
        "found": True,
        "connected": connected,
        "account": row[1],
        "last_sync_at": last_sync_at,
        "age_seconds": age_seconds,
        "error": error,
    })
    watchdog_status_raw = await database.get_app_state("watchdog_status")
    watchdog_status = json.loads(watchdog_status_raw) if watchdog_status_raw else {}
    broker_snapshot = await database.get_latest_broker_sync_snapshot() or {}
    freshness = evaluate_broker_freshness(watchdog_status, broker_snapshot)
    fallback_active = bool(freshness.get("broker_sync_connected") and freshness.get("broker_sync_fresh"))

    if not connected and not fallback_active:
        issues.append(RecoveryIssue(
            issue_type="TWS_DISCONNECTED",
            severity="CRITICAL",
            message=f"TWS heartbeat is disconnected: {error or 'unknown error'}",
            source="tws_heartbeat",
            details={"error": error},
        ))

    if last_sync_dt is None and not fallback_active:
        issues.append(RecoveryIssue(
            issue_type="TWS_HEARTBEAT_MISSING_TIMESTAMP",
            severity="HIGH",
            message="TWS heartbeat is missing last_sync_at",
            source="tws_heartbeat",
        ))
    elif age_seconds is not None and age_seconds > heartbeat_block_buy_seconds() and not fallback_active:
        issues.append(RecoveryIssue(
            issue_type="TWS_HEARTBEAT_BLOCK_STALE",
            severity="CRITICAL",
            message=f"TWS heartbeat stale for {age_seconds} seconds",
            source="tws_heartbeat",
            details={"age_seconds": age_seconds},
        ))
    elif age_seconds is not None and age_seconds > heartbeat_degraded_seconds() and not fallback_active:
        issues.append(RecoveryIssue(
            issue_type="TWS_HEARTBEAT_RECOVERY_STALE",
            severity="HIGH",
            message=f"TWS heartbeat stale for {age_seconds} seconds",
            source="tws_heartbeat",
            details={"age_seconds": age_seconds},
        ))

    heartbeat["freshness"] = freshness
    heartbeat["fallback_active"] = fallback_active
    return heartbeat, issues


async def _collect_reconciliation_issues(db: aiosqlite.Connection) -> list[RecoveryIssue]:
    if not await _table_exists(db, "reconciliation_issues"):
        return []

    async with db.execute(
        """
        SELECT symbol, issue_type, severity, details, created_at
        FROM reconciliation_issues
        WHERE status = 'OPEN'
        ORDER BY id DESC
        LIMIT 50
        """
    ) as cursor:
        rows = await cursor.fetchall()

    issues: list[RecoveryIssue] = []

    for row in rows:
        severity = str(row[2] or "MEDIUM").upper()
        issue_type = "RECONCILIATION_HIGH" if severity == "HIGH" else "RECONCILIATION_ISSUE"
        issues.append(RecoveryIssue(
            issue_type=issue_type,
            severity=severity,
            message=f"Open reconciliation issue: {row[1]}",
            source="reconciliation_issues",
            symbol=str(row[0] or "").upper() or None,
            details={
                "reconciliation_issue_type": row[1],
                "details": row[3],
                "created_at": row[4],
            },
        ))

    return issues


async def _collect_position_staleness_issues(db: aiosqlite.Connection) -> list[RecoveryIssue]:
    if not await _table_exists(db, "tws_positions"):
        return []

    async with db.execute(
        "SELECT symbol, updated_at FROM tws_positions"
    ) as cursor:
        rows = await cursor.fetchall()

    issues: list[RecoveryIssue] = []

    for row in rows:
        updated_at = parse_dt(row[1])
        if not updated_at:
            continue

        age_seconds = int((now_utc() - updated_at).total_seconds())
        if age_seconds > position_stale_seconds():
            symbol = str(row[0] or "").upper()
            issues.append(RecoveryIssue(
                issue_type="TWS_POSITION_STALE",
                severity="MEDIUM",
                message=f"TWS position stale: {symbol} ({age_seconds} seconds)",
                source="tws_positions",
                symbol=symbol or None,
                details={"age_seconds": age_seconds, "updated_at": row[1]},
            ))

    return issues


async def evaluate_recovery_status() -> dict[str, Any]:
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        heartbeat, heartbeat_issues = await _collect_heartbeat_issues(db)
        reconciliation_issues = await _collect_reconciliation_issues(db)
        position_issues = await _collect_position_staleness_issues(db)

    issues = heartbeat_issues + reconciliation_issues + position_issues
    state = _state_from_issues(issues)
    buy_blocked = state.value in BUY_BLOCKING_STATES

    return {
        "state": state.value,
        "healthy": state == RecoveryState.HEALTHY,
        "buy_blocked": buy_blocked,
        "buy_block_reason": _buy_block_reason(state, issues) if buy_blocked else None,
        "checked_at": now_iso(),
        "heartbeat": heartbeat,
        "thresholds": {
            "heartbeat_degraded_seconds": heartbeat_degraded_seconds(),
            "heartbeat_block_buy_seconds": heartbeat_block_buy_seconds(),
            "position_stale_seconds": position_stale_seconds(),
        },
        "issues_count": len(issues),
        "issues": [issue.to_dict() for issue in issues],
    }


def _buy_block_reason(state: RecoveryState, issues: list[RecoveryIssue]) -> str:
    if state == RecoveryState.MANUAL_REVIEW_REQUIRED:
        return "Manual review required before opening new BUY orders"

    if issues:
        return issues[0].message

    return f"Recovery state blocks BUY orders: {state.value}"


async def run_recovery_check() -> dict[str, Any]:
    status = await evaluate_recovery_status()
    previous_state = await database.get_app_state(RECOVERY_STATE_KEY)

    await database.set_app_state(RECOVERY_STATUS_KEY, _json_payload(status))
    await database.set_app_state(RECOVERY_STATE_KEY, status["state"])

    if previous_state != status["state"]:
        await database.safe_record_trade_journal_event({
            "event_type": "RECOVERY_STATE_CHANGED",
            "decision": status["state"],
            "reason": status.get("buy_block_reason") or f"Recovery state is {status['state']}",
            "source_module": "recovery_manager",
            "raw_payload": {
                "previous_state": previous_state,
                "status": status,
            },
        })

    log.info(
        "Recovery check complete | state=%s | buy_blocked=%s | issues=%s",
        status["state"],
        status["buy_blocked"],
        status["issues_count"],
    )

    return status


async def get_recovery_status() -> dict[str, Any]:
    saved = await database.get_app_state(RECOVERY_STATUS_KEY)
    if saved:
        try:
            return json.loads(saved)
        except Exception:
            pass

    return await run_recovery_check()


def _sync_table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def evaluate_recovery_status_sync() -> dict[str, Any]:
    issues: list[RecoveryIssue] = []
    heartbeat = {
        "found": False,
        "connected": False,
        "account": None,
        "last_sync_at": None,
        "age_seconds": None,
        "error": None,
    }

    with closing(sqlite3.connect(config.DB_PATH)) as db:
        watchdog_row = db.execute("SELECT value FROM app_state WHERE key='watchdog_status'").fetchone()
        watchdog_status = json.loads(watchdog_row[0]) if watchdog_row and watchdog_row[0] else {}
        broker_row = db.execute("SELECT connected, synced_at FROM broker_sync_snapshots ORDER BY id DESC LIMIT 1").fetchone()
        broker_snapshot = {"connected": bool(broker_row[0]), "synced_at": broker_row[1]} if broker_row else {}
        freshness = evaluate_broker_freshness(watchdog_status, broker_snapshot)
        fallback_active = bool(freshness.get("broker_sync_connected") and freshness.get("broker_sync_fresh"))

        if _sync_table_exists(db, "tws_heartbeat"):
            row = db.execute(
                "SELECT connected, account, last_sync_at, error FROM tws_heartbeat WHERE id = 1"
            ).fetchone()
        else:
            row = None

        if not row:
            issues.append(RecoveryIssue(
                issue_type="NO_TWS_HEARTBEAT",
                severity="CRITICAL",
                message="No TWS heartbeat found",
                source="tws_heartbeat",
            ))
        else:
            connected = bool(row[0])
            last_sync_dt = parse_dt(row[2])
            age_seconds = int((now_utc() - last_sync_dt).total_seconds()) if last_sync_dt else None
            heartbeat.update({
                "found": True,
                "connected": connected,
                "account": row[1],
                "last_sync_at": row[2],
                "age_seconds": age_seconds,
                "error": row[3],
            })

            if not connected and not fallback_active:
                issues.append(RecoveryIssue(
                    issue_type="TWS_DISCONNECTED",
                    severity="CRITICAL",
                    message=f"TWS heartbeat is disconnected: {row[3] or 'unknown error'}",
                    source="tws_heartbeat",
                    details={"error": row[3]},
                ))

            if last_sync_dt is None and not fallback_active:
                issues.append(RecoveryIssue(
                    issue_type="TWS_HEARTBEAT_MISSING_TIMESTAMP",
                    severity="HIGH",
                    message="TWS heartbeat is missing last_sync_at",
                    source="tws_heartbeat",
                ))
            elif age_seconds is not None and age_seconds > heartbeat_block_buy_seconds() and not fallback_active:
                issues.append(RecoveryIssue(
                    issue_type="TWS_HEARTBEAT_BLOCK_STALE",
                    severity="CRITICAL",
                    message=f"TWS heartbeat stale for {age_seconds} seconds",
                    source="tws_heartbeat",
                    details={"age_seconds": age_seconds},
                ))
            elif age_seconds is not None and age_seconds > heartbeat_degraded_seconds() and not fallback_active:
                issues.append(RecoveryIssue(
                    issue_type="TWS_HEARTBEAT_RECOVERY_STALE",
                    severity="HIGH",
                    message=f"TWS heartbeat stale for {age_seconds} seconds",
                    source="tws_heartbeat",
                    details={"age_seconds": age_seconds},
                ))

        if _sync_table_exists(db, "reconciliation_issues"):
            rows = db.execute(
                """
                SELECT symbol, issue_type, severity, details, created_at
                FROM reconciliation_issues
                WHERE status = 'OPEN'
                ORDER BY id DESC
                LIMIT 50
                """
            ).fetchall()

            for rec in rows:
                severity = str(rec[2] or "MEDIUM").upper()
                issue_type = "RECONCILIATION_HIGH" if severity == "HIGH" else "RECONCILIATION_ISSUE"
                issues.append(RecoveryIssue(
                    issue_type=issue_type,
                    severity=severity,
                    message=f"Open reconciliation issue: {rec[1]}",
                    source="reconciliation_issues",
                    symbol=str(rec[0] or "").upper() or None,
                    details={
                        "reconciliation_issue_type": rec[1],
                        "details": rec[3],
                        "created_at": rec[4],
                    },
                ))

    state = _state_from_issues(issues)
    buy_blocked = state.value in BUY_BLOCKING_STATES

    heartbeat["freshness"] = freshness
    heartbeat["fallback_active"] = fallback_active
    return {
        "state": state.value,
        "healthy": state == RecoveryState.HEALTHY,
        "buy_blocked": buy_blocked,
        "buy_block_reason": _buy_block_reason(state, issues) if buy_blocked else None,
        "checked_at": now_iso(),
        "heartbeat": heartbeat,
        "issues_count": len(issues),
        "issues": [issue.to_dict() for issue in issues],
    }


def require_buy_allowed(source_module: str = "buy_order") -> None:
    status = evaluate_recovery_status_sync()

    if bool((status.get("heartbeat") or {}).get("fallback_active")):
        database.safe_record_trade_journal_event_sync({
            "event_type": "RECOVERY_MANAGER_BROKER_FALLBACK_USED",
            "decision": "ALLOWED",
            "reason": "Broker-sync freshness fallback satisfied heartbeat requirements",
            "source_module": source_module,
            "raw_payload": {"freshness": (status.get("heartbeat") or {}).get("freshness")},
        })

    if not status.get("buy_blocked"):
        return

    reason = status.get("buy_block_reason") or f"Recovery state blocks BUY orders: {status.get('state')}"

    database.safe_record_trade_journal_event_sync({
        "event_type": "BUY_BLOCKED_BY_RECOVERY_MANAGER",
        "decision": "BLOCKED",
        "reason": reason,
        "source_module": source_module,
        "raw_payload": status,
    })

    raise RuntimeError(f"BUY blocked by recovery manager: {reason}")

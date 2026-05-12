from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiosqlite

VALID_RECONCILIATION_STATUSES = {"OPEN", "RESOLVED", "AUTO_FIXED", "IGNORED"}
OPEN_STATUS = "OPEN"
RESOLVED_STATUS = "RESOLVED"
AUTO_FIXED_STATUS = "AUTO_FIXED"
IGNORED_STATUS = "IGNORED"

CREATE_RECONCILIATION_ISSUES = """
CREATE TABLE IF NOT EXISTS reconciliation_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    issue_type TEXT,
    severity TEXT,
    db_quantity REAL,
    tws_quantity REAL,
    execution_quantity REAL,
    details TEXT,
    status TEXT DEFAULT 'OPEN',
    created_at TEXT,
    resolved_at TEXT
)
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_status(status: str | None) -> str:
    normalized = str(status or OPEN_STATUS).upper().strip()
    if normalized not in VALID_RECONCILIATION_STATUSES:
        return OPEN_STATUS
    return normalized


async def ensure_reconciliation_issue_schema(db: aiosqlite.Connection) -> None:
    await db.execute(CREATE_RECONCILIATION_ISSUES)

    async with db.execute("PRAGMA table_info(reconciliation_issues)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}

    if "status" not in columns:
        await db.execute("ALTER TABLE reconciliation_issues ADD COLUMN status TEXT DEFAULT 'OPEN'")

    if "resolved_at" not in columns:
        await db.execute("ALTER TABLE reconciliation_issues ADD COLUMN resolved_at TEXT")

    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_reconciliation_issues_status ON reconciliation_issues(status)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_reconciliation_issues_key ON reconciliation_issues(symbol, issue_type)"
    )


def issue_key(issue: dict[str, Any]) -> tuple[str, str]:
    return (
        str(issue.get("symbol") or "").upper().strip(),
        str(issue.get("issue_type") or "").upper().strip(),
    )


def _normalize_issue(issue: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(issue)
    normalized["symbol"] = str(normalized.get("symbol") or "").upper().strip()
    normalized["issue_type"] = str(normalized.get("issue_type") or "").upper().strip()
    normalized["severity"] = str(normalized.get("severity") or "MEDIUM").upper().strip()
    normalized["db_quantity"] = float(normalized.get("db_quantity") or 0)
    normalized["tws_quantity"] = float(normalized.get("tws_quantity") or 0)
    normalized["execution_quantity"] = float(normalized.get("execution_quantity") or 0)
    normalized["details"] = str(normalized.get("details") or "")
    return normalized


def row_to_issue(row: aiosqlite.Row) -> dict[str, Any]:
    data = dict(row)
    data["status"] = _clean_status(data.get("status"))
    return data


async def fetch_reconciliation_counters(db: aiosqlite.Connection) -> dict[str, int]:
    await ensure_reconciliation_issue_schema(db)
    db.row_factory = aiosqlite.Row

    counters = {
        "open": 0,
        "resolved": 0,
        "auto_fixed": 0,
        "ignored": 0,
    }

    async with db.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM reconciliation_issues
        GROUP BY status
        """
    ) as cursor:
        rows = await cursor.fetchall()

    for row in rows:
        status = _clean_status(row["status"]).lower()
        counters[status] = int(row["count"] or 0)

    return counters


async def fetch_reconciliation_issues(
    db: aiosqlite.Connection,
    *,
    status: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    await ensure_reconciliation_issue_schema(db)
    db.row_factory = aiosqlite.Row
    limit = max(1, min(int(limit or 200), 1000))

    if status:
        normalized_status = _clean_status(status)
        sql = """
            SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
                   execution_quantity, details, status, created_at, resolved_at
            FROM reconciliation_issues
            WHERE status = ?
            ORDER BY id DESC
            LIMIT ?
        """
        params: tuple[Any, ...] = (normalized_status, limit)
    else:
        sql = """
            SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
                   execution_quantity, details, status, created_at, resolved_at
            FROM reconciliation_issues
            ORDER BY id DESC
            LIMIT ?
        """
        params = (limit,)

    async with db.execute(sql, params) as cursor:
        rows = await cursor.fetchall()

    return [row_to_issue(row) for row in rows]


async def _fetch_latest_issue_for_key(
    db: aiosqlite.Connection,
    *,
    symbol: str,
    issue_type: str,
) -> dict[str, Any] | None:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        """
        SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
               execution_quantity, details, status, created_at, resolved_at
        FROM reconciliation_issues
        WHERE symbol = ? AND issue_type = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (symbol, issue_type),
    ) as cursor:
        row = await cursor.fetchone()

    return row_to_issue(row) if row else None


async def sync_reconciliation_issue_lifecycle(
    db: aiosqlite.Connection,
    current_issues: list[dict[str, Any]],
    *,
    managed_issue_types: set[str] | None = None,
) -> dict[str, Any]:
    """Synchronize current reconciliation findings with persisted issue lifecycle.

    Historical rows are never deleted. Existing OPEN rows remain OPEN while the
    same issue persists, are marked RESOLVED when the issue disappears, and new
    rows are inserted when an issue first appears or recurs after resolution.
    IGNORED rows suppress reopening the same issue key until manually changed.
    When managed_issue_types is provided, only those issue types are resolved by
    this sync pass so independent reconciliation checks do not close each other.
    """
    await ensure_reconciliation_issue_schema(db)
    db.row_factory = aiosqlite.Row
    resolved_at = now_iso()

    managed_issue_types = {
        str(issue_type).upper().strip()
        for issue_type in managed_issue_types
    } if managed_issue_types is not None else None

    normalized_current = [_normalize_issue(issue) for issue in current_issues]
    current_by_key = {issue_key(issue): issue for issue in normalized_current}

    async with db.execute(
        """
        SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
               execution_quantity, details, status, created_at, resolved_at
        FROM reconciliation_issues
        WHERE status = 'OPEN'
        ORDER BY id DESC
        """
    ) as cursor:
        open_rows = [row_to_issue(row) for row in await cursor.fetchall()]

    open_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    duplicate_open_ids: list[int] = []

    for row in open_rows:
        issue_type = str(row.get("issue_type") or "").upper().strip()
        if managed_issue_types is not None and issue_type not in managed_issue_types:
            continue

        key = issue_key(row)
        if key in open_by_key:
            duplicate_open_ids.append(int(row["id"]))
            continue
        open_by_key[key] = row

    resolved_issues: list[dict[str, Any]] = []

    for duplicate_id in duplicate_open_ids:
        await db.execute(
            """
            UPDATE reconciliation_issues
            SET status = 'RESOLVED', resolved_at = ?
            WHERE id = ? AND status = 'OPEN'
            """,
            (resolved_at, duplicate_id),
        )

    for key, row in open_by_key.items():
        if key in current_by_key:
            continue
        await db.execute(
            """
            UPDATE reconciliation_issues
            SET status = 'RESOLVED', resolved_at = ?
            WHERE id = ? AND status = 'OPEN'
            """,
            (resolved_at, row["id"]),
        )
        row = dict(row)
        row["status"] = RESOLVED_STATUS
        row["resolved_at"] = resolved_at
        resolved_issues.append(row)

    new_issues: list[dict[str, Any]] = []

    for key, issue in current_by_key.items():
        open_row = open_by_key.get(key)

        if open_row:
            await db.execute(
                """
                UPDATE reconciliation_issues
                SET severity = ?,
                    db_quantity = ?,
                    tws_quantity = ?,
                    execution_quantity = ?,
                    details = ?,
                    resolved_at = NULL
                WHERE id = ? AND status = 'OPEN'
                """,
                (
                    issue["severity"],
                    issue["db_quantity"],
                    issue["tws_quantity"],
                    issue["execution_quantity"],
                    issue["details"],
                    open_row["id"],
                ),
            )
            continue

        latest = await _fetch_latest_issue_for_key(
            db,
            symbol=issue["symbol"],
            issue_type=issue["issue_type"],
        )

        if latest and latest.get("status") == IGNORED_STATUS:
            continue

        created_at = now_iso()
        cursor = await db.execute(
            """
            INSERT INTO reconciliation_issues (
                symbol, issue_type, severity, db_quantity, tws_quantity,
                execution_quantity, details, status, created_at, resolved_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, NULL)
            """,
            (
                issue["symbol"],
                issue["issue_type"],
                issue["severity"],
                issue["db_quantity"],
                issue["tws_quantity"],
                issue["execution_quantity"],
                issue["details"],
                created_at,
            ),
        )
        issue = dict(issue)
        issue["id"] = cursor.lastrowid
        issue["status"] = OPEN_STATUS
        issue["created_at"] = created_at
        issue["resolved_at"] = None
        new_issues.append(issue)

    open_issues = await fetch_reconciliation_issues(db, status=OPEN_STATUS, limit=1000)
    counters = await fetch_reconciliation_counters(db)

    return {
        "open_issues": open_issues,
        "resolved_issues": resolved_issues,
        "new_issues": new_issues,
        "counters": counters,
    }

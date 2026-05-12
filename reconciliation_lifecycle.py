from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

import aiosqlite

import config

log = logging.getLogger(__name__)

STATUS_OPEN = "OPEN"
STATUS_RESOLVED = "RESOLVED"
STATUS_AUTO_FIXED = "AUTO_FIXED"
STATUS_IGNORED = "IGNORED"
ISSUE_STATUSES = (STATUS_OPEN, STATUS_RESOLVED, STATUS_AUTO_FIXED, STATUS_IGNORED)

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


def normalize_status(status: str | None) -> str:
    normalized = str(status or STATUS_OPEN).upper().strip()
    if normalized not in ISSUE_STATUSES:
        return STATUS_OPEN
    return normalized


def issue_key(issue: dict) -> tuple[str, str]:
    return (
        str(issue.get("symbol") or "").upper().strip(),
        str(issue.get("issue_type") or "").upper().strip(),
    )


def normalize_issue(issue: dict) -> dict:
    normalized = dict(issue)
    normalized["symbol"] = str(normalized.get("symbol") or "").upper().strip()
    normalized["issue_type"] = str(normalized.get("issue_type") or "").upper().strip()
    normalized["severity"] = str(normalized.get("severity") or "MEDIUM").upper().strip()
    normalized["db_quantity"] = float(normalized.get("db_quantity") or 0)
    normalized["tws_quantity"] = float(normalized.get("tws_quantity") or 0)
    normalized["execution_quantity"] = float(normalized.get("execution_quantity") or 0)
    normalized["details"] = str(normalized.get("details") or "")
    return normalized


async def init_reconciliation_lifecycle_db(db: aiosqlite.Connection | None = None) -> None:
    owns_connection = db is None
    if db is None:
        db = await aiosqlite.connect(config.DB_PATH)

    try:
        await db.execute(CREATE_RECONCILIATION_ISSUES)
        async with db.execute("PRAGMA table_info(reconciliation_issues)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}

        if "status" not in columns:
            await db.execute("ALTER TABLE reconciliation_issues ADD COLUMN status TEXT DEFAULT 'OPEN'")
        if "resolved_at" not in columns:
            await db.execute("ALTER TABLE reconciliation_issues ADD COLUMN resolved_at TEXT")

        await db.execute(
            """
            UPDATE reconciliation_issues
            SET status = 'OPEN'
            WHERE status IS NULL OR TRIM(status) = ''
            """
        )
        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reconciliation_issues_status_key
            ON reconciliation_issues(status, symbol, issue_type)
            """
        )
        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reconciliation_issues_created_at
            ON reconciliation_issues(created_at)
            """
        )
        await db.commit()
    finally:
        if owns_connection:
            await db.close()


async def init_reconciliation_db() -> None:
    await init_reconciliation_lifecycle_db()


async def reconcile_issue_lifecycle(
    db: aiosqlite.Connection,
    current_issues: Iterable[dict],
    *,
    resolved_at: str | None = None,
) -> list[dict]:
    """Merge the current reconciliation snapshot into issue lifecycle history.

    Existing OPEN rows remain OPEN while the same symbol/issue_type is still present.
    OPEN rows absent from the current snapshot are marked RESOLVED. Historical rows
    with RESOLVED, AUTO_FIXED, or IGNORED status are preserved, so a recurring issue
    inserts a new OPEN row instead of mutating old history.
    """
    await init_reconciliation_lifecycle_db(db)

    current = [normalize_issue(issue) for issue in current_issues]
    current_by_key = {issue_key(issue): issue for issue in current}
    timestamp = resolved_at or now_iso()

    db.row_factory = aiosqlite.Row
    async with db.execute(
        """
        SELECT *
        FROM reconciliation_issues
        WHERE status = 'OPEN'
        ORDER BY id ASC
        """
    ) as cursor:
        open_rows = await cursor.fetchall()

    open_by_key: dict[tuple[str, str], aiosqlite.Row] = {}
    duplicate_open_ids: list[int] = []
    for row in open_rows:
        key = (str(row["symbol"] or "").upper().strip(), str(row["issue_type"] or "").upper().strip())
        if key not in open_by_key:
            open_by_key[key] = row
        else:
            duplicate_open_ids.append(int(row["id"]))

    if duplicate_open_ids:
        await db.executemany(
            """
            UPDATE reconciliation_issues
            SET status = 'RESOLVED',
                resolved_at = ?
            WHERE id = ? AND status = 'OPEN'
            """,
            [(timestamp, issue_id) for issue_id in duplicate_open_ids],
        )

    for key, row in open_by_key.items():
        if key in current_by_key:
            issue = current_by_key[key]
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
                    row["id"],
                ),
            )
        else:
            await db.execute(
                """
                UPDATE reconciliation_issues
                SET status = 'RESOLVED',
                    resolved_at = ?
                WHERE id = ? AND status = 'OPEN'
                """,
                (timestamp, row["id"]),
            )

    for key, issue in current_by_key.items():
        if key in open_by_key:
            continue
        await db.execute(
            """
            INSERT INTO reconciliation_issues (
                symbol,
                issue_type,
                severity,
                db_quantity,
                tws_quantity,
                execution_quantity,
                details,
                status,
                created_at,
                resolved_at
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
                timestamp,
            ),
        )

    await db.commit()
    return await fetch_open_reconciliation_issues(db)


async def fetch_open_reconciliation_issues(db: aiosqlite.Connection | None = None) -> list[dict]:
    owns_connection = db is None
    if db is None:
        db = await aiosqlite.connect(config.DB_PATH)

    try:
        await init_reconciliation_lifecycle_db(db)
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
                   execution_quantity, details, status, created_at, resolved_at
            FROM reconciliation_issues
            WHERE status = 'OPEN'
            ORDER BY severity DESC, created_at DESC, id DESC
            """
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        if owns_connection:
            await db.close()


async def fetch_reconciliation_counters(db: aiosqlite.Connection | None = None) -> dict:
    owns_connection = db is None
    if db is None:
        db = await aiosqlite.connect(config.DB_PATH)

    try:
        await init_reconciliation_lifecycle_db(db)
        db.row_factory = aiosqlite.Row
        counters = {status.lower(): 0 for status in ISSUE_STATUSES}
        async with db.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM reconciliation_issues
            GROUP BY status
            """
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            status = normalize_status(row["status"]).lower()
            counters[status] = int(row["count"] or 0)
        counters["total"] = sum(counters.values())
        counters["non_open"] = counters["total"] - counters[STATUS_OPEN.lower()]
        return counters
    finally:
        if owns_connection:
            await db.close()


async def update_reconciliation_issue_status(issue_id: int, status: str) -> dict | None:
    selected_status = normalize_status(status)
    resolved_at = None if selected_status == STATUS_OPEN else now_iso()

    async with aiosqlite.connect(config.DB_PATH) as db:
        await init_reconciliation_lifecycle_db(db)
        await db.execute(
            """
            UPDATE reconciliation_issues
            SET status = ?,
                resolved_at = ?
            WHERE id = ?
            """,
            (selected_status, resolved_at, int(issue_id)),
        )
        await db.commit()
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
                   execution_quantity, details, status, created_at, resolved_at
            FROM reconciliation_issues
            WHERE id = ?
            """,
            (int(issue_id),),
        ) as cursor:
            row = await cursor.fetchone()

    return dict(row) if row else None


async def get_reconciliation_status() -> dict:
    async with aiosqlite.connect(config.DB_PATH) as db:
        issues = await fetch_open_reconciliation_issues(db)
        counters = await fetch_reconciliation_counters(db)

    return {
        "ok": len(issues) == 0,
        "issues_count": len(issues),
        "open_count": len(issues),
        "issues": issues,
        "counters": counters,
        "checked_at": now_iso(),
    }


async def get_reconciliation_history(limit: int = 200, status: str | None = None) -> dict:
    limit = max(1, min(int(limit or 200), 1000))
    selected_status = normalize_status(status) if status else None

    async with aiosqlite.connect(config.DB_PATH) as db:
        await init_reconciliation_lifecycle_db(db)
        db.row_factory = aiosqlite.Row
        params: tuple = (limit,)
        where = ""
        if selected_status:
            where = "WHERE status = ?"
            params = (selected_status, limit)

        async with db.execute(
            f"""
            SELECT id, symbol, issue_type, severity, db_quantity, tws_quantity,
                   execution_quantity, details, status, created_at, resolved_at
            FROM reconciliation_issues
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            params,
        ) as cursor:
            rows = await cursor.fetchall()
        counters = await fetch_reconciliation_counters(db)

    return {
        "history": [dict(row) for row in rows],
        "count": len(rows),
        "limit": limit,
        "status": selected_status,
        "counters": counters,
        "checked_at": now_iso(),
    }

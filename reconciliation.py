from __future__ import annotations

import logging
from datetime import datetime, timezone

import aiosqlite

import config

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


async def init_reconciliation_db() -> None:
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(CREATE_RECONCILIATION_ISSUES)
        await db.commit()


async def _load_db_positions(db: aiosqlite.Connection) -> dict[str, dict]:
    db.row_factory = aiosqlite.Row

    cursor = await db.execute(
        """
        SELECT
            symbol,
            quantity,
            status
        FROM positions
        WHERE status = 'OPEN'
        """
    )

    rows = await cursor.fetchall()

    result = {}

    for row in rows:
        symbol = str(row["symbol"] or "").upper().strip()

        if not symbol:
            continue

        result[symbol] = {
            "symbol": symbol,
            "quantity": float(row["quantity"] or 0),
            "status": row["status"],
        }

    return result


async def _load_tws_positions(db: aiosqlite.Connection) -> dict[str, dict]:
    db.row_factory = aiosqlite.Row

    cursor = await db.execute(
        """
        SELECT
            symbol,
            quantity,
            avg_cost,
            market_value,
            unrealized_pnl,
            updated_at
        FROM tws_positions
        """
    )

    rows = await cursor.fetchall()

    result = {}

    for row in rows:
        symbol = str(row["symbol"] or "").upper().strip()

        if not symbol:
            continue

        result[symbol] = {
            "symbol": symbol,
            "quantity": float(row["quantity"] or 0),
            "avg_cost": float(row["avg_cost"] or 0),
            "market_value": float(row["market_value"] or 0),
            "unrealized_pnl": float(row["unrealized_pnl"] or 0),
            "updated_at": row["updated_at"],
        }

    return result


async def _load_execution_quantities(db: aiosqlite.Connection) -> dict[str, float]:
    db.row_factory = aiosqlite.Row

    cursor = await db.execute(
        """
        SELECT
            symbol,
            side,
            SUM(quantity) AS qty
        FROM executions
        GROUP BY symbol, side
        """
    )

    rows = await cursor.fetchall()

    result: dict[str, float] = {}

    for row in rows:
        symbol = str(row["symbol"] or "").upper().strip()
        side = str(row["side"] or "").upper().strip()
        qty = float(row["qty"] or 0)

        if not symbol:
            continue

        if symbol not in result:
            result[symbol] = 0.0

        if side in ("BOT", "BUY"):
            result[symbol] += qty

        elif side in ("SLD", "SELL"):
            result[symbol] -= qty

    return result


async def _insert_issue(
    db: aiosqlite.Connection,
    symbol: str,
    issue_type: str,
    severity: str,
    db_quantity: float,
    tws_quantity: float,
    execution_quantity: float,
    details: str,
) -> None:
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
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
        """,
        (
            symbol,
            issue_type,
            severity,
            db_quantity,
            tws_quantity,
            execution_quantity,
            details,
            now_iso(),
        ),
    )


async def run_reconciliation_once() -> dict:
    await init_reconciliation_db()

    async with aiosqlite.connect(config.DB_PATH) as db:
        db_positions = await _load_db_positions(db)
        tws_positions = await _load_tws_positions(db)
        execution_quantities = await _load_execution_quantities(db)

        await db.execute(
            """
            UPDATE reconciliation_issues
            SET status = 'RESOLVED',
                resolved_at = ?
            WHERE status = 'OPEN'
            """,
            (now_iso(),),
        )

        symbols = set()
        symbols.update(db_positions.keys())
        symbols.update(tws_positions.keys())
        symbols.update(execution_quantities.keys())

        issues = []

        for symbol in sorted(symbols):
            db_qty = float(db_positions.get(symbol, {}).get("quantity", 0) or 0)
            tws_qty = float(tws_positions.get(symbol, {}).get("quantity", 0) or 0)
            exec_qty = float(execution_quantities.get(symbol, 0) or 0)

            # DB says open, TWS has nothing
            if db_qty > 0 and tws_qty <= 0:
                issue = {
                    "symbol": symbol,
                    "issue_type": "DB_OPEN_BUT_TWS_FLAT",
                    "severity": "HIGH",
                    "db_quantity": db_qty,
                    "tws_quantity": tws_qty,
                    "execution_quantity": exec_qty,
                    "details": "DB has an OPEN position but TWS has no matching open position.",
                }
                issues.append(issue)

            # TWS has position, DB does not
            elif tws_qty > 0 and db_qty <= 0:
                issue = {
                    "symbol": symbol,
                    "issue_type": "TWS_OPEN_BUT_DB_FLAT",
                    "severity": "HIGH",
                    "db_quantity": db_qty,
                    "tws_quantity": tws_qty,
                    "execution_quantity": exec_qty,
                    "details": "TWS has an open position but DB does not have matching OPEN position.",
                }
                issues.append(issue)

            # both open but quantity mismatch
            elif db_qty > 0 and tws_qty > 0 and abs(db_qty - tws_qty) > 0.0001:
                issue = {
                    "symbol": symbol,
                    "issue_type": "POSITION_QUANTITY_MISMATCH",
                    "severity": "MEDIUM",
                    "db_quantity": db_qty,
                    "tws_quantity": tws_qty,
                    "execution_quantity": exec_qty,
                    "details": "DB position quantity does not match TWS quantity.",
                }
                issues.append(issue)

            # executions do not match TWS net position
            if exec_qty and tws_qty and abs(exec_qty - tws_qty) > 0.0001:
                issue = {
                    "symbol": symbol,
                    "issue_type": "EXECUTION_TWS_MISMATCH",
                    "severity": "MEDIUM",
                    "db_quantity": db_qty,
                    "tws_quantity": tws_qty,
                    "execution_quantity": exec_qty,
                    "details": "Net execution quantity does not match TWS open quantity.",
                }
                issues.append(issue)

        for issue in issues:
            await _insert_issue(
                db,
                issue["symbol"],
                issue["issue_type"],
                issue["severity"],
                issue["db_quantity"],
                issue["tws_quantity"],
                issue["execution_quantity"],
                issue["details"],
            )

        await db.commit()

    log.info(
        "Reconciliation complete | issues=%s",
        len(issues),
    )

    return {
        "ok": len(issues) == 0,
        "issues_count": len(issues),
        "issues": issues,
        "checked_at": now_iso(),
    }
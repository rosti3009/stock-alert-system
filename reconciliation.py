from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import aiosqlite

import config
import database
from ibkr_asyncio_compat import ensure_event_loop
from ibkr_client import IBKRClient
from reconciliation_lifecycle import (
    init_reconciliation_lifecycle_db,
    reconcile_issue_lifecycle,
)

log = logging.getLogger(__name__)

PAPER_TWS_PORT = 7497
BASELINE_SOURCE = "TWS_BASELINE_ADOPTED"
RECONCILIATION_CLIENT_ID_OFFSET = 450
FLAT_RECONCILIATION_REASON = "Closed in TWS / reconciled flat"


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
    await init_reconciliation_lifecycle_db()


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


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

    if not await _table_exists(db, "tws_positions"):
        return {}

    cursor = await db.execute(
        """
        SELECT
            symbol,
            quantity,
            avg_cost,
            market_price,
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
            "market_price": float(row["market_price"] or 0),
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


def _require_paper_adoption_allowed() -> None:
    if not config.IBKR_PAPER_TRADING:
        raise RuntimeError("TWS baseline adoption blocked: IBKR_PAPER_TRADING is false")

    if config.IBKR_ENABLE_REAL_TRADING:
        raise RuntimeError("TWS baseline adoption blocked: LIVE trading is enabled")

    if int(config.IBKR_PORT) != PAPER_TWS_PORT:
        raise RuntimeError(
            f"TWS baseline adoption blocked: IBKR port is not Paper port {PAPER_TWS_PORT}"
        )

    if str(config.TRADING_MODE or "").upper() == "LIVE":
        raise RuntimeError("TWS baseline adoption blocked: TRADING_MODE is LIVE")


def _require_paper_reconciliation_allowed() -> None:
    if not config.IBKR_PAPER_TRADING:
        raise RuntimeError("TWS flat reconciliation blocked: IBKR_PAPER_TRADING is false")

    if config.IBKR_ENABLE_REAL_TRADING:
        raise RuntimeError("TWS flat reconciliation blocked: LIVE trading is enabled")

    if int(config.IBKR_PORT) != PAPER_TWS_PORT:
        raise RuntimeError(
            f"TWS flat reconciliation blocked: IBKR port is not Paper port {PAPER_TWS_PORT}"
        )

    if str(config.TRADING_MODE or "").upper() == "LIVE":
        raise RuntimeError("TWS flat reconciliation blocked: TRADING_MODE is LIVE")


def _position_symbol(position) -> str:
    contract = getattr(position, "contract", None)
    symbol = getattr(contract, "symbol", None)

    if symbol is None and isinstance(position, dict):
        symbol = position.get("symbol")

    return str(symbol or "").strip().upper()


def _position_quantity(position) -> float:
    if isinstance(position, dict):
        return float(position.get("quantity", position.get("position", 0)) or 0)

    return float(getattr(position, "position", 0) or 0)


def _current_tws_positions_from_client(ibkr_client=None) -> dict[str, dict]:
    ensure_event_loop()

    client_created = ibkr_client is None
    client = ibkr_client or IBKRClient(
        client_id=int(config.IBKR_CLIENT_ID) + RECONCILIATION_CLIENT_ID_OFFSET
    )
    connected = False

    try:
        if hasattr(client, "is_connected") and client.is_connected():
            connected = True
        elif hasattr(client, "connect"):
            connected = bool(client.connect())
        else:
            connected = True

        if not connected:
            raise RuntimeError(
                "TWS flat reconciliation blocked: unable to connect to IBKR TWS Paper Trading"
            )

        positions: dict[str, dict] = {}

        for position in list(client.get_positions()):
            symbol = _position_symbol(position)

            if not symbol:
                continue

            positions[symbol] = {
                "symbol": symbol,
                "quantity": _position_quantity(position),
            }

        return positions

    finally:
        if client_created and hasattr(client, "disconnect"):
            client.disconnect()


async def close_db_positions_flat_in_tws(
    *,
    dry_run: bool = False,
    ibkr_client=None,
) -> dict:
    """Close local OPEN positions whose symbols are no longer long in TWS.

    This is a DB-only reconciliation path for manual TWS sells. It only reads
    TWS positions and updates local SQLite state; it never submits IBKR orders.
    """
    _require_paper_reconciliation_allowed()
    await database.init_db()

    tws_positions = await asyncio.to_thread(
        _current_tws_positions_from_client,
        ibkr_client=ibkr_client,
    )
    tws_long_symbols = {
        symbol
        for symbol, position in tws_positions.items()
        if float(position.get("quantity") or 0) > 0
    }

    closed_symbols: list[str] = []
    skipped_symbols: list[str] = []
    skipped_reasons: dict[str, str] = {}
    close_payloads: list[dict] = []
    remaining_issues: list[dict] = []
    would_close_symbols: list[str] = []
    db_open_before = 0

    async with aiosqlite.connect(config.DB_PATH) as db:
        await init_reconciliation_db()
        await _ensure_position_source_column(db)
        db_positions = await _load_db_positions(db)
        db_open_before = len(db_positions)
        now = now_iso()

        for symbol in sorted(db_positions.keys()):
            db_position = db_positions[symbol]
            db_quantity = float(db_position.get("quantity") or 0)
            tws_position = tws_positions.get(symbol)
            tws_quantity = float((tws_position or {}).get("quantity") or 0)

            if symbol in tws_long_symbols:
                skipped_symbols.append(symbol)
                skipped_reasons[symbol] = "TWS still reports a positive open position"
                continue

            issue = {
                "symbol": symbol,
                "issue_type": "DB_OPEN_TWS_FLAT",
                "db_quantity": db_quantity,
                "tws_quantity": tws_quantity,
                "details": "Local DB position is OPEN but TWS has no positive quantity",
            }
            remaining_issues.append(issue)
            would_close_symbols.append(symbol)

            if dry_run:
                continue

            await db.execute(
                """
                UPDATE positions
                SET status = 'CLOSED',
                    action = 'RECONCILED_CLOSED',
                    reason = ?,
                    closed_at = ?,
                    updated_at = ?
                WHERE symbol = ?
                  AND status = 'OPEN'
                """,
                (FLAT_RECONCILIATION_REASON, now, now, symbol),
            )
            closed_symbols.append(symbol)
            close_payloads.append({
                "symbol": symbol,
                "quantity": db_quantity,
                "tws_quantity": tws_quantity,
                "status": "CLOSED",
                "action": "RECONCILED_CLOSED",
                "reason": FLAT_RECONCILIATION_REASON,
                "closed_at": now,
                "updated_at": now,
            })

        if not dry_run:
            await db.commit()
            remaining_issues = [
                issue
                for issue in remaining_issues
                if issue["symbol"] not in set(closed_symbols)
            ]

    for payload in close_payloads:
        await database.safe_record_trade_journal_event({
            "symbol": payload["symbol"],
            "event_type": "POSITION_RECONCILED_CLOSED",
            "decision": "RECONCILED_CLOSED",
            "reason": FLAT_RECONCILIATION_REASON,
            "source_module": "reconciliation.close_db_positions_flat_in_tws",
            "quantity": payload["quantity"],
            "raw_payload": payload,
        })

    return {
        "tws_positions_count": len(tws_positions),
        "db_open_before": db_open_before,
        "closed_count": len(closed_symbols),
        "closed_symbols": closed_symbols,
        "would_close_symbols": would_close_symbols,
        "skipped_symbols": skipped_symbols,
        "skipped_reasons": skipped_reasons,
        "remaining_issues": remaining_issues,
        "dry_run": bool(dry_run),
    }


def close_db_positions_flat_in_tws_worker(
    *,
    dry_run: bool = False,
    ibkr_client=None,
) -> dict:
    """Run flat TWS reconciliation from a synchronous worker thread.

    FastAPI handlers use this wrapper so ib_insync synchronous APIs never run
    on the already-running request event loop. The worker thread installs a
    default event loop for ib_insync compatibility before driving the async DB
    portion of the reconciliation. The TWS read itself is still isolated with
    ``asyncio.to_thread`` inside ``close_db_positions_flat_in_tws`` so sync IBKR
    calls do not execute while that worker loop is running.
    """
    loop = ensure_event_loop()
    if loop.is_running():
        raise RuntimeError(
            "TWS flat reconciliation worker must not run inside an active event loop"
        )

    return loop.run_until_complete(
        close_db_positions_flat_in_tws(
            dry_run=dry_run,
            ibkr_client=ibkr_client,
        )
    )


async def _ensure_position_recovery_columns(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(positions)") as cursor:
        existing = {row[1] for row in await cursor.fetchall()}

    if "source" not in existing:
        await db.execute("ALTER TABLE positions ADD COLUMN source TEXT")
    if "recovery_source_position_id" not in existing:
        await db.execute("ALTER TABLE positions ADD COLUMN recovery_source_position_id INTEGER")


async def _ensure_position_source_column(db: aiosqlite.Connection) -> None:
    await _ensure_position_recovery_columns(db)


def _baseline_notes(position: dict) -> str:
    return (
        f"source={BASELINE_SOURCE} | "
        f"avg_cost={position.get('avg_cost')} | "
        f"market_price={position.get('market_price')} | "
        f"market_value={position.get('market_value')} | "
        f"tws_updated_at={position.get('updated_at')}"
    )


async def adopt_tws_positions_as_baseline() -> dict:
    """Manually adopt existing paper TWS positions into local positions.

    This function only reads the latest TWS mirror tables and writes local DB
    baseline rows plus journal events. It never places, cancels, or closes
    orders and is intended to be called only by the operator-triggered API.
    """
    _require_paper_adoption_allowed()
    await database.init_db()

    adopted_symbols: list[str] = []
    skipped_symbols: list[str] = []
    skipped_reasons: dict[str, str] = {}
    adopted_payloads: list[dict] = []

    async with aiosqlite.connect(config.DB_PATH) as db:
        await init_reconciliation_db()
        await _ensure_position_source_column(db)

        db_positions = await _load_db_positions(db)
        tws_positions = await _load_tws_positions(db)

        now = now_iso()

        for symbol in sorted(tws_positions.keys()):
            tws_position = tws_positions[symbol]
            quantity = float(tws_position.get("quantity") or 0)

            if quantity <= 0:
                skipped_symbols.append(symbol)
                skipped_reasons[symbol] = "TWS quantity is not positive"
                continue

            if symbol in db_positions:
                skipped_symbols.append(symbol)
                skipped_reasons[symbol] = "Position already exists as OPEN in local DB"
                continue

            avg_cost = float(tws_position.get("avg_cost") or 0)
            market_price = float(tws_position.get("market_price") or 0)
            buy_price = avg_cost if avg_cost > 0 else market_price

            if buy_price <= 0:
                skipped_symbols.append(symbol)
                skipped_reasons[symbol] = "TWS position has no usable avg_cost or market_price"
                continue

            payload = {
                "symbol": symbol,
                "quantity": quantity,
                "buy_price": buy_price,
                "current_price": market_price if market_price > 0 else buy_price,
                "avg_cost": avg_cost,
                "market_price": market_price,
                "market_value": tws_position.get("market_value"),
                "unrealized_pnl": tws_position.get("unrealized_pnl"),
                "tws_updated_at": tws_position.get("updated_at"),
                "source": BASELINE_SOURCE,
            }

            await db.execute(
                """
                INSERT INTO positions (
                    symbol, buy_price, quantity, buy_date, current_price,
                    profit_amount, profit_percent, stop_loss, take_profit_1,
                    take_profit_2, status, action, reason, notes, source,
                    created_at, updated_at, closed_at
                )
                VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL,
                    'OPEN', 'HOLD', ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(symbol) DO UPDATE SET
                    buy_price = excluded.buy_price,
                    quantity = excluded.quantity,
                    buy_date = excluded.buy_date,
                    current_price = excluded.current_price,
                    profit_amount = NULL,
                    profit_percent = NULL,
                    stop_loss = NULL,
                    take_profit_1 = NULL,
                    take_profit_2 = NULL,
                    status = 'OPEN',
                    action = 'HOLD',
                    reason = excluded.reason,
                    notes = excluded.notes,
                    source = excluded.source,
                    updated_at = excluded.updated_at,
                    closed_at = NULL
                WHERE positions.status IS NULL OR positions.status != 'OPEN'
                """,
                (
                    symbol,
                    round(buy_price, 4),
                    round(quantity, 6),
                    now,
                    round(market_price if market_price > 0 else buy_price, 4),
                    "Adopted from existing TWS position",
                    _baseline_notes(payload),
                    BASELINE_SOURCE,
                    now,
                    now,
                ),
            )

            adopted_symbols.append(symbol)
            adopted_payloads.append(payload)

        await db.commit()

    for payload in adopted_payloads:
        await database.safe_record_trade_journal_event({
            "symbol": payload["symbol"],
            "event_type": "POSITION_BASELINE_ADOPTED",
            "decision": "ADOPTED",
            "reason": "Adopted from existing TWS position",
            "source_module": "reconciliation.adopt_tws_positions_as_baseline",
            "price": payload["buy_price"],
            "quantity": payload["quantity"],
            "unrealized_pnl": payload.get("unrealized_pnl"),
            "raw_payload": payload,
        })

    reconciliation = await run_reconciliation_once()

    return {
        "adopted_count": len(adopted_symbols),
        "skipped_count": len(skipped_symbols),
        "symbols_adopted": adopted_symbols,
        "symbols_skipped": skipped_symbols,
        "skipped_reasons": skipped_reasons,
        "remaining_reconciliation_issues": reconciliation.get("issues", []),
        "remaining_reconciliation_issues_count": reconciliation.get("issues_count", 0),
        "checked_at": now_iso(),
    }


RECOVERY_NOTE = "Recovered from live TWS position after reconciliation mismatch"
RECOVERY_SOURCE = "TWS_RECONCILIATION_RECOVERY"


def _append_recovery_note(existing_notes: str | None) -> str:
    notes = str(existing_notes or "").strip()
    if RECOVERY_NOTE in notes:
        return notes
    return f"{notes}\n{RECOVERY_NOTE}".strip() if notes else RECOVERY_NOTE


async def _load_latest_closed_position(db: aiosqlite.Connection, symbol: str) -> dict | None:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        """
        SELECT *
        FROM positions
        WHERE UPPER(TRIM(symbol)) = ?
          AND status = 'CLOSED'
        ORDER BY COALESCE(closed_at, updated_at, created_at) DESC, id DESC
        LIMIT 1
        """,
        (symbol,),
    ) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _recover_closed_position_from_tws(
    db: aiosqlite.Connection,
    symbol: str,
    tws_position: dict,
) -> dict | None:
    """Reopen a closed DB position when TWS still shows a live long position.

    This is a DB-only recovery path: it never places, cancels, or modifies TWS
    orders. It is intentionally limited to positive TWS quantities and only runs
    when no DB OPEN position exists for the symbol.
    """
    quantity = float(tws_position.get("quantity") or 0)
    if quantity <= 0:
        return None

    existing_open_qty = await _load_db_positions(db)
    if symbol in existing_open_qty:
        return None

    closed_position = await _load_latest_closed_position(db, symbol)
    if not closed_position:
        return None

    await _ensure_position_recovery_columns(db)

    avg_cost = float(tws_position.get("avg_cost") or 0)
    market_price = float(tws_position.get("market_price") or 0)
    buy_price = float(closed_position.get("buy_price") or 0)
    if buy_price <= 0 and avg_cost > 0:
        buy_price = avg_cost
    if buy_price <= 0 and market_price > 0:
        buy_price = market_price

    current_price = market_price if market_price > 0 else buy_price
    profit_amount = None
    profit_percent = None
    if buy_price > 0 and current_price > 0:
        profit_amount = round((current_price - buy_price) * quantity, 2)
        profit_percent = round(((current_price - buy_price) / buy_price) * 100, 2)

    now = now_iso()
    notes = _append_recovery_note(closed_position.get("notes"))
    reason = RECOVERY_NOTE

    cursor = await db.execute(
        """
        UPDATE positions
        SET status = 'OPEN',
            action = 'HOLD',
            quantity = ?,
            buy_price = ?,
            current_price = ?,
            profit_amount = ?,
            profit_percent = ?,
            reason = ?,
            notes = ?,
            source = ?,
            updated_at = ?,
            closed_at = NULL,
            recovery_source_position_id = COALESCE(recovery_source_position_id, id)
        WHERE id = ?
          AND status = 'CLOSED'
          AND NOT EXISTS (
              SELECT 1
              FROM positions p2
              WHERE UPPER(TRIM(p2.symbol)) = ?
                AND p2.status = 'OPEN'
                AND p2.id != positions.id
          )
        """,
        (
            round(quantity, 6),
            round(buy_price, 4),
            round(current_price, 4),
            profit_amount,
            profit_percent,
            reason,
            notes,
            RECOVERY_SOURCE,
            now,
            closed_position["id"],
            symbol,
        ),
    )

    if cursor.rowcount <= 0:
        return None

    recovered = dict(closed_position)
    recovered.update({
        "status": "OPEN",
        "action": "HOLD",
        "quantity": round(quantity, 6),
        "buy_price": round(buy_price, 4),
        "current_price": round(current_price, 4),
        "profit_amount": profit_amount,
        "profit_percent": profit_percent,
        "reason": reason,
        "notes": notes,
        "source": RECOVERY_SOURCE,
        "updated_at": now,
        "closed_at": None,
        "recovery_source_position_id": closed_position["id"],
    })
    return recovered


async def run_reconciliation_once() -> dict:
    await init_reconciliation_db()

    async with aiosqlite.connect(config.DB_PATH) as db:
        db_positions = await _load_db_positions(db)
        tws_positions = await _load_tws_positions(db)
        execution_quantities = await _load_execution_quantities(db)

        symbols = set()
        symbols.update(db_positions.keys())
        symbols.update(tws_positions.keys())
        symbols.update(execution_quantities.keys())

        issues = []
        recovered_positions = []

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
                    "broker_position_quantity": tws_qty,
                    "db_position_status": "OPEN",
                    "reconciliation_decision": "REVIEW_CLOSE_CONFIRMATION_REQUIRED",
                    "position_truth_source": "BROKER_SNAPSHOT",
                }
                issues.append(issue)

            # TWS has position, DB does not
            elif tws_qty > 0 and db_qty <= 0:
                recovered = await _recover_closed_position_from_tws(
                    db,
                    symbol,
                    tws_positions.get(symbol, {}),
                )
                if recovered:
                    recovered_positions.append(recovered)
                    db_qty = float(recovered.get("quantity") or 0)
                    db_positions[symbol] = {
                        "symbol": symbol,
                        "quantity": db_qty,
                        "status": "OPEN",
                    }
                else:
                    issue = {
                        "symbol": symbol,
                        "issue_type": "TWS_OPEN_BUT_DB_FLAT",
                        "severity": "HIGH",
                        "db_quantity": db_qty,
                        "tws_quantity": tws_qty,
                        "execution_quantity": exec_qty,
                        "details": "TWS has an open position but DB does not have matching OPEN position.",
                        "broker_position_quantity": tws_qty,
                        "db_position_status": "CLOSED_OR_MISSING",
                        "reconciliation_decision": "REOPEN_FROM_BROKER_TRUTH_CANDIDATE",
                        "position_truth_source": "BROKER_SNAPSHOT",
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
                    "broker_position_quantity": tws_qty,
                    "db_position_status": "OPEN",
                    "reconciliation_decision": "SYNC_QUANTITY_TO_BROKER_REQUIRED",
                    "position_truth_source": "BROKER_SNAPSHOT",
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
                    "broker_position_quantity": tws_qty,
                    "db_position_status": "OPEN" if db_qty > 0 else "CLOSED_OR_MISSING",
                    "reconciliation_decision": "EXECUTION_RECON_REQUIRED",
                    "position_truth_source": "BROKER_SNAPSHOT",
                }
                issues.append(issue)

        open_issues = await reconcile_issue_lifecycle(db, issues)

    for recovered in recovered_positions:
        await database.safe_record_trade_journal_event({
            "symbol": recovered.get("symbol"),
            "event_type": "POSITION_RECONCILIATION_RECOVERED",
            "decision": "RECOVERED",
            "reason": RECOVERY_NOTE,
            "source_module": "reconciliation.run_reconciliation_once",
            "price": recovered.get("current_price"),
            "quantity": recovered.get("quantity"),
            "unrealized_pnl": recovered.get("profit_amount"),
            "raw_payload": recovered,
        })

    log.info(
        "Reconciliation complete | current_issues=%s open_issues=%s",
        len(issues),
        len(open_issues),
    )

    return {
        "ok": len(open_issues) == 0,
        "issues_count": len(open_issues),
        "issues": open_issues,
        "recovered_positions_count": len(recovered_positions),
        "recovered_positions": recovered_positions,
        "checked_at": now_iso(),
    }

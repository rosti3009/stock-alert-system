from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiosqlite
from ib_insync import IB

import config
import database

log = logging.getLogger(__name__)

ACCOUNT_SYNC_CLIENT_ID_OFFSET = 600
ACTIVE_ORDER_STATUSES = {
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
    "ApiPending",
}

ACCOUNT_FIELDS = {
    "NetLiquidation": "net_liquidation",
    "TotalCashValue": "total_cash_value",
    "BuyingPower": "buying_power",
    "AvailableFunds": "available_funds",
    "GrossPositionValue": "gross_position_value",
    "ExcessLiquidity": "excess_liquidity",
    "MaintMarginReq": "maint_margin_req",
    "UnrealizedPnL": "unrealized_pnl",
    "RealizedPnL": "realized_pnl",
}

CREATE_ACCOUNT_SUMMARY = """
CREATE TABLE IF NOT EXISTS account_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    account TEXT,
    net_liquidation REAL,
    total_cash_value REAL,
    buying_power REAL,
    available_funds REAL,
    gross_position_value REAL,
    excess_liquidity REAL,
    maint_margin_req REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    raw_json TEXT
)
"""

CREATE_OPEN_ORDERS = """
CREATE TABLE IF NOT EXISTS open_orders (
    order_id INTEGER PRIMARY KEY,
    perm_id INTEGER,
    symbol TEXT,
    action TEXT,
    order_type TEXT,
    total_quantity REAL,
    limit_price REAL,
    aux_price REAL,
    status TEXT,
    filled REAL,
    remaining REAL,
    avg_fill_price REAL,
    account TEXT,
    submitted_at TEXT,
    updated_at TEXT,
    raw_json TEXT
)
"""

CREATE_EXECUTION_HISTORY = """
CREATE TABLE IF NOT EXISTS execution_history (
    exec_id TEXT PRIMARY KEY,
    symbol TEXT,
    side TEXT,
    quantity REAL,
    fill_price REAL,
    commission REAL,
    realized_pnl REAL,
    order_id INTEGER,
    perm_id INTEGER,
    account TEXT,
    exchange TEXT,
    execution_timestamp TEXT,
    created_at TEXT,
    raw_json TEXT
)
"""

CREATE_EQUITY_CURVE = """
CREATE TABLE IF NOT EXISTS equity_curve (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    account TEXT,
    net_liquidation REAL,
    total_cash_value REAL,
    buying_power REAL,
    available_funds REAL,
    gross_position_value REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    source TEXT,
    raw_json TEXT
)
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def safe_str(value) -> str:
    try:
        return str(value or "")
    except Exception:
        return ""


def encode_json(value) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=str)


async def init_account_sync_db() -> None:
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(CREATE_ACCOUNT_SUMMARY)
        await db.execute(CREATE_OPEN_ORDERS)
        await db.execute(CREATE_EXECUTION_HISTORY)
        await db.execute(CREATE_EQUITY_CURVE)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_summary_timestamp ON account_summary(timestamp)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_open_orders_symbol ON open_orders(symbol)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_open_orders_status ON open_orders(status)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_execution_history_symbol ON execution_history(symbol)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_execution_history_timestamp ON execution_history(execution_timestamp)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_equity_curve_timestamp ON equity_curve(timestamp)"
        )
        await db.commit()


def _account_summary_from_rows(rows, pnl) -> dict:
    fields = {field: 0.0 for field in ACCOUNT_FIELDS.values()}
    raw_rows = []
    account = None

    for row in rows or []:
        tag = safe_str(getattr(row, "tag", ""))
        value = safe_str(getattr(row, "value", ""))
        currency = safe_str(getattr(row, "currency", ""))
        row_account = safe_str(getattr(row, "account", ""))

        raw_rows.append({
            "tag": tag,
            "value": value,
            "currency": currency,
            "account": row_account,
        })

        if row_account and not account:
            account = row_account

        field_name = ACCOUNT_FIELDS.get(tag)
        if field_name:
            fields[field_name] = safe_float(value)

    if pnl is not None:
        pnl_unrealized = safe_float(getattr(pnl, "unrealizedPnL", None))
        pnl_realized = safe_float(getattr(pnl, "realizedPnL", None))

        if pnl_unrealized:
            fields["unrealized_pnl"] = pnl_unrealized
        if pnl_realized:
            fields["realized_pnl"] = pnl_realized

    return {
        "timestamp": now_iso(),
        "account": account,
        **fields,
        "raw_json": encode_json({"account_summary": raw_rows, "pnl": getattr(pnl, "__dict__", None)}),
    }


def _order_row_from_trade(trade, timestamp: str) -> dict:
    contract = trade.contract
    order = trade.order
    status = trade.orderStatus
    order_time = None

    if getattr(trade, "log", None):
        order_time = trade.log[0].time

    filled = safe_float(status.filled)
    remaining = safe_float(status.remaining)
    order_status = safe_str(status.status)

    if filled > 0 and remaining > 0:
        order_status = "Partially Filled"
    elif filled > 0 and remaining <= 0:
        order_status = "Filled"

    return {
        "order_id": safe_int(order.orderId),
        "perm_id": safe_int(order.permId),
        "symbol": safe_str(getattr(contract, "symbol", "")).upper(),
        "action": safe_str(order.action).upper(),
        "order_type": safe_str(order.orderType),
        "total_quantity": safe_float(order.totalQuantity),
        "limit_price": safe_float(order.lmtPrice),
        "aux_price": safe_float(order.auxPrice),
        "status": order_status,
        "filled": filled,
        "remaining": remaining,
        "avg_fill_price": safe_float(status.avgFillPrice),
        "account": safe_str(order.account),
        "submitted_at": safe_str(order_time) or timestamp,
        "updated_at": timestamp,
        "raw_json": encode_json({
            "contract": getattr(contract, "__dict__", {}),
            "order": getattr(order, "__dict__", {}),
            "status": getattr(status, "__dict__", {}),
        }),
    }


def _execution_row_from_fill(fill) -> dict:
    contract = fill.contract
    execution = fill.execution
    commission_report = getattr(fill, "commissionReport", None)

    return {
        "exec_id": safe_str(execution.execId),
        "symbol": safe_str(getattr(contract, "symbol", "")).upper(),
        "side": safe_str(execution.side).upper(),
        "quantity": safe_float(execution.shares),
        "fill_price": safe_float(execution.price),
        "commission": safe_float(getattr(commission_report, "commission", 0.0)),
        "realized_pnl": safe_float(getattr(commission_report, "realizedPNL", 0.0)),
        "order_id": safe_int(execution.orderId),
        "perm_id": safe_int(execution.permId),
        "account": safe_str(execution.acctNumber),
        "exchange": safe_str(execution.exchange),
        "execution_timestamp": safe_str(execution.time),
        "created_at": now_iso(),
        "raw_json": encode_json({
            "contract": getattr(contract, "__dict__", {}),
            "execution": getattr(execution, "__dict__", {}),
            "commission_report": getattr(commission_report, "__dict__", {}),
        }),
    }


def fetch_account_state_sync() -> dict:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ib = IB()
    pnl = None
    account = None

    try:
        client_id = int(config.IBKR_CLIENT_ID) + ACCOUNT_SYNC_CLIENT_ID_OFFSET
        ib.connect(
            config.IBKR_HOST,
            int(config.IBKR_PORT),
            clientId=client_id,
            timeout=10,
        )

        accounts = ib.managedAccounts()
        if accounts:
            account = accounts[0]

        try:
            summary_rows = ib.reqAccountSummary()
        except Exception:
            summary_rows = ib.accountSummary()

        if account:
            try:
                pnl = ib.reqPnL(account, "")
                ib.sleep(1)
            except Exception as exc:
                log.warning("reqPnL failed: %s", exc)
                pnl = None

        try:
            ib.reqOpenOrders()
        except Exception:
            pass

        ib.sleep(1)
        open_orders = [
            _order_row_from_trade(trade, now_iso())
            for trade in ib.openTrades()
        ]

        try:
            fills = ib.reqExecutions()
        except Exception:
            fills = ib.executions()

        executions = [_execution_row_from_fill(fill) for fill in fills or []]

        positions = []
        for pos in ib.positions():
            positions.append({
                "symbol": safe_str(getattr(pos.contract, "symbol", "")).upper(),
                "quantity": safe_float(pos.position),
                "avg_cost": safe_float(pos.avgCost),
                "account": safe_str(pos.account),
            })

        summary = _account_summary_from_rows(summary_rows, pnl)
        summary["account"] = summary.get("account") or account

        return {
            "connected": True,
            "timestamp": summary["timestamp"],
            "account": summary.get("account"),
            "account_summary": summary,
            "open_orders": open_orders,
            "executions": executions,
            "positions": positions,
            "error": None,
        }

    finally:
        try:
            if pnl is not None and account:
                ib.cancelPnL(account, "")
        except Exception:
            pass

        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass

        try:
            loop.close()
        except Exception:
            pass


async def save_account_snapshot(snapshot: dict) -> None:
    await init_account_sync_db()
    summary = snapshot.get("account_summary") or {}
    timestamp = snapshot.get("timestamp") or summary.get("timestamp") or now_iso()
    open_orders = snapshot.get("open_orders") or []
    seen_order_ids = {
        safe_int(order.get("order_id"))
        for order in open_orders
        if safe_int(order.get("order_id"))
    }

    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO account_summary (
                timestamp, account, net_liquidation, total_cash_value,
                buying_power, available_funds, gross_position_value,
                excess_liquidity, maint_margin_req, unrealized_pnl,
                realized_pnl, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                summary.get("account"),
                summary.get("net_liquidation"),
                summary.get("total_cash_value"),
                summary.get("buying_power"),
                summary.get("available_funds"),
                summary.get("gross_position_value"),
                summary.get("excess_liquidity"),
                summary.get("maint_margin_req"),
                summary.get("unrealized_pnl"),
                summary.get("realized_pnl"),
                summary.get("raw_json") or encode_json(summary),
            ),
        )

        await db.execute(
            """
            INSERT INTO equity_curve (
                timestamp, account, net_liquidation, total_cash_value,
                buying_power, available_funds, gross_position_value,
                unrealized_pnl, realized_pnl, source, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                summary.get("account"),
                summary.get("net_liquidation"),
                summary.get("total_cash_value"),
                summary.get("buying_power"),
                summary.get("available_funds"),
                summary.get("gross_position_value"),
                summary.get("unrealized_pnl"),
                summary.get("realized_pnl"),
                "IBKR_ACCOUNT_SYNC",
                encode_json(summary),
            ),
        )

        for order in open_orders:
            await db.execute(
                """
                INSERT INTO open_orders (
                    order_id, perm_id, symbol, action, order_type,
                    total_quantity, limit_price, aux_price, status,
                    filled, remaining, avg_fill_price, account,
                    submitted_at, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    perm_id = excluded.perm_id,
                    symbol = excluded.symbol,
                    action = excluded.action,
                    order_type = excluded.order_type,
                    total_quantity = excluded.total_quantity,
                    limit_price = excluded.limit_price,
                    aux_price = excluded.aux_price,
                    status = excluded.status,
                    filled = excluded.filled,
                    remaining = excluded.remaining,
                    avg_fill_price = excluded.avg_fill_price,
                    account = excluded.account,
                    updated_at = excluded.updated_at,
                    raw_json = excluded.raw_json
                """,
                (
                    order.get("order_id"),
                    order.get("perm_id"),
                    order.get("symbol"),
                    order.get("action"),
                    order.get("order_type"),
                    order.get("total_quantity"),
                    order.get("limit_price"),
                    order.get("aux_price"),
                    order.get("status"),
                    order.get("filled"),
                    order.get("remaining"),
                    order.get("avg_fill_price"),
                    order.get("account"),
                    order.get("submitted_at") or timestamp,
                    timestamp,
                    order.get("raw_json") or encode_json(order),
                ),
            )

        if seen_order_ids:
            placeholders = ",".join("?" for _ in seen_order_ids)
            await db.execute(
                f"""
                UPDATE open_orders
                SET status = 'Cancelled',
                    remaining = 0,
                    updated_at = ?
                WHERE status IN ('PendingSubmit', 'PreSubmitted', 'Submitted', 'ApiPending')
                  AND order_id NOT IN ({placeholders})
                """,
                (timestamp, *seen_order_ids),
            )
        else:
            await db.execute(
                """
                UPDATE open_orders
                SET status = 'Cancelled',
                    remaining = 0,
                    updated_at = ?
                WHERE status IN ('PendingSubmit', 'PreSubmitted', 'Submitted', 'ApiPending')
                """,
                (timestamp,),
            )

        for execution in snapshot.get("executions") or []:
            await db.execute(
                """
                INSERT OR IGNORE INTO execution_history (
                    exec_id, symbol, side, quantity, fill_price,
                    commission, realized_pnl, order_id, perm_id,
                    account, exchange, execution_timestamp, created_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution.get("exec_id"),
                    execution.get("symbol"),
                    execution.get("side"),
                    execution.get("quantity"),
                    execution.get("fill_price"),
                    execution.get("commission"),
                    execution.get("realized_pnl"),
                    execution.get("order_id"),
                    execution.get("perm_id"),
                    execution.get("account"),
                    execution.get("exchange"),
                    execution.get("execution_timestamp"),
                    execution.get("created_at") or timestamp,
                    execution.get("raw_json") or encode_json(execution),
                ),
            )

        await db.commit()


async def run_account_sync_once() -> dict:
    try:
        snapshot = await asyncio.to_thread(fetch_account_state_sync)
        await save_account_snapshot(snapshot)
        log.info(
            "Account sync complete | account=%s orders=%s executions=%s",
            snapshot.get("account"),
            len(snapshot.get("open_orders") or []),
            len(snapshot.get("executions") or []),
        )
        return snapshot

    except Exception as exc:
        log.warning("Account sync failed: %s", exc)
        return {
            "connected": False,
            "timestamp": now_iso(),
            "account": None,
            "account_summary": {},
            "open_orders": [],
            "executions": [],
            "positions": [],
            "error": str(exc),
        }


async def get_latest_account_summary() -> dict:
    await init_account_sync_db()
    sql = """
    SELECT *
    FROM account_summary
    ORDER BY timestamp DESC, id DESC
    LIMIT 1
    """

    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql) as cursor:
            row = await cursor.fetchone()

    return dict(row) if row else {}


async def get_open_orders(limit: int = 100) -> list[dict]:
    await init_account_sync_db()
    limit = max(1, min(int(limit or 100), 500))
    sql = """
    SELECT *
    FROM open_orders
    ORDER BY updated_at DESC
    LIMIT ?
    """

    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


async def get_execution_history(limit: int = 100) -> list[dict]:
    await init_account_sync_db()
    limit = max(1, min(int(limit or 100), 1000))
    sql = """
    SELECT *
    FROM execution_history
    ORDER BY execution_timestamp DESC, created_at DESC
    LIMIT ?
    """

    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


async def _load_db_positions(db: aiosqlite.Connection) -> dict[str, float]:
    cursor = await db.execute(
        """
        SELECT symbol, quantity
        FROM positions
        WHERE status = 'OPEN'
        """
    )
    rows = await cursor.fetchall()
    return {
        safe_str(row["symbol"]).upper(): safe_float(row["quantity"])
        for row in rows
        if safe_str(row["symbol"])
    }


async def _load_ibkr_positions(db: aiosqlite.Connection) -> dict[str, float]:
    try:
        cursor = await db.execute(
            """
            SELECT symbol, quantity
            FROM tws_positions
            """
        )
        rows = await cursor.fetchall()
    except Exception as exc:
        log.warning("Unable to load TWS positions for reconciliation: %s", exc)
        return {}

    return {
        safe_str(row["symbol"]).upper(): safe_float(row["quantity"])
        for row in rows
        if safe_str(row["symbol"])
    }


async def _load_db_active_orders(db: aiosqlite.Connection) -> dict[int, dict]:
    cursor = await db.execute(
        """
        SELECT order_id, symbol, status, remaining
        FROM open_orders
        WHERE status IN ('PendingSubmit', 'PreSubmitted', 'Submitted', 'ApiPending')
        """
    )
    rows = await cursor.fetchall()
    return {safe_int(row["order_id"]): dict(row) for row in rows}


async def _load_ibkr_active_orders(db: aiosqlite.Connection) -> dict[int, dict]:
    try:
        cursor = await db.execute(
            """
            SELECT order_id, symbol, status, remaining
            FROM tws_orders
            WHERE status IN ('PendingSubmit', 'PreSubmitted', 'Submitted', 'ApiPending')
            """
        )
        rows = await cursor.fetchall()
    except Exception as exc:
        log.warning("Unable to load TWS orders for reconciliation: %s", exc)
        return {}

    return {safe_int(row["order_id"]): dict(row) for row in rows}


async def _journal_reconciliation_issue(issue: dict) -> None:
    await database.safe_record_trade_journal_event({
        "symbol": issue.get("symbol"),
        "event_type": "RECONCILIATION_MISMATCH",
        "decision": "WARNING_ONLY",
        "reason": issue.get("details"),
        "source_module": "account_sync.reconciliation",
        "quantity": issue.get("db_quantity"),
        "raw_payload": issue,
    })


async def run_account_reconciliation_once() -> dict:
    await init_account_sync_db()
    issues = []

    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        db_positions = await _load_db_positions(db)
        ibkr_positions = await _load_ibkr_positions(db)
        db_orders = await _load_db_active_orders(db)
        ibkr_orders = await _load_ibkr_active_orders(db)

    for symbol in sorted(set(db_positions) | set(ibkr_positions)):
        db_qty = db_positions.get(symbol, 0.0)
        ibkr_qty = ibkr_positions.get(symbol, 0.0)

        if abs(db_qty - ibkr_qty) > 0.0001:
            issues.append({
                "symbol": symbol,
                "issue_type": "POSITION_MISMATCH",
                "severity": "HIGH",
                "db_quantity": db_qty,
                "ibkr_quantity": ibkr_qty,
                "details": "DB open position quantity does not match IBKR/TWS position quantity.",
            })

    for order_id in sorted(set(db_orders) | set(ibkr_orders)):
        db_order = db_orders.get(order_id)
        ibkr_order = ibkr_orders.get(order_id)

        if db_order and not ibkr_order:
            issues.append({
                "symbol": db_order.get("symbol"),
                "issue_type": "DB_OPEN_ORDER_NOT_IN_IBKR",
                "severity": "MEDIUM",
                "order_id": order_id,
                "db_status": db_order.get("status"),
                "ibkr_status": None,
                "details": "Local open_orders table has an active order missing from IBKR/TWS open orders.",
            })

        elif ibkr_order and not db_order:
            issues.append({
                "symbol": ibkr_order.get("symbol"),
                "issue_type": "IBKR_OPEN_ORDER_NOT_IN_DB",
                "severity": "MEDIUM",
                "order_id": order_id,
                "db_status": None,
                "ibkr_status": ibkr_order.get("status"),
                "details": "IBKR/TWS has an active order missing from local open_orders table.",
            })

    for issue in issues:
        log.warning("RECONCILIATION MISMATCH | %s", issue)
        await _journal_reconciliation_issue(issue)

    return {
        "ok": len(issues) == 0,
        "issues_count": len(issues),
        "issues": issues,
        "checked_at": now_iso(),
    }

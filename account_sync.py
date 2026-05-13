from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiosqlite

import config
import database
import execution_sync
from circuit_breaker import record_ibkr_error

log = logging.getLogger(__name__)

ACCOUNT_SYNC_CLIENT_ID_OFFSET = 600
RECONCILIATION_STATUS_KEY = "account_sync_reconciliation_status"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def safe_str(value) -> str:
    try:
        if value is None:
            return ""
        return str(value)
    except Exception:
        return ""


CREATE_ACCOUNT_SUMMARY = """
CREATE TABLE IF NOT EXISTS account_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag TEXT NOT NULL,
    value TEXT,
    currency TEXT,
    account TEXT,
    updated_at TEXT
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
    raw_json TEXT,
    updated_at TEXT
)
"""

CREATE_EXECUTION_HISTORY = """
CREATE TABLE IF NOT EXISTS execution_history (
    exec_id TEXT PRIMARY KEY,
    symbol TEXT,
    side TEXT,
    quantity REAL,
    price REAL,
    order_id INTEGER,
    perm_id INTEGER,
    account TEXT,
    exchange TEXT,
    time TEXT,
    commission REAL DEFAULT 0,
    realized_pnl REAL DEFAULT 0,
    raw_json TEXT,
    created_at TEXT
)
"""

CREATE_EQUITY_CURVE = """
CREATE TABLE IF NOT EXISTS equity_curve (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    account TEXT,
    net_liquidation REAL,
    total_cash REAL,
    buying_power REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    source TEXT DEFAULT 'account_sync'
)
"""


def _json_payload(data) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"repr": repr(data)}, ensure_ascii=False)


async def init_account_sync_db() -> None:
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(CREATE_ACCOUNT_SUMMARY)
        await db.execute(CREATE_OPEN_ORDERS)
        await db.execute(CREATE_EXECUTION_HISTORY)
        await db.execute(CREATE_EQUITY_CURVE)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_account_summary_tag ON account_summary(tag)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_open_orders_symbol ON open_orders(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_execution_history_symbol ON execution_history(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_equity_curve_timestamp ON equity_curve(timestamp)")
        await db.commit()


def _account_value(summary: list[dict], tag: str) -> float:
    for row in summary:
        if row.get("tag") == tag:
            return safe_float(row.get("value"))
    return 0.0


def fetch_account_snapshot_sync() -> dict:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    from ib_insync import IB

    ib = IB()

    try:
        client_id = int(config.IBKR_CLIENT_ID) + ACCOUNT_SYNC_CLIENT_ID_OFFSET

        ib.connect(
            config.IBKR_HOST,
            int(config.IBKR_PORT),
            clientId=client_id,
            timeout=10,
            readonly=True,
        )

        account = None
        accounts = ib.managedAccounts()
        if accounts:
            account = accounts[0]

        summary_rows = ib.accountSummary()

        ib.reqAllOpenOrders()
        ib.sleep(1)
        open_trades = ib.openTrades()

        executions = ib.executions()
        synced_at = now_iso()

        account_summary = []
        seen_summary = set()

        for row in summary_rows:
            item = {
                "tag": safe_str(row.tag),
                "value": safe_str(row.value),
                "currency": safe_str(row.currency),
                "account": safe_str(row.account or account),
            }
            key = (item["tag"], item["currency"], item["account"])
            if key in seen_summary:
                continue
            seen_summary.add(key)
            account_summary.append(item)

        open_orders = []
        seen_orders = set()

        for trade in open_trades:
            contract = trade.contract
            order = trade.order
            status = trade.orderStatus
            order_id = safe_int(order.orderId)
            perm_id = safe_int(order.permId)
            symbol = safe_str(contract.symbol).upper()
            key = (order_id, perm_id, symbol)
            if key in seen_orders:
                continue
            seen_orders.add(key)
            open_orders.append({
                "order_id": order_id,
                "perm_id": perm_id,
                "symbol": symbol,
                "action": safe_str(order.action).upper(),
                "order_type": safe_str(order.orderType),
                "total_quantity": safe_float(order.totalQuantity),
                "limit_price": safe_float(order.lmtPrice),
                "aux_price": safe_float(order.auxPrice),
                "status": safe_str(status.status),
                "filled": safe_float(status.filled),
                "remaining": safe_float(status.remaining),
                "avg_fill_price": safe_float(status.avgFillPrice),
                "account": safe_str(order.account or account),
                "raw_json": _json_payload({
                    "contract": getattr(contract, "__dict__", {}),
                    "order": getattr(order, "__dict__", {}),
                    "status": getattr(status, "__dict__", {}),
                }),
            })

        execution_history = execution_sync.normalize_execution_items(executions)
        for row in execution_history:
            if not row.get("account"):
                row["account"] = safe_str(account)

        return {
            "connected": True,
            "account": account,
            "account_summary": account_summary,
            "open_orders": open_orders,
            "execution_history": execution_history,
            "equity": {
                "timestamp": synced_at,
                "account": account,
                "net_liquidation": _account_value(account_summary, "NetLiquidation"),
                "total_cash": _account_value(account_summary, "TotalCashValue"),
                "buying_power": _account_value(account_summary, "BuyingPower"),
                "unrealized_pnl": _account_value(account_summary, "UnrealizedPnL"),
                "realized_pnl": _account_value(account_summary, "RealizedPnL"),
            },
            "error": None,
            "synced_at": synced_at,
        }

    finally:
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
    synced_at = snapshot.get("synced_at") or now_iso()

    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute("DELETE FROM account_summary")
        await db.execute("DELETE FROM open_orders")

        for row in snapshot.get("account_summary", []):
            await db.execute(
                """
                INSERT INTO account_summary (tag, value, currency, account, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (row.get("tag"), row.get("value"), row.get("currency"), row.get("account"), synced_at),
            )

        for row in snapshot.get("open_orders", []):
            await db.execute(
                """
                INSERT OR REPLACE INTO open_orders (
                    order_id, perm_id, symbol, action, order_type, total_quantity,
                    limit_price, aux_price, status, filled, remaining, avg_fill_price,
                    account, raw_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("order_id"), row.get("perm_id"), row.get("symbol"), row.get("action"),
                    row.get("order_type"), row.get("total_quantity"), row.get("limit_price"),
                    row.get("aux_price"), row.get("status"), row.get("filled"), row.get("remaining"),
                    row.get("avg_fill_price"), row.get("account"), row.get("raw_json"), synced_at,
                ),
            )

        for row in snapshot.get("execution_history", []):
            await db.execute(
                """
                INSERT OR IGNORE INTO execution_history (
                    exec_id, symbol, side, quantity, price, order_id, perm_id,
                    account, exchange, time, commission, realized_pnl, raw_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("exec_id"), row.get("symbol"), row.get("side"), row.get("quantity"),
                    row.get("price"), row.get("order_id"), row.get("perm_id"), row.get("account"),
                    row.get("exchange"), row.get("time"), row.get("commission"), row.get("realized_pnl"),
                    row.get("raw_json"), synced_at,
                ),
            )

        equity = snapshot.get("equity") or {}
        await db.execute(
            """
            INSERT INTO equity_curve (
                timestamp, account, net_liquidation, total_cash, buying_power,
                unrealized_pnl, realized_pnl, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'account_sync')
            """,
            (
                equity.get("timestamp") or synced_at,
                equity.get("account") or snapshot.get("account"),
                equity.get("net_liquidation"), equity.get("total_cash"), equity.get("buying_power"),
                equity.get("unrealized_pnl"), equity.get("realized_pnl"),
            ),
        )

        await db.commit()


async def run_account_sync_once() -> dict:
    await init_account_sync_db()

    try:
        snapshot = await asyncio.to_thread(fetch_account_snapshot_sync)
    except Exception as exc:
        log.warning("Account sync failed: %s", exc)
        try:
            await record_ibkr_error(str(exc), source="account_sync.run_account_sync_once")
        except Exception:
            pass
        snapshot = {
            "connected": False,
            "account": None,
            "account_summary": [],
            "open_orders": [],
            "execution_history": [],
            "equity": {"timestamp": now_iso()},
            "error": str(exc),
            "synced_at": now_iso(),
        }

    await save_account_snapshot(snapshot)
    return snapshot


async def _fetch_rows(sql: str, params: tuple = ()) -> list[dict]:
    await init_account_sync_db()
    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def get_account_summary() -> list[dict]:
    return await _fetch_rows(
        """
        SELECT tag, value, currency, account, updated_at
        FROM account_summary
        ORDER BY tag, currency, account
        """
    )


async def get_open_orders() -> list[dict]:
    return await _fetch_rows(
        """
        SELECT order_id, perm_id, symbol, action, order_type, total_quantity,
               limit_price, aux_price, status, filled, remaining, avg_fill_price,
               account, updated_at
        FROM open_orders
        ORDER BY updated_at DESC, order_id DESC
        """
    )


async def get_executions(limit: int = 200, symbol: str | None = None) -> list[dict]:
    limit = max(1, min(int(limit or 200), 1000))
    if symbol:
        return await _fetch_rows(
            """
            SELECT exec_id, symbol, side, quantity, price, order_id, perm_id,
                   account, exchange, time, commission, realized_pnl, created_at
            FROM execution_history
            WHERE symbol = ?
            ORDER BY COALESCE(time, created_at) DESC
            LIMIT ?
            """,
            (symbol.strip().upper(), limit),
        )
    return await _fetch_rows(
        """
        SELECT exec_id, symbol, side, quantity, price, order_id, perm_id,
               account, exchange, time, commission, realized_pnl, created_at
        FROM execution_history
        ORDER BY COALESCE(time, created_at) DESC
        LIMIT ?
        """,
        (limit,),
    )


async def get_equity_curve(limit: int = 500, session_only: bool = True) -> list[dict]:
    limit = max(1, min(int(limit or 500), 5000))

    if not session_only:
        return await _fetch_rows(
            """
            SELECT timestamp, account, net_liquidation, total_cash, buying_power,
                   unrealized_pnl, realized_pnl, source
            FROM equity_curve
            ORDER BY timestamp ASC, id ASC
            LIMIT ?
            """,
            (limit,),
        )

    session = await database.get_active_paper_session()
    baseline_id = safe_int((session or {}).get("equity_curve_start_id"), 0)
    baseline_timestamp = (session or {}).get("equity_curve_start_timestamp")
    session_start_equity = safe_float(
        (session or {}).get("session_start_equity"),
        safe_float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0), 5000.0),
    )
    realized_baseline = safe_float((session or {}).get("realized_pnl_baseline"))

    rows = await _fetch_rows(
        """
        SELECT id, timestamp, account, net_liquidation, total_cash, buying_power,
               unrealized_pnl, realized_pnl, source
        FROM equity_curve
        WHERE id > ?
        ORDER BY timestamp ASC, id ASC
        LIMIT ?
        """,
        (baseline_id, max(1, limit - 1)),
    )

    curve = [
        {
            "timestamp": baseline_timestamp or (session or {}).get("started_at"),
            "account": None,
            "net_liquidation": round(session_start_equity, 2),
            "total_cash": None,
            "buying_power": None,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "source": "paper_session_baseline",
            "session_id": (session or {}).get("session_id"),
            "session_start_equity": round(session_start_equity, 2),
        }
    ]

    for row in rows:
        row.pop("id", None)
        if row.get("realized_pnl") is not None:
            row["realized_pnl"] = round(safe_float(row.get("realized_pnl")) - realized_baseline, 2)
        row["session_id"] = (session or {}).get("session_id")
        curve.append(row)

    return curve[:limit]


async def _load_open_position_quantities(db: aiosqlite.Connection) -> dict[str, float]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        """
        SELECT symbol, quantity
        FROM positions
        WHERE status = 'OPEN'
        """
    ) as cursor:
        rows = await cursor.fetchall()
    return {str(row["symbol"]).upper(): safe_float(row["quantity"]) for row in rows if row["symbol"]}


async def _load_execution_quantities(db: aiosqlite.Connection) -> dict[str, float]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        """
        SELECT symbol, side, SUM(quantity) AS quantity
        FROM execution_history
        GROUP BY symbol, side
        """
    ) as cursor:
        rows = await cursor.fetchall()

    result: dict[str, float] = {}
    for row in rows:
        symbol = str(row["symbol"] or "").upper().strip()
        side = str(row["side"] or "").upper().strip()
        quantity = safe_float(row["quantity"])
        if not symbol:
            continue
        result.setdefault(symbol, 0.0)
        if side in {"BOT", "BUY"}:
            result[symbol] += quantity
        elif side in {"SLD", "SELL"}:
            result[symbol] -= quantity
    return result


async def run_reconciliation_status_check() -> dict:
    await init_account_sync_db()

    async with aiosqlite.connect(config.DB_PATH) as db:
        db_positions = await _load_open_position_quantities(db)
        execution_quantities = await _load_execution_quantities(db)

    symbols = sorted(set(db_positions) | set(execution_quantities))
    mismatches = []

    for symbol in symbols:
        db_qty = safe_float(db_positions.get(symbol))
        execution_qty = safe_float(execution_quantities.get(symbol))
        if abs(db_qty - execution_qty) <= 0.0001:
            continue
        mismatch = {
            "symbol": symbol,
            "issue_type": "POSITION_EXECUTION_QUANTITY_MISMATCH",
            "severity": "MEDIUM",
            "db_quantity": db_qty,
            "execution_quantity": execution_qty,
            "details": "Open position quantity does not match net execution history quantity.",
        }
        mismatches.append(mismatch)
        await database.safe_record_trade_journal_event({
            "symbol": symbol,
            "event_type": "RECONCILIATION_MISMATCH",
            "decision": "REVIEW_REQUIRED",
            "reason": mismatch["details"],
            "source_module": "account_sync.run_reconciliation_status_check",
            "quantity": db_qty,
            "raw_payload": mismatch,
        })

    status = {
        "ok": len(mismatches) == 0,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "checked_at": now_iso(),
    }

    await database.set_app_state(RECONCILIATION_STATUS_KEY, _json_payload(status))
    return status


async def get_reconciliation_status() -> dict:
    raw = await database.get_app_state(RECONCILIATION_STATUS_KEY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return await run_reconciliation_status_check()

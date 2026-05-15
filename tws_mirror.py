from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiosqlite
from ib_insync import IB

import config
import database
from circuit_breaker import record_ibkr_error, reset_ibkr_error_count

log = logging.getLogger(__name__)

TWS_MIRROR_CLIENT_ID_OFFSET = 400


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


CREATE_TWS_HEARTBEAT = """
CREATE TABLE IF NOT EXISTS tws_heartbeat (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    connected INTEGER DEFAULT 0,
    account TEXT,
    last_sync_at TEXT,
    error TEXT
)
"""

CREATE_TWS_ACCOUNT = """
CREATE TABLE IF NOT EXISTS tws_account (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag TEXT,
    value TEXT,
    currency TEXT,
    account TEXT,
    updated_at TEXT
)
"""

CREATE_TWS_POSITIONS = """
CREATE TABLE IF NOT EXISTS tws_positions (
    symbol TEXT PRIMARY KEY,
    quantity REAL,
    avg_cost REAL,
    market_price REAL,
    market_value REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    account TEXT,
    updated_at TEXT
)
"""

CREATE_TWS_ORDERS = """
CREATE TABLE IF NOT EXISTS tws_orders (
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


async def init_tws_mirror_db() -> None:
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(CREATE_TWS_HEARTBEAT)
        await db.execute(CREATE_TWS_ACCOUNT)
        await db.execute(CREATE_TWS_POSITIONS)
        await db.execute(CREATE_TWS_ORDERS)
        await db.commit()


def fetch_tws_snapshot_sync() -> dict:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ib = IB()

    try:
        client_id = int(config.IBKR_CLIENT_ID) + TWS_MIRROR_CLIENT_ID_OFFSET

        ib.connect(
            config.IBKR_HOST,
            int(config.IBKR_PORT),
            clientId=client_id,
            timeout=10,
        )

        account = None

        accounts = ib.managedAccounts()
        if accounts:
            account = accounts[0]

        summary_rows = ib.accountSummary()
        positions_rows = ib.positions()
        portfolio_rows = ib.portfolio()

        ib.reqAllOpenOrders()
        ib.sleep(1)

        open_trades = ib.openTrades()

        account_summary = []
        seen_account_rows = set()

        for row in summary_rows:
            item = {
                "tag": safe_str(row.tag),
                "value": safe_str(row.value),
                "currency": safe_str(row.currency),
                "account": safe_str(row.account),
            }

            unique_key = (
                item["tag"],
                item["value"],
                item["currency"],
                item["account"],
            )

            if unique_key in seen_account_rows:
                continue

            seen_account_rows.add(unique_key)
            account_summary.append(item)

        portfolio_by_symbol = {}

        for item in portfolio_rows:
            contract = item.contract
            symbol = safe_str(contract.symbol).upper()

            if not symbol:
                continue

            portfolio_by_symbol[symbol] = {
                "market_price": safe_float(item.marketPrice),
                "market_value": safe_float(item.marketValue),
                "unrealized_pnl": safe_float(item.unrealizedPNL),
                "realized_pnl": safe_float(item.realizedPNL),
            }

        positions = []
        seen_positions = set()

        for pos in positions_rows:
            contract = pos.contract
            symbol = safe_str(contract.symbol).upper()

            if not symbol:
                continue

            if symbol in seen_positions:
                continue

            seen_positions.add(symbol)

            portfolio_data = portfolio_by_symbol.get(symbol, {})

            positions.append(
                {
                    "symbol": symbol,
                    "quantity": safe_float(pos.position),
                    "avg_cost": safe_float(pos.avgCost),
                    "market_price": safe_float(portfolio_data.get("market_price")),
                    "market_value": safe_float(portfolio_data.get("market_value")),
                    "unrealized_pnl": safe_float(portfolio_data.get("unrealized_pnl")),
                    "realized_pnl": safe_float(portfolio_data.get("realized_pnl")),
                    "account": safe_str(pos.account),
                }
            )

        orders = []
        seen_orders = set()

        for trade in open_trades:
            contract = trade.contract
            order = trade.order
            status = trade.orderStatus

            order_id = safe_int(order.orderId)
            perm_id = safe_int(order.permId)

            unique_key = (
                order_id,
                perm_id,
                safe_str(contract.symbol).upper(),
            )

            if unique_key in seen_orders:
                continue

            seen_orders.add(unique_key)

            orders.append(
                {
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "symbol": safe_str(contract.symbol).upper(),
                    "action": safe_str(order.action).upper(),
                    "order_type": safe_str(order.orderType),
                    "total_quantity": safe_float(order.totalQuantity),
                    "limit_price": safe_float(order.lmtPrice),
                    "aux_price": safe_float(order.auxPrice),
                    "status": safe_str(status.status),
                    "filled": safe_float(status.filled),
                    "remaining": safe_float(status.remaining),
                    "avg_fill_price": safe_float(status.avgFillPrice),
                    "account": safe_str(order.account),
                    "raw_json": json.dumps(
                        {
                            "contract": {
                                "symbol": safe_str(contract.symbol),
                                "exchange": safe_str(contract.exchange),
                                "currency": safe_str(contract.currency),
                                "localSymbol": safe_str(contract.localSymbol),
                            },
                            "order": {
                                "orderId": order.orderId,
                                "permId": order.permId,
                                "action": order.action,
                                "orderType": order.orderType,
                                "totalQuantity": order.totalQuantity,
                                "lmtPrice": order.lmtPrice,
                                "auxPrice": order.auxPrice,
                                "tif": order.tif,
                            },
                            "status": {
                                "status": status.status,
                                "filled": status.filled,
                                "remaining": status.remaining,
                                "avgFillPrice": status.avgFillPrice,
                            },
                        },
                        ensure_ascii=False,
                    ),
                }
            )

        return {
            "connected": True,
            "account": account,
            "account_summary": account_summary,
            "positions": positions,
            "orders": orders,
            "error": None,
            "synced_at": now_iso(),
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


async def save_tws_snapshot(snapshot: dict) -> None:
    synced_at = snapshot.get("synced_at") or now_iso()

    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO tws_heartbeat (
                id, connected, account, last_sync_at, error
            )
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                connected = excluded.connected,
                account = excluded.account,
                last_sync_at = excluded.last_sync_at,
                error = excluded.error
            """,
            (
                1 if snapshot.get("connected") else 0,
                snapshot.get("account"),
                synced_at,
                snapshot.get("error"),
            ),
        )

        await db.execute("DELETE FROM tws_account")
        await db.execute("DELETE FROM tws_positions")
        await db.execute("DELETE FROM tws_orders")

        seen_account_rows = set()

        for row in snapshot.get("account_summary", []):
            unique_key = (
                row.get("tag"),
                row.get("value"),
                row.get("currency"),
                row.get("account"),
            )

            if unique_key in seen_account_rows:
                continue

            seen_account_rows.add(unique_key)

            await db.execute(
                """
                INSERT INTO tws_account (
                    tag, value, currency, account, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    row.get("tag"),
                    row.get("value"),
                    row.get("currency"),
                    row.get("account"),
                    synced_at,
                ),
            )

        for row in snapshot.get("positions", []):
            await db.execute(
                """
                INSERT INTO tws_positions (
                    symbol, quantity, avg_cost,
                    market_price, market_value,
                    unrealized_pnl, realized_pnl,
                    account, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("symbol"),
                    row.get("quantity"),
                    row.get("avg_cost"),
                    row.get("market_price"),
                    row.get("market_value"),
                    row.get("unrealized_pnl"),
                    row.get("realized_pnl"),
                    row.get("account"),
                    synced_at,
                ),
            )

        for row in snapshot.get("orders", []):
            await db.execute(
                """
                INSERT OR REPLACE INTO tws_orders (
                    order_id, perm_id, symbol, action,
                    order_type, total_quantity, limit_price,
                    aux_price, status, filled, remaining,
                    avg_fill_price, account, raw_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("order_id"),
                    row.get("perm_id"),
                    row.get("symbol"),
                    row.get("action"),
                    row.get("order_type"),
                    row.get("total_quantity"),
                    row.get("limit_price"),
                    row.get("aux_price"),
                    row.get("status"),
                    row.get("filled"),
                    row.get("remaining"),
                    row.get("avg_fill_price"),
                    row.get("account"),
                    row.get("raw_json"),
                    synced_at,
                ),
            )

        await db.commit()


async def run_tws_mirror_once() -> dict:
    await init_tws_mirror_db()

    try:
        snapshot = await asyncio.to_thread(fetch_tws_snapshot_sync)

    except Exception as e:
        log.warning("TWS MIRROR FAILED: %s", e)
        try:
            await record_ibkr_error(str(e), source="tws_mirror.run_tws_mirror_once")
        except Exception:
            pass

        snapshot = {
            "connected": False,
            "account": None,
            "account_summary": [],
            "positions": [],
            "orders": [],
            "error": str(e),
            "synced_at": now_iso(),
        }

    await save_tws_snapshot(snapshot)

    if snapshot.get("connected"):
        await database.set_app_state("tws_mirror_last_success_at", snapshot.get("synced_at") or now_iso())
        await reset_ibkr_error_count()

    log.info(
        "TWS MIRROR SYNC | connected=%s | account=%s | positions=%s | orders=%s",
        snapshot.get("connected"),
        snapshot.get("account"),
        len(snapshot.get("positions", [])),
        len(snapshot.get("orders", [])),
    )

    return snapshot
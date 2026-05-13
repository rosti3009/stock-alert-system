from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiosqlite
from ib_insync import IB

import config
import database
import order_lifecycle
from circuit_breaker import record_ibkr_error

log = logging.getLogger(__name__)

EXECUTION_CLIENT_ID_OFFSET = 500


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def safe_str(v):
    try:
        return str(v or "")
    except Exception:
        return ""


def fetch_executions_sync():

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ib = IB()

    try:
        client_id = (
            int(config.IBKR_CLIENT_ID)
            + EXECUTION_CLIENT_ID_OFFSET
        )

        ib.connect(
            config.IBKR_HOST,
            int(config.IBKR_PORT),
            clientId=client_id,
            timeout=10,
        )

        ib.sleep(1)

        fills = ib.fills()
        executions = fills or ib.executions()

        rows = []

        for item in executions:

            exec_data = item.execution
            contract = item.contract
            commission_report = getattr(item, "commissionReport", None)

            rows.append(
                {
                    "exec_id": safe_str(exec_data.execId),
                    "symbol": safe_str(contract.symbol).upper(),
                    "side": safe_str(exec_data.side).upper(),
                    "quantity": safe_float(exec_data.shares),
                    "price": safe_float(exec_data.price),
                    "order_id": safe_int(exec_data.orderId),
                    "perm_id": safe_int(exec_data.permId),
                    "account": safe_str(exec_data.acctNumber),
                    "exchange": safe_str(exec_data.exchange),
                    "time": safe_str(exec_data.time),
                    "commission": safe_float(getattr(commission_report, "commission", 0.0)),
                    "realized_pnl": safe_float(getattr(commission_report, "realizedPNL", 0.0)),
                    "raw_json": json.dumps(
                        {
                            "execution": exec_data.__dict__,
                            "contract": contract.__dict__,
                            "commission_report": getattr(commission_report, "__dict__", {}),
                        },
                        default=str,
                        ensure_ascii=False,
                    ),
                }
            )

        return rows

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


async def sync_executions():

    try:

        rows = await asyncio.to_thread(
            fetch_executions_sync
        )

        async with aiosqlite.connect(
            config.DB_PATH
        ) as db:

            inserted = 0
            lifecycle_rows = []

            for row in rows:

                cursor = await db.execute(
                    """
                    INSERT OR IGNORE INTO executions (
                        exec_id,
                        symbol,
                        side,
                        quantity,
                        price,
                        order_id,
                        perm_id,
                        account,
                        exchange,
                        time,
                        commission,
                        realized_pnl,
                        raw_json,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["exec_id"],
                        row["symbol"],
                        row["side"],
                        row["quantity"],
                        row["price"],
                        row["order_id"],
                        row["perm_id"],
                        row["account"],
                        row["exchange"],
                        row["time"],
                        row["commission"],
                        row["realized_pnl"],
                        row["raw_json"],
                        now_iso(),
                    ),
                )

                if cursor.rowcount > 0:
                    inserted += 1
                    lifecycle_rows.append(row)

            await db.commit()

        for row in lifecycle_rows:
            await order_lifecycle.safe_record_order_lifecycle_event({
                "symbol": row["symbol"],
                "side": row["side"],
                "quantity": row["quantity"],
                "price": row["price"],
                "order_id": row["order_id"],
                "perm_id": row["perm_id"],
                "client_id": int(config.IBKR_CLIENT_ID) + EXECUTION_CLIENT_ID_OFFSET,
                "source_module": "execution_sync.sync_executions",
                "state": order_lifecycle.OrderState.FILLED,
                "reason": f"Execution sync observed fill exec_id={row['exec_id']}",
                "raw_payload": row,
            })

        result = {
            "ok": True,
            "fetched_count": len(rows),
            "inserted_count": inserted,
            "duplicate_count": max(0, len(rows) - inserted),
            "synced_at": now_iso(),
        }

        log.info(
            "Execution sync complete | fetched=%s inserted=%s duplicates=%s",
            result["fetched_count"],
            result["inserted_count"],
            result["duplicate_count"],
        )
        return result

    except Exception as e:

        log.warning(
            "Execution sync failed: %s",
            e,
        )
        try:
            await record_ibkr_error(str(e), source="execution_sync.sync_executions")
        except Exception:
            pass
        return {
            "ok": False,
            "error": str(e),
            "fetched_count": 0,
            "inserted_count": 0,
            "duplicate_count": 0,
            "synced_at": now_iso(),
        }

async def get_executions(limit: int = 200, symbol: str | None = None) -> list[dict]:
    limit = max(1, min(int(limit or 200), 1000))
    await database.init_db()
    params: tuple
    if symbol:
        where = "WHERE symbol = ?"
        params = (symbol.strip().upper(), limit)
    else:
        where = ""
        params = (limit,)

    async with aiosqlite.connect(config.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"""
            SELECT exec_id, symbol, side, quantity, price, order_id, perm_id,
                   account, exchange, time, commission, realized_pnl, created_at
            FROM executions
            {where}
            ORDER BY COALESCE(time, created_at) DESC
            LIMIT ?
            """,
            params,
        ) as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]

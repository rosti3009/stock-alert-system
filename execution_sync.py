from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiosqlite
from ib_insync import IB

import config
import order_lifecycle

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

        executions = ib.executions()

        rows = []

        for item in executions:

            exec_data = item.execution
            contract = item.contract

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
                    "commission": 0.0,
                    "realized_pnl": 0.0,
                    "raw_json": json.dumps(
                        {
                            "execution": exec_data.__dict__,
                            "contract": contract.__dict__,
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

        log.info(
            "Execution sync complete | executions=%s",
            inserted,
        )

    except Exception as e:

        log.warning(
            "Execution sync failed: %s",
            e,
        )
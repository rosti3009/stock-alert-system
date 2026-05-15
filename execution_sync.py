from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiosqlite
from ibkr_asyncio_compat import ensure_event_loop

ensure_event_loop()

from ib_insync import IB

import config
import database
import order_lifecycle
from circuit_breaker import record_ibkr_error, reset_ibkr_error_count

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


def safe_optional_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _get_value(obj, name, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _raw_payload(obj):
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    return getattr(obj, "__dict__", safe_str(obj))


def _looks_like_execution(obj) -> bool:
    return bool(_get_value(obj, "execId"))


def _looks_like_commission_report(obj) -> bool:
    if obj is None:
        return False
    return (
        _get_value(obj, "commission") is not None
        or _get_value(obj, "realizedPNL") is not None
        or _get_value(obj, "realized_pnl") is not None
    )


def _looks_like_contract(obj) -> bool:
    return bool(_get_value(obj, "symbol")) and not _looks_like_execution(obj)


def _unpack_execution_item(item):
    execution = _get_value(item, "execution")
    contract = _get_value(item, "contract")
    commission_report = _get_value(item, "commissionReport")

    if execution is not None:
        return execution, contract, commission_report

    if _looks_like_execution(item):
        return item, contract, commission_report

    if isinstance(item, (tuple, list)):
        for part in item:
            if execution is None and _looks_like_execution(part):
                execution = part
            elif commission_report is None and _looks_like_commission_report(part):
                commission_report = part
            elif contract is None and _looks_like_contract(part):
                contract = part
        if execution is not None:
            return execution, contract, commission_report

    return None, contract, commission_report


def normalize_execution_items(items) -> list[dict]:
    rows = []
    for index, item in enumerate(items or []):
        try:
            rows.append(normalize_execution_item(item))
        except Exception as e:
            log.warning(
                "Skipping malformed IBKR execution row at index %s: %s | row=%s",
                index,
                e,
                safe_str(item),
            )
    return rows


def normalize_execution_item(item) -> dict:
    exec_data, contract, commission_report = _unpack_execution_item(item)
    if exec_data is None:
        raise ValueError("missing execution data")

    exec_id = safe_str(_get_value(exec_data, "execId"))
    if not exec_id:
        raise ValueError("missing execution execId")

    commission = safe_optional_float(_get_value(commission_report, "commission"))
    realized_pnl = safe_optional_float(
        _get_value(commission_report, "realizedPNL", _get_value(commission_report, "realized_pnl"))
    )

    return {
        "exec_id": exec_id,
        "symbol": safe_str(_get_value(contract, "symbol")).upper(),
        "side": safe_str(_get_value(exec_data, "side")).upper(),
        "quantity": safe_float(_get_value(exec_data, "shares")),
        "price": safe_float(_get_value(exec_data, "price")),
        "order_id": safe_int(_get_value(exec_data, "orderId")),
        "perm_id": safe_int(_get_value(exec_data, "permId")),
        "account": safe_str(_get_value(exec_data, "acctNumber")),
        "exchange": safe_str(_get_value(exec_data, "exchange")),
        "time": safe_str(_get_value(exec_data, "time")),
        "commission": commission,
        "realized_pnl": realized_pnl,
        "raw_json": json.dumps(
            {
                "execution": _raw_payload(exec_data),
                "contract": _raw_payload(contract),
                "commission_report": _raw_payload(commission_report),
            },
            default=str,
            ensure_ascii=False,
        ),
    }


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

        return normalize_execution_items(executions)

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
        await database.set_app_state("execution_sync_last_success_at", result["synced_at"])
        await reset_ibkr_error_count()

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

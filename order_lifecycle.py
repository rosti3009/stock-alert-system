from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

import aiosqlite

import config
import database

log = logging.getLogger(__name__)


class OrderState(StrEnum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


TERMINAL_STATES = {
    OrderState.FILLED,
    OrderState.CANCELLED,
    OrderState.REJECTED,
    OrderState.EXPIRED,
    OrderState.FAILED,
}

JOURNAL_EVENT_BY_STATE = {
    OrderState.SUBMITTED: "ORDER_SUBMITTED",
    OrderState.ACKNOWLEDGED: "ORDER_ACKNOWLEDGED",
    OrderState.PARTIALLY_FILLED: "ORDER_PARTIALLY_FILLED",
    OrderState.FILLED: "ORDER_FILLED",
    OrderState.CANCELLED: "ORDER_CANCELLED",
    OrderState.REJECTED: "ORDER_REJECTED",
    OrderState.FAILED: "ORDER_FAILED",
}

CREATE_ORDER_LIFECYCLE_EVENTS = """
CREATE TABLE IF NOT EXISTS order_lifecycle_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT,
    side TEXT,
    quantity REAL,
    price REAL,
    order_id INTEGER,
    perm_id INTEGER,
    client_id INTEGER,
    source_module TEXT,
    state TEXT NOT NULL,
    previous_state TEXT,
    reason TEXT,
    raw_json TEXT
)
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_state(state: str | OrderState | None) -> OrderState:
    try:
        return OrderState(str(state or OrderState.UNKNOWN).upper())
    except ValueError:
        return OrderState.UNKNOWN


def map_ibkr_status_to_state(status: str | None, filled: Any = None, remaining: Any = None) -> OrderState:
    normalized = str(status or "").strip().lower()
    filled_qty = _safe_float(filled)
    remaining_qty = _safe_float(remaining)

    if normalized in {"pendingcancel"}:
        return OrderState.CANCEL_REQUESTED
    if normalized in {"cancelled", "apicancelled"}:
        return OrderState.CANCELLED
    if normalized in {"filled"}:
        return OrderState.FILLED
    if normalized in {"inactive"}:
        return OrderState.REJECTED
    if normalized in {"expired"}:
        return OrderState.EXPIRED
    if normalized in {"submitted", "presubmitted"}:
        if filled_qty > 0 and remaining_qty > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.ACKNOWLEDGED
    if normalized in {"pendingsubmit"}:
        return OrderState.SUBMITTED
    if filled_qty > 0 and remaining_qty > 0:
        return OrderState.PARTIALLY_FILLED
    if filled_qty > 0 and remaining_qty <= 0:
        return OrderState.FILLED
    return OrderState.UNKNOWN


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _json_payload(payload: Any) -> str:
    if payload is None:
        return "{}"
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"repr": repr(payload)}, ensure_ascii=False)


def _db_path() -> str:
    return getattr(config, "DB_PATH", database.DB_PATH)


async def init_order_lifecycle_db() -> None:
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(CREATE_ORDER_LIFECYCLE_EVENTS)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_timestamp ON order_lifecycle_events(timestamp)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_symbol ON order_lifecycle_events(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_order_id ON order_lifecycle_events(order_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_perm_id ON order_lifecycle_events(perm_id)")
        await db.commit()


def init_order_lifecycle_db_sync() -> None:
    with closing(sqlite3.connect(_db_path())) as db:
        db.execute(CREATE_ORDER_LIFECYCLE_EVENTS)
        db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_timestamp ON order_lifecycle_events(timestamp)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_symbol ON order_lifecycle_events(symbol)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_order_id ON order_lifecycle_events(order_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_perm_id ON order_lifecycle_events(perm_id)")
        db.commit()


def _base_row(event: dict[str, Any], previous_state: str | None = None) -> dict[str, Any]:
    symbol = str(event.get("symbol") or "").strip().upper() or None
    side = str(event.get("side") or event.get("action") or "").strip().upper() or None
    state = normalize_state(event.get("state"))
    return {
        "timestamp": event.get("timestamp") or now_iso(),
        "symbol": symbol,
        "side": side,
        "quantity": event.get("quantity"),
        "price": event.get("price", event.get("limit_price")),
        "order_id": _safe_int(event.get("order_id")),
        "perm_id": _safe_int(event.get("perm_id")),
        "client_id": _safe_int(event.get("client_id")),
        "source_module": event.get("source_module"),
        "state": state.value,
        "previous_state": event.get("previous_state", previous_state),
        "reason": event.get("reason"),
        "raw_json": _json_payload(event.get("raw_json", event.get("raw_payload", event))),
    }


def _previous_state_queries(row: dict[str, Any]) -> list[tuple[str, tuple[Any, ...]]]:
    if normalize_state(row.get("state")) == OrderState.CREATED:
        return []

    queries: list[tuple[str, tuple[Any, ...]]] = []

    if row.get("perm_id"):
        queries.append((
            "SELECT state FROM order_lifecycle_events WHERE perm_id = ? ORDER BY timestamp DESC, id DESC LIMIT 1",
            (row["perm_id"],),
        ))

    if row.get("order_id"):
        queries.append((
            "SELECT state FROM order_lifecycle_events WHERE order_id = ? ORDER BY timestamp DESC, id DESC LIMIT 1",
            (row["order_id"],),
        ))

    queries.append((
        """
        SELECT state FROM order_lifecycle_events
        WHERE symbol IS ? AND side IS ? AND client_id IS ? AND order_id IS NULL AND perm_id IS NULL
        ORDER BY timestamp DESC, id DESC LIMIT 1
        """,
        (row.get("symbol"), row.get("side"), row.get("client_id")),
    ))

    return queries


def _insert_sql() -> str:
    return """
    INSERT INTO order_lifecycle_events (
        timestamp, symbol, side, quantity, price, order_id, perm_id, client_id,
        source_module, state, previous_state, reason, raw_json
    ) VALUES (
        :timestamp, :symbol, :side, :quantity, :price, :order_id, :perm_id, :client_id,
        :source_module, :state, :previous_state, :reason, :raw_json
    )
    """


async def record_order_lifecycle_event(event: dict[str, Any]) -> dict[str, Any]:
    await init_order_lifecycle_db()
    row = _base_row(event)

    async with aiosqlite.connect(_db_path()) as db:
        if not row.get("previous_state"):
            for sql, params in _previous_state_queries(row):
                async with db.execute(sql, params) as cursor:
                    previous = await cursor.fetchone()
                if previous:
                    row["previous_state"] = previous[0]
                    break

        await db.execute(_insert_sql(), row)
        await db.commit()

    await journal_lifecycle_transition(row)
    return row


def record_order_lifecycle_event_sync(event: dict[str, Any]) -> dict[str, Any]:
    init_order_lifecycle_db_sync()
    row = _base_row(event)

    with closing(sqlite3.connect(_db_path())) as db:
        if not row.get("previous_state"):
            for sql, params in _previous_state_queries(row):
                previous = db.execute(sql, params).fetchone()
                if previous:
                    row["previous_state"] = previous[0]
                    break
        db.execute(_insert_sql(), row)
        db.commit()

    journal_lifecycle_transition_sync(row)
    return row


async def safe_record_order_lifecycle_event(event: dict[str, Any]) -> None:
    try:
        await record_order_lifecycle_event(event)
    except Exception as exc:
        log.warning("Order lifecycle insert failed: %s", exc)


def safe_record_order_lifecycle_event_sync(event: dict[str, Any]) -> None:
    try:
        record_order_lifecycle_event_sync(event)
    except Exception as exc:
        log.warning("Order lifecycle sync insert failed: %s", exc)


async def journal_lifecycle_transition(row: dict[str, Any]) -> None:
    event_type = JOURNAL_EVENT_BY_STATE.get(normalize_state(row.get("state")))
    if not event_type:
        return
    await database.safe_record_trade_journal_event(_journal_event(row, event_type))


def journal_lifecycle_transition_sync(row: dict[str, Any]) -> None:
    event_type = JOURNAL_EVENT_BY_STATE.get(normalize_state(row.get("state")))
    if not event_type:
        return
    database.safe_record_trade_journal_event_sync(_journal_event(row, event_type))


def _journal_event(row: dict[str, Any], event_type: str) -> dict[str, Any]:
    return {
        "timestamp": row.get("timestamp"),
        "symbol": row.get("symbol"),
        "event_type": event_type,
        "decision": row.get("state"),
        "reason": row.get("reason"),
        "source_module": row.get("source_module") or "order_lifecycle",
        "price": row.get("price"),
        "quantity": row.get("quantity"),
        "raw_payload": row,
    }


async def get_order_lifecycle_events(limit: int = 200, symbol: str | None = None) -> list[dict[str, Any]]:
    await init_order_lifecycle_db()
    limit = max(1, min(int(limit or 200), 1000))

    if symbol:
        sql = """
        SELECT * FROM order_lifecycle_events
        WHERE symbol = ?
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """
        params = (symbol.strip().upper(), limit)
    else:
        sql = """
        SELECT * FROM order_lifecycle_events
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """
        params = (limit,)

    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


async def get_latest_order_lifecycle_states(limit: int = 200) -> list[dict[str, Any]]:
    await init_order_lifecycle_db()
    limit = max(1, min(int(limit or 200), 1000))
    sql = """
    SELECT ole.*
    FROM order_lifecycle_events ole
    INNER JOIN (
        SELECT
            COALESCE(CAST(perm_id AS TEXT), CAST(order_id AS TEXT), symbol || ':' || side) AS order_key,
            MAX(id) AS max_id
        FROM order_lifecycle_events
        GROUP BY order_key
    ) latest ON ole.id = latest.max_id
    ORDER BY ole.timestamp DESC, ole.id DESC
    LIMIT ?
    """
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]

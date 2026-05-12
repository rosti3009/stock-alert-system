from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
import aiosqlite

from config import DB_PATH, ACCOUNT_BALANCE

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


CREATE_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    price REAL,
    rsi REAL,
    ma20 REAL,
    ma50 REAL,
    ma200 REAL,
    volume REAL,
    avg_volume REAL,
    trend TEXT,
    entry_price REAL,
    stop_loss REAL,
    take_profit_1 REAL,
    take_profit_2 REAL,
    risk_percent REAL,
    score REAL,
    weekly_score REAL,
    reasons TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_LAST_SIGNALS = """
CREATE TABLE IF NOT EXISTS last_signals (
    symbol TEXT PRIMARY KEY,
    last_signal_type TEXT,
    last_signal_price REAL,
    last_signal_score REAL,
    last_signal_time TEXT
)
"""

CREATE_SCAN_RUNS = """
CREATE TABLE IF NOT EXISTS scan_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT,
    finished_at TEXT,
    total_symbols INTEGER DEFAULT 0,
    scanned_count INTEGER DEFAULT 0,
    skipped_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    buy_signals INTEGER DEFAULT 0,
    sell_signals INTEGER DEFAULT 0,
    status TEXT
)
"""

CREATE_DAILY_CANDIDATES = """
CREATE TABLE IF NOT EXISTS daily_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_run_id INTEGER,
    symbol TEXT,
    price REAL,
    rsi REAL,
    ma20 REAL,
    ma50 REAL,
    ma200 REAL,
    volume REAL,
    avg_volume REAL,
    atr REAL,
    trend TEXT,
    signal TEXT,
    entry_price REAL,
    stop_loss REAL,
    take_profit_1 REAL,
    take_profit_2 REAL,
    risk_percent REAL,
    rr_ratio REAL,
    score REAL,
    weekly_score REAL,
    weekly_rank INTEGER,
    reasons TEXT,
    weekly_reasons TEXT,
    error TEXT,
    skip_reason TEXT,
    created_at TEXT
)
"""

CREATE_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    buy_price REAL NOT NULL,
    quantity REAL DEFAULT 0,
    buy_date TEXT,
    current_price REAL,
    profit_amount REAL,
    profit_percent REAL,
    stop_loss REAL,
    take_profit_1 REAL,
    take_profit_2 REAL,
    status TEXT DEFAULT 'OPEN',
    action TEXT DEFAULT 'HOLD',
    reason TEXT,
    notes TEXT,
    created_at TEXT,
    updated_at TEXT,
    closed_at TEXT
)
"""

CREATE_EXECUTIONS = """
CREATE TABLE IF NOT EXISTS executions (
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

CREATE_TRADE_JOURNAL = """
CREATE TABLE IF NOT EXISTS trade_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    symbol TEXT,
    event_type TEXT NOT NULL,
    decision TEXT,
    reason TEXT,
    source_module TEXT,
    signal_score REAL,
    weekly_score REAL,
    market_regime TEXT,
    price REAL,
    quantity REAL,
    stop_loss REAL,
    take_profit_1 REAL,
    take_profit_2 REAL,
    risk_percent REAL,
    realized_pnl REAL,
    unrealized_pnl REAL,
    raw_json TEXT
)
"""


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

CREATE_APP_STATE = """
CREATE TABLE IF NOT EXISTS app_state (
    key TEXT PRIMARY KEY,
    value TEXT
)
"""


async def _ensure_columns(db: aiosqlite.Connection, table: str, columns: dict[str, str]) -> None:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        existing = {row[1] for row in await cursor.fetchall()}

    for name, col_type in columns.items():
        if name not in existing:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_SIGNALS)
        await db.execute(CREATE_LAST_SIGNALS)
        await db.execute(CREATE_SCAN_RUNS)
        await db.execute(CREATE_DAILY_CANDIDATES)
        await db.execute(CREATE_POSITIONS)
        await db.execute(CREATE_APP_STATE)
        await db.execute(CREATE_EXECUTIONS)
        await db.execute(CREATE_TRADE_JOURNAL)
        await db.execute(CREATE_ACCOUNT_SUMMARY)
        await db.execute(CREATE_OPEN_ORDERS)
        await db.execute(CREATE_EXECUTION_HISTORY)
        await db.execute(CREATE_EQUITY_CURVE)

        await _ensure_columns(db, "signals", {
            "score": "REAL",
            "weekly_score": "REAL",
            "reasons": "TEXT",
        })

        await _ensure_columns(db, "last_signals", {
            "last_signal_score": "REAL",
            "last_signal_time": "TEXT",
        })

        await _ensure_columns(db, "daily_candidates", {
            "atr": "REAL",
            "entry_price": "REAL",
            "rr_ratio": "REAL",
            "weekly_score": "REAL",
            "weekly_rank": "INTEGER",
            "weekly_reasons": "TEXT",
            "error": "TEXT",
            "skip_reason": "TEXT",
        })

        await _ensure_columns(db, "positions", {
            "quantity": "REAL DEFAULT 0",
            "profit_amount": "REAL",
            "profit_percent": "REAL",
            "closed_at": "TEXT",
        })

        await db.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_daily_candidates_symbol ON daily_candidates(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_daily_candidates_score ON daily_candidates(weekly_score)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_timestamp ON trade_journal(timestamp)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_symbol ON trade_journal(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_event_type ON trade_journal(event_type)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_account_summary_tag ON account_summary(tag)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_open_orders_symbol ON open_orders(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_execution_history_symbol ON execution_history(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_equity_curve_timestamp ON equity_curve(timestamp)")
        await db.commit()


def _json(value) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _decode_json(value):
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        return json.loads(value)
    except Exception:
        return [str(value)]


def _journal_payload(data: dict | None) -> str:
    if data is None:
        return "{}"

    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"repr": repr(data)}, ensure_ascii=False)


def _journal_row(event: dict) -> dict:
    raw_payload = event.get("raw_json")

    if raw_payload is None:
        raw_payload = event.get("raw_payload")

    return {
        "timestamp": event.get("timestamp") or now_iso(),
        "symbol": str(event.get("symbol") or "").strip().upper() or None,
        "event_type": event.get("event_type"),
        "decision": event.get("decision"),
        "reason": event.get("reason"),
        "source_module": event.get("source_module"),
        "signal_score": event.get("signal_score", event.get("score")),
        "weekly_score": event.get("weekly_score"),
        "market_regime": event.get("market_regime"),
        "price": event.get("price", event.get("entry_price", event.get("buy_price"))),
        "quantity": event.get("quantity"),
        "stop_loss": event.get("stop_loss"),
        "take_profit_1": event.get("take_profit_1"),
        "take_profit_2": event.get("take_profit_2"),
        "risk_percent": event.get("risk_percent"),
        "realized_pnl": event.get("realized_pnl"),
        "unrealized_pnl": event.get("unrealized_pnl", event.get("profit_amount")),
        "raw_json": _journal_payload(raw_payload if raw_payload is not None else event),
    }


def _trade_journal_insert_sql() -> str:
    return """
    INSERT INTO trade_journal (
        timestamp, symbol, event_type, decision, reason, source_module,
        signal_score, weekly_score, market_regime, price, quantity,
        stop_loss, take_profit_1, take_profit_2, risk_percent,
        realized_pnl, unrealized_pnl, raw_json
    )
    VALUES (
        :timestamp, :symbol, :event_type, :decision, :reason, :source_module,
        :signal_score, :weekly_score, :market_regime, :price, :quantity,
        :stop_loss, :take_profit_1, :take_profit_2, :risk_percent,
        :realized_pnl, :unrealized_pnl, :raw_json
    )
    """


async def record_trade_journal_event(event: dict) -> None:
    if not event.get("event_type"):
        raise ValueError("trade journal event_type is required")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_JOURNAL)
        await db.execute(_trade_journal_insert_sql(), _journal_row(event))
        await db.commit()


async def safe_record_trade_journal_event(event: dict) -> None:
    try:
        await record_trade_journal_event(event)
    except Exception as exc:
        log.warning("Trade journal insert failed: %s", exc)


def safe_record_trade_journal_event_sync(event: dict) -> None:
    try:
        if not event.get("event_type"):
            raise ValueError("trade journal event_type is required")

        with sqlite3.connect(DB_PATH) as db:
            db.execute(CREATE_TRADE_JOURNAL)
            db.execute(_trade_journal_insert_sql(), _journal_row(event))
            db.commit()

    except Exception as exc:
        log.warning("Trade journal sync insert failed: %s", exc)


async def get_trade_journal(limit: int = 200, symbol: str | None = None) -> list[dict]:
    limit = max(1, min(int(limit or 200), 1000))

    if symbol:
        sql = """
        SELECT *
        FROM trade_journal
        WHERE symbol = ?
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """
        params = (symbol.strip().upper(), limit)
    else:
        sql = """
        SELECT *
        FROM trade_journal
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """
        params = (limit,)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_JOURNAL)
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


def _row_to_dict(row) -> dict:
    d = dict(row)

    if "reasons" in d:
        d["reasons"] = _decode_json(d.get("reasons"))

    if "weekly_reasons" in d:
        d["weekly_reasons"] = _decode_json(d.get("weekly_reasons"))

    return d


async def get_app_state(key: str, default: str | None = None) -> str | None:
    sql = "SELECT value FROM app_state WHERE key = ?"

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (key,)) as cursor:
            row = await cursor.fetchone()

    return row[0] if row else default


async def set_app_state(key: str, value: str) -> None:
    sql = """
    INSERT INTO app_state (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, (key, value))
        await db.commit()


async def save_signal(row: dict) -> None:
    sql = """
    INSERT INTO signals (
        symbol, signal_type, price, rsi, ma20, ma50, ma200,
        volume, avg_volume, trend, entry_price, stop_loss,
        take_profit_1, take_profit_2, risk_percent,
        score, weekly_score, reasons, created_at
    )
    VALUES (
        :symbol, :signal_type, :price, :rsi, :ma20, :ma50, :ma200,
        :volume, :avg_volume, :trend, :entry_price, :stop_loss,
        :take_profit_1, :take_profit_2, :risk_percent,
        :score, :weekly_score, :reasons, :created_at
    )
    """

    safe = {
        "symbol": row.get("symbol"),
        "signal_type": row.get("signal_type") or row.get("signal"),
        "price": row.get("price"),
        "rsi": row.get("rsi"),
        "ma20": row.get("ma20"),
        "ma50": row.get("ma50"),
        "ma200": row.get("ma200"),
        "volume": row.get("volume"),
        "avg_volume": row.get("avg_volume"),
        "trend": row.get("trend"),
        "entry_price": row.get("entry_price"),
        "stop_loss": row.get("stop_loss"),
        "take_profit_1": row.get("take_profit_1"),
        "take_profit_2": row.get("take_profit_2"),
        "risk_percent": row.get("risk_percent"),
        "score": row.get("score"),
        "weekly_score": row.get("weekly_score") or row.get("score"),
        "reasons": _json(row.get("reasons")),
        "created_at": row.get("created_at") or now_iso(),
    }

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, safe)
        await db.commit()


async def get_recent_signals(limit: int = 50) -> list[dict]:
    sql = """
    SELECT *
    FROM signals
    ORDER BY created_at DESC
    LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [_row_to_dict(row) for row in rows]


async def get_last_signal(symbol: str) -> dict | None:
    sql = """
    SELECT symbol, last_signal_type, last_signal_price, last_signal_score, last_signal_time
    FROM last_signals
    WHERE symbol = ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (symbol.strip().upper(),)) as cursor:
            row = await cursor.fetchone()

    return dict(row) if row else None


async def upsert_last_signal(symbol: str, signal_type: str, price: float | None, score: float | None = None) -> None:
    sql = """
    INSERT INTO last_signals (
        symbol, last_signal_type, last_signal_price, last_signal_score, last_signal_time
    )
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(symbol) DO UPDATE SET
        last_signal_type = excluded.last_signal_type,
        last_signal_price = excluded.last_signal_price,
        last_signal_score = excluded.last_signal_score,
        last_signal_time = excluded.last_signal_time
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, (symbol.strip().upper(), signal_type, price, score, now_iso()))
        await db.commit()


async def start_scan_run(total_symbols: int) -> int:
    sql = """
    INSERT INTO scan_runs (
        started_at, total_symbols, status
    )
    VALUES (?, ?, ?)
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(sql, (now_iso(), total_symbols, "running"))
        await db.commit()
        return int(cursor.lastrowid)


async def finish_scan_run(scan_run_id: int, stats: dict, status: str = "completed") -> None:
    sql = """
    UPDATE scan_runs
    SET finished_at = ?,
        scanned_count = ?,
        skipped_count = ?,
        error_count = ?,
        buy_signals = ?,
        sell_signals = ?,
        status = ?
    WHERE id = ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, (
            now_iso(),
            stats.get("scanned_count", 0),
            stats.get("skipped_count", 0),
            stats.get("error_count", 0),
            stats.get("buy_signals", 0),
            stats.get("sell_signals", 0),
            status,
            scan_run_id,
        ))
        await db.commit()


async def get_scan_runs(limit: int = 20) -> list[dict]:
    sql = """
    SELECT *
    FROM scan_runs
    ORDER BY id DESC
    LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


async def save_daily_candidate(row: dict, scan_run_id: int) -> None:
    sql = """
    INSERT INTO daily_candidates (
        scan_run_id, symbol, price, rsi, ma20, ma50, ma200,
        volume, avg_volume, atr, trend, signal,
        entry_price, stop_loss, take_profit_1, take_profit_2,
        risk_percent, rr_ratio, score, weekly_score, weekly_rank,
        reasons, weekly_reasons, error, skip_reason, created_at
    )
    VALUES (
        :scan_run_id, :symbol, :price, :rsi, :ma20, :ma50, :ma200,
        :volume, :avg_volume, :atr, :trend, :signal,
        :entry_price, :stop_loss, :take_profit_1, :take_profit_2,
        :risk_percent, :rr_ratio, :score, :weekly_score, :weekly_rank,
        :reasons, :weekly_reasons, :error, :skip_reason, :created_at
    )
    """

    safe = {
        "scan_run_id": scan_run_id,
        "symbol": row.get("symbol"),
        "price": row.get("price"),
        "rsi": row.get("rsi"),
        "ma20": row.get("ma20"),
        "ma50": row.get("ma50"),
        "ma200": row.get("ma200"),
        "volume": row.get("volume"),
        "avg_volume": row.get("avg_volume"),
        "atr": row.get("atr"),
        "trend": row.get("trend"),
        "signal": row.get("signal"),
        "entry_price": row.get("entry_price"),
        "stop_loss": row.get("stop_loss"),
        "take_profit_1": row.get("take_profit_1"),
        "take_profit_2": row.get("take_profit_2"),
        "risk_percent": row.get("risk_percent"),
        "rr_ratio": row.get("rr_ratio"),
        "score": row.get("score"),
        "weekly_score": row.get("weekly_score"),
        "weekly_rank": row.get("weekly_rank"),
        "reasons": _json(row.get("reasons")),
        "weekly_reasons": _json(row.get("weekly_reasons")),
        "error": row.get("error"),
        "skip_reason": row.get("skip_reason"),
        "created_at": row.get("created_at") or now_iso(),
    }

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, safe)
        await db.commit()


async def get_latest_candidates(limit: int = 500) -> list[dict]:
    sql = """
    SELECT dc.*
    FROM daily_candidates dc
    INNER JOIN (
        SELECT symbol, MAX(id) AS max_id
        FROM daily_candidates
        WHERE symbol IS NOT NULL
        GROUP BY symbol
    ) latest ON dc.symbol = latest.symbol AND dc.id = latest.max_id
    ORDER BY dc.id DESC
    LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [_row_to_dict(row) for row in rows]


async def get_top_weekly(limit: int = 10) -> list[dict]:
    sql = """
    SELECT *
    FROM daily_candidates
    WHERE weekly_score IS NOT NULL
      AND weekly_score > 0
      AND signal NOT IN ('ERROR', 'SKIPPED')
    ORDER BY weekly_score DESC, score DESC, id DESC
    LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    result = [_row_to_dict(row) for row in rows]

    for i, row in enumerate(result, start=1):
        row["weekly_rank"] = i

    return result


async def get_open_positions() -> list[dict]:
    sql = """
    SELECT *
    FROM positions
    WHERE status = 'OPEN'
    ORDER BY created_at ASC
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


async def get_all_positions(limit: int = 100) -> list[dict]:
    sql = """
    SELECT *
    FROM positions
    ORDER BY status ASC, updated_at DESC, created_at DESC
    LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [dict(row) for row in rows]


async def count_open_positions() -> int:
    sql = "SELECT COUNT(*) FROM positions WHERE status = 'OPEN'"

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql) as cursor:
            row = await cursor.fetchone()

    return int(row[0]) if row else 0


async def add_position(data: dict, max_open_positions: int = 10) -> dict:
    symbol = str(data.get("symbol", "")).strip().upper()
    buy_price = float(data.get("buy_price", 0))
    quantity = float(data.get("quantity", 0) or 0)

    if not symbol:
        raise ValueError("Missing symbol")

    if buy_price <= 0:
        raise ValueError("buy_price must be greater than 0")

    existing = await get_position(symbol)

    if existing and (existing.get("status") or "OPEN") == "OPEN":
        raise ValueError(f"{symbol} already exists as an open position")

    open_count = await count_open_positions()

    if open_count >= max_open_positions:
        raise ValueError(f"Maximum open positions reached: {max_open_positions}")

    stop_loss = data.get("stop_loss")
    take_profit_1 = data.get("take_profit_1")
    take_profit_2 = data.get("take_profit_2")

    if stop_loss is None:
        stop_loss = buy_price * 0.92

    if take_profit_1 is None:
        take_profit_1 = buy_price * 1.08

    if take_profit_2 is None:
        take_profit_2 = buy_price * 1.16

    now = now_iso()

    sql = """
    INSERT INTO positions (
        symbol, buy_price, quantity, buy_date, current_price,
        profit_amount, profit_percent, stop_loss, take_profit_1, take_profit_2,
        status, action, reason, notes, created_at, updated_at, closed_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', 'HOLD', ?, ?, ?, ?, NULL)
    ON CONFLICT(symbol) DO UPDATE SET
        buy_price = excluded.buy_price,
        quantity = excluded.quantity,
        buy_date = excluded.buy_date,
        current_price = excluded.current_price,
        profit_amount = NULL,
        profit_percent = NULL,
        stop_loss = excluded.stop_loss,
        take_profit_1 = excluded.take_profit_1,
        take_profit_2 = excluded.take_profit_2,
        status = 'OPEN',
        action = 'HOLD',
        reason = excluded.reason,
        notes = excluded.notes,
        updated_at = excluded.updated_at,
        closed_at = NULL
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, (
            symbol,
            round(buy_price, 4),
            round(quantity, 6),
            data.get("buy_date") or now,
            data.get("current_price") or buy_price,
            None,
            None,
            round(float(stop_loss), 4),
            round(float(take_profit_1), 4),
            round(float(take_profit_2), 4),
            data.get("reason") or "Position added",
            data.get("notes"),
            now,
            now,
        ))
        await db.commit()

    return await get_position(symbol)


async def get_position(symbol: str) -> dict | None:
    sql = """
    SELECT *
    FROM positions
    WHERE symbol = ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (symbol.strip().upper(),)) as cursor:
            row = await cursor.fetchone()

    return dict(row) if row else None


async def update_position(symbol: str, updates: dict) -> dict | None:
    allowed = {
        "current_price",
        "profit_amount",
        "profit_percent",
        "stop_loss",
        "take_profit_1",
        "take_profit_2",
        "status",
        "action",
        "reason",
        "notes",
        "updated_at",
        "closed_at",
    }

    fields = []
    values = []

    for key, value in updates.items():
        if key in allowed:
            fields.append(f"{key} = ?")
            values.append(value)

    if not fields:
        return await get_position(symbol)

    values.append(symbol.strip().upper())

    sql = f"""
    UPDATE positions
    SET {", ".join(fields)}
    WHERE symbol = ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, values)
        await db.commit()

    return await get_position(symbol)


async def close_position(symbol: str, reason: str = "Closed manually") -> dict | None:
    updated = await update_position(symbol, {
        "status": "CLOSED",
        "action": "CLOSED",
        "reason": reason,
        "closed_at": now_iso(),
        "updated_at": now_iso(),
    })

    if updated and not str(reason or "").upper().startswith("AUTO:"):
        await safe_record_trade_journal_event({
            "symbol": symbol,
            "event_type": "MANUAL_CLOSE",
            "decision": "CLOSED",
            "reason": reason,
            "source_module": "database.close_position",
            "price": updated.get("current_price"),
            "quantity": updated.get("quantity"),
            "stop_loss": updated.get("stop_loss"),
            "take_profit_1": updated.get("take_profit_1"),
            "take_profit_2": updated.get("take_profit_2"),
            "realized_pnl": updated.get("profit_amount"),
            "raw_payload": updated,
        })

    return updated


async def get_performance_summary() -> dict:
    query = """
    SELECT
        COUNT(*) as total_trades,
        SUM(CASE WHEN profit_amount > 0 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN profit_amount < 0 THEN 1 ELSE 0 END) as losses,
        SUM(profit_amount) as total_pnl,
        SUM(CASE WHEN profit_amount > 0 THEN profit_amount ELSE 0 END) as gross_profit,
        SUM(CASE WHEN profit_amount < 0 THEN ABS(profit_amount) ELSE 0 END) as gross_loss
    FROM positions
    WHERE status = 'CLOSED'
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(query)
        row = await cursor.fetchone()

    if not row:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "total_pnl": 0,
            "profit_factor": 0,
        }

    total = row["total_trades"] or 0
    wins = row["wins"] or 0
    losses = row["losses"] or 0
    gross_profit = row["gross_profit"] or 0
    gross_loss = row["gross_loss"] or 0

    win_rate = (wins / total * 100) if total > 0 else 0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(row["total_pnl"] or 0, 2),
        "profit_factor": round(profit_factor, 2),
    }


async def get_realized_pnl() -> float:
    query = """
    SELECT SUM(profit_amount) as realized_pnl
    FROM positions
    WHERE status = 'CLOSED'
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(query)
        row = await cursor.fetchone()

    return float(row[0] or 0)


async def get_equity_curve(start_balance: float | None = None) -> list[dict]:
    if start_balance is None:
        start_balance = float(ACCOUNT_BALANCE)

    query = """
    SELECT
        symbol,
        profit_amount,
        profit_percent,
        closed_at,
        updated_at,
        created_at
    FROM positions
    WHERE status = 'CLOSED'
    ORDER BY COALESCE(closed_at, updated_at, created_at) ASC
    """

    equity = float(start_balance)

    curve = [{
        "label": "START",
        "equity": round(equity, 2),
        "pnl": 0,
        "profit_percent": 0,
        "closed_at": None,
    }]

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query) as cursor:
            rows = await cursor.fetchall()

    for row in rows:
        pnl = float(row["profit_amount"] or 0)
        equity += pnl

        curve.append({
            "label": row["symbol"],
            "equity": round(equity, 2),
            "pnl": round(pnl, 2),
            "profit_percent": row["profit_percent"] or 0,
            "closed_at": row["closed_at"] or row["updated_at"] or row["created_at"],
        })

    return curve

async def get_priority_symbols(limit: int = 200) -> list[str]:
    sql = """
    SELECT symbol
    FROM daily_candidates
    WHERE symbol IS NOT NULL
      AND signal NOT IN ('ERROR', 'SKIPPED')
      AND (
        weekly_score >= 60
        OR score >= 60
        OR signal = 'BUY'
      )
    GROUP BY symbol
    ORDER BY
      MAX(COALESCE(weekly_score, 0)) DESC,
      MAX(COALESCE(score, 0)) DESC,
      MAX(COALESCE(volume, 0)) DESC,
      MAX(id) DESC
    LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (limit,)) as cursor:
            rows = await cursor.fetchall()

    return [
        str(row[0]).strip().upper()
        for row in rows
        if row and row[0]
    ]
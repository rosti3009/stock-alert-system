from __future__ import annotations

import json
import logging
import sqlite3
import asyncio
from contextlib import closing
from datetime import datetime, timezone
import aiosqlite

import learning_analytics

from config import DB_PATH, ACCOUNT_BALANCE, VIRTUAL_TRADING_CAPITAL_USD

log = logging.getLogger(__name__)
APP_STATE_WRITE_LOCK = asyncio.Lock()


async def apply_sqlite_pragmas(db: aiosqlite.Connection) -> None:
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA busy_timeout=5000;")
    await db.execute("PRAGMA synchronous=NORMAL;")


def apply_sqlite_pragmas_sync(db: sqlite3.Connection) -> None:
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA busy_timeout=5000;")
    db.execute("PRAGMA synchronous=NORMAL;")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        await apply_sqlite_pragmas(db)
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        await apply_sqlite_pragmas(db)
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
    return dict(row) if row else None


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
    source TEXT,
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

PAPER_SESSION_STATE_KEY = "paper_trading_active_session"
RESETTABLE_SESSION_STATE_KEYS = {
    "portfolio_risk_state",
    "portfolio_risk_latest",
    "scan_offset",
}



CREATE_TRADE_DECISIONS = """
CREATE TABLE IF NOT EXISTS trade_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    setup_type TEXT DEFAULT 'UNKNOWN',
    entry_reason TEXT,
    score_breakdown TEXT,
    rvol REAL,
    vwap_status TEXT,
    breakout_status TEXT,
    momentum_score REAL,
    market_regime TEXT,
    sector TEXT,
    strategy_mode TEXT,
    entry_time TEXT,
    entry_price REAL,
    created_at TEXT
)
"""

CREATE_REJECTED_SETUPS = """
CREATE TABLE IF NOT EXISTS rejected_setups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    strategy_mode TEXT,
    rejection_reason TEXT,
    failed_filter TEXT,
    score REAL,
    rvol REAL,
    vwap_status TEXT,
    momentum_score REAL,
    spread_percent REAL,
    slippage_estimate REAL,
    market_regime TEXT,
    sector TEXT,
    time_of_day TEXT,
    raw_json TEXT
)
"""

CREATE_SETUP_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS setup_performance (
    setup_type TEXT PRIMARY KEY,
    total_trades INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0,
    avg_profit_amount REAL DEFAULT 0,
    avg_profit_percent REAL DEFAULT 0,
    total_profit_amount REAL DEFAULT 0,
    avg_hold_minutes REAL DEFAULT 0,
    updated_at TEXT
)
"""

CREATE_TRADE_OUTCOMES = """
CREATE TABLE IF NOT EXISTS trade_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    setup_type TEXT DEFAULT 'UNKNOWN',
    entry_time TEXT,
    exit_time TEXT,
    entry_price REAL,
    exit_price REAL,
    exit_reason TEXT,
    exit_engine TEXT,
    hold_minutes REAL,
    profit_amount REAL,
    profit_percent REAL,
    max_favorable_excursion REAL,
    max_adverse_excursion REAL,
    trailing_stop_used INTEGER DEFAULT 0,
    force_exit_used INTEGER DEFAULT 0,
    rvol REAL,
    rvol_range TEXT,
    market_regime TEXT,
    sector TEXT,
    time_of_day TEXT,
    raw_json TEXT,
    created_at TEXT
)
"""


CREATE_TRADE_REVIEWS = """
CREATE TABLE IF NOT EXISTS trade_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    position_id INTEGER,
    setup_type TEXT DEFAULT 'UNKNOWN',
    entry_time TEXT,
    entry_price REAL,
    entry_reason TEXT,
    entry_score REAL,
    entry_indicators_json TEXT,
    exit_time TEXT,
    exit_price REAL,
    exit_reason TEXT,
    exit_engine TEXT,
    exit_indicators_json TEXT,
    profit_amount REAL,
    profit_percent REAL,
    hold_minutes REAL,
    max_favorable_excursion REAL,
    max_adverse_excursion REAL,
    review_summary TEXT,
    lessons_json TEXT,
    created_at TEXT
)
"""

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


async def _ensure_columns(db: aiosqlite.Connection, table: str, columns: dict[str, str]) -> None:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        existing = {row[1] for row in await cursor.fetchall()}

    for name, col_type in columns.items():
        if name not in existing:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await apply_sqlite_pragmas(db)
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
        await db.execute(CREATE_ORDER_LIFECYCLE_EVENTS)
        await db.execute(CREATE_TRADE_DECISIONS)
        await db.execute(CREATE_REJECTED_SETUPS)
        await db.execute(CREATE_SETUP_PERFORMANCE)
        await db.execute(CREATE_TRADE_OUTCOMES)
        await db.execute(CREATE_TRADE_REVIEWS)
        await db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
        await db.execute(CREATE_ORDERS)
        await db.execute(CREATE_EXECUTIONS_V2)
        await db.execute(CREATE_RECONCILIATION_EVENTS)

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
            "source": "TEXT",
            "recovery_source_position_id": "INTEGER",
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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_timestamp ON order_lifecycle_events(timestamp)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_symbol ON order_lifecycle_events(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_order_id ON order_lifecycle_events(order_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_lifecycle_perm_id ON order_lifecycle_events(perm_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_rejected_setups_timestamp ON rejected_setups(timestamp)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_rejected_setups_symbol ON rejected_setups(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_decisions_symbol ON trade_decisions(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_outcomes_symbol ON trade_outcomes(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_outcomes_setup_type ON trade_outcomes(setup_type)")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_reviews_position_id ON trade_reviews(position_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_reviews_symbol ON trade_reviews(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trade_reviews_exit_time ON trade_reviews(exit_time)")
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
        await apply_sqlite_pragmas(db)
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

        with closing(sqlite3.connect(DB_PATH)) as db:
            apply_sqlite_pragmas_sync(db)
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
        await apply_sqlite_pragmas(db)
        async with db.execute(sql, (key,)) as cursor:
            row = await cursor.fetchone()

    return row[0] if row else default


async def set_app_state(key: str, value: str) -> None:
    sql = """
    INSERT INTO app_state (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """

    async with APP_STATE_WRITE_LOCK:
        async with aiosqlite.connect(DB_PATH) as db:
            await apply_sqlite_pragmas(db)
            await db.execute(sql, (key, value))
            await db.commit()


def set_app_state_sync(key: str, value: str) -> None:
    sql = """
    INSERT INTO app_state (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """

    with closing(sqlite3.connect(DB_PATH)) as db:
        apply_sqlite_pragmas_sync(db)
        db.execute(CREATE_APP_STATE)
        db.execute(sql, (key, value))
        db.commit()


async def delete_app_state(key: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM app_state WHERE key = ?", (key,))
        await db.commit()


async def delete_app_states(keys: list[str] | set[str] | tuple[str, ...]) -> None:
    if not keys:
        return

    placeholders = ",".join("?" for _ in keys)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"DELETE FROM app_state WHERE key IN ({placeholders})", tuple(keys))
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
    WHERE UPPER(TRIM(COALESCE(status, ''))) = 'OPEN'
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

    if updated:
        try:
            import live_position_tracker
            await live_position_tracker.prune_closed_positions()
        except Exception as exc:
            log.warning("Live position tracker prune failed after close: %s", exc)

    if updated:
        try:
            await record_trade_outcome({
                **updated,
                "symbol": symbol,
                "exit_reason": reason,
                "exit_engine": "auto_trader" if str(reason or "").upper().startswith("AUTO:") else "manual",
                "trailing_stop_used": "TRAIL" in str(reason or "").upper(),
                "force_exit_used": "FORCE" in str(reason or "").upper() or "EMERGENCY" in str(reason or "").upper(),
            })
        except Exception as exc:
            log.warning("Trade outcome analytics insert failed after close: %s", exc)

    if updated:
        await safe_upsert_trade_review_for_position(updated)

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


async def get_total_realized_pnl() -> float:
    query = """
    SELECT SUM(profit_amount) as realized_pnl
    FROM positions
    WHERE status = 'CLOSED'
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(query)
        row = await cursor.fetchone()

    return float(row[0] or 0)


def _paper_session_payload(
    *,
    session_id: str,
    started_at: str,
    realized_pnl_baseline: float,
    daily_realized_pnl_baseline: float,
    equity_curve_start_id: int | None,
    equity_curve_start_timestamp: str | None,
    session_start_equity: float,
) -> dict:
    return {
        "session_id": session_id,
        "started_at": started_at,
        "session_start_equity": round(float(session_start_equity or 0), 2),
        "realized_pnl_baseline": round(float(realized_pnl_baseline or 0), 2),
        "daily_realized_pnl_baseline": round(float(daily_realized_pnl_baseline or 0), 2),
        "equity_curve_start_id": equity_curve_start_id,
        "equity_curve_start_timestamp": equity_curve_start_timestamp,
        "active": True,
    }


async def get_latest_equity_curve_checkpoint() -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_EQUITY_CURVE)
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, timestamp, net_liquidation
            FROM equity_curve
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """
        ) as cursor:
            row = await cursor.fetchone()

    return dict(row) if row else None


async def get_daily_realized_pnl_total() -> float:
    start = datetime.combine(datetime.now(timezone.utc).date(), datetime.min.time(), tzinfo=timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_EXECUTION_HISTORY)
        async with db.execute(
            """
            SELECT SUM(realized_pnl)
            FROM execution_history
            WHERE COALESCE(time, created_at) >= ?
            """,
            (start,),
        ) as cursor:
            row = await cursor.fetchone()
    return float(row[0] or 0)


async def get_active_paper_session(create_if_missing: bool = True) -> dict | None:
    raw = await get_app_state(PAPER_SESSION_STATE_KEY)
    if raw:
        try:
            session = json.loads(raw)
            if isinstance(session, dict):
                session.setdefault("active", True)
                session.setdefault("session_start_equity", float(VIRTUAL_TRADING_CAPITAL_USD))
                session.setdefault("realized_pnl_baseline", 0.0)
                session.setdefault("daily_realized_pnl_baseline", 0.0)
                return session
        except Exception:
            log.warning("Invalid paper session state; creating a fresh baseline")

    if not create_if_missing:
        return None

    started_at = now_iso()
    session = _paper_session_payload(
        session_id=started_at,
        started_at=started_at,
        realized_pnl_baseline=0.0,
        daily_realized_pnl_baseline=0.0,
        equity_curve_start_id=0,
        equity_curve_start_timestamp=None,
        session_start_equity=float(VIRTUAL_TRADING_CAPITAL_USD),
    )
    await set_app_state(PAPER_SESSION_STATE_KEY, json.dumps(session, ensure_ascii=False, default=str))
    return session


async def reset_active_paper_session() -> dict:
    open_count = await count_open_positions()
    if open_count > 0:
        return {
            "status": "blocked",
            "reason": "Cannot reset paper trading session while positions are open.",
            "open_positions": open_count,
        }

    started_at = now_iso()
    checkpoint = await get_latest_equity_curve_checkpoint()
    realized_baseline = await get_total_realized_pnl()
    daily_baseline = await get_daily_realized_pnl_total()
    start_equity = float(VIRTUAL_TRADING_CAPITAL_USD) + float(realized_baseline)
    if checkpoint and checkpoint.get("net_liquidation") is not None:
        try:
            start_equity = float(checkpoint.get("net_liquidation"))
        except Exception:
            pass

    previous_session = await get_active_paper_session(create_if_missing=False)
    session = _paper_session_payload(
        session_id=started_at,
        started_at=started_at,
        realized_pnl_baseline=realized_baseline,
        daily_realized_pnl_baseline=daily_baseline,
        equity_curve_start_id=checkpoint.get("id") if checkpoint else None,
        equity_curve_start_timestamp=checkpoint.get("timestamp") if checkpoint else None,
        session_start_equity=start_equity,
    )

    await set_app_state(PAPER_SESSION_STATE_KEY, json.dumps(session, ensure_ascii=False, default=str))
    await delete_app_states(RESETTABLE_SESSION_STATE_KEYS)

    await safe_record_trade_journal_event({
        "event_type": "PAPER_SESSION_RESET",
        "decision": "RESET",
        "reason": "Started a fresh paper trading session after all positions were flat.",
        "source_module": "database.reset_active_paper_session",
        "realized_pnl": 0.0,
        "raw_payload": {
            "session": session,
            "previous_session": previous_session,
            "preserved_history": [
                "closed_positions",
                "trade_journal",
                "execution_history",
                "equity_curve",
                "scan_runs",
            ],
        },
    })

    return {
        "status": "reset",
        "open_positions": open_count,
        "session": session,
        "previous_session": previous_session,
        "preserved_history": True,
        "orders_submitted": 0,
    }


async def get_realized_pnl() -> float:
    total = await get_total_realized_pnl()
    session = await get_active_paper_session()
    baseline = float((session or {}).get("realized_pnl_baseline") or 0)
    return float(total) - baseline


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

def _analytics_json(data: dict | None) -> str:
    try:
        return json.dumps(data or {}, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"repr": repr(data)}, ensure_ascii=False)


def _failed_filter(reason: str | None, fallback: str | None = None) -> str | None:
    text = str(reason or fallback or "").lower()
    mapping = [
        ("score", "score"),
        ("regime", "market_regime"),
        ("position", "position_limit"),
        ("sizing", "position_sizing"),
        ("execution", "execution_quality"),
        ("risk", "risk"),
        ("vwap", "vwap"),
        ("rvol", "rvol"),
        ("spread", "spread"),
        ("slippage", "slippage"),
        ("market", "market_hours"),
        ("watchdog", "watchdog"),
    ]
    for needle, value in mapping:
        if needle in text:
            return value
    return fallback or "unknown"


def _symbol(value) -> str | None:
    symbol = str(value or "").strip().upper()
    return symbol or None


def _score_breakdown(row: dict) -> str:
    payload = {
        "score": row.get("score"),
        "weekly_score": row.get("weekly_score"),
        "intraday_technical_score": row.get("intraday_technical_score"),
        "reasons": row.get("reasons"),
        "intraday_score_reasons": row.get("intraday_score_reasons"),
    }
    return _analytics_json(payload)


async def record_rejected_setup(data: dict) -> None:
    symbol = _symbol(data.get("symbol"))
    if not symbol:
        return
    timestamp = data.get("timestamp") or now_iso()
    row = {
        "symbol": symbol,
        "timestamp": timestamp,
        "strategy_mode": data.get("strategy_mode"),
        "rejection_reason": data.get("rejection_reason") or data.get("reason"),
        "failed_filter": data.get("failed_filter") or _failed_filter(data.get("rejection_reason") or data.get("reason"), data.get("event_type")),
        "score": data.get("score") or data.get("signal_score") or data.get("weekly_score"),
        "rvol": learning_analytics.rvol(data),
        "vwap_status": learning_analytics.vwap_status(data),
        "momentum_score": learning_analytics.momentum_score(data),
        "spread_percent": data.get("spread_percent") or (data.get("execution_quality") or {}).get("spread_percent"),
        "slippage_estimate": data.get("slippage_estimate") or (data.get("execution_quality") or {}).get("slippage_estimate"),
        "market_regime": data.get("market_regime"),
        "sector": data.get("sector"),
        "time_of_day": data.get("time_of_day") or learning_analytics.time_of_day(timestamp),
        "raw_json": _analytics_json(data),
    }
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_REJECTED_SETUPS)
        await db.execute("""
            INSERT INTO rejected_setups (
                symbol, timestamp, strategy_mode, rejection_reason, failed_filter, score, rvol,
                vwap_status, momentum_score, spread_percent, slippage_estimate, market_regime,
                sector, time_of_day, raw_json
            ) VALUES (
                :symbol, :timestamp, :strategy_mode, :rejection_reason, :failed_filter, :score, :rvol,
                :vwap_status, :momentum_score, :spread_percent, :slippage_estimate, :market_regime,
                :sector, :time_of_day, :raw_json
            )
        """, row)
        await db.commit()


async def record_trade_decision(data: dict) -> None:
    symbol = _symbol(data.get("symbol"))
    if not symbol:
        return
    entry_time = data.get("entry_time") or data.get("timestamp") or now_iso()
    row = {
        "symbol": symbol,
        "setup_type": learning_analytics.classify_setup_type(data),
        "entry_reason": data.get("entry_reason") or data.get("reason"),
        "score_breakdown": data.get("score_breakdown") if isinstance(data.get("score_breakdown"), str) else _score_breakdown(data),
        "rvol": learning_analytics.rvol(data),
        "vwap_status": learning_analytics.vwap_status(data),
        "breakout_status": data.get("breakout_status"),
        "momentum_score": learning_analytics.momentum_score(data),
        "market_regime": data.get("market_regime"),
        "sector": data.get("sector"),
        "strategy_mode": data.get("strategy_mode"),
        "entry_time": entry_time,
        "entry_price": data.get("entry_price") or data.get("price") or data.get("buy_price"),
        "created_at": now_iso(),
    }
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_DECISIONS)
        await db.execute("""
            INSERT INTO trade_decisions (
                symbol, setup_type, entry_reason, score_breakdown, rvol, vwap_status, breakout_status,
                momentum_score, market_regime, sector, strategy_mode, entry_time, entry_price, created_at
            ) VALUES (
                :symbol, :setup_type, :entry_reason, :score_breakdown, :rvol, :vwap_status, :breakout_status,
                :momentum_score, :market_regime, :sector, :strategy_mode, :entry_time, :entry_price, :created_at
            )
        """, row)
        await db.commit()


async def _latest_trade_decision(symbol: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_DECISIONS)
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM trade_decisions
            WHERE symbol = ?
            ORDER BY entry_time DESC, id DESC
            LIMIT 1
        """, (symbol,)) as cursor:
            row = await cursor.fetchone()
    return dict(row) if row else None


async def record_trade_outcome(data: dict) -> None:
    symbol = _symbol(data.get("symbol"))
    if not symbol:
        return
    decision = await _latest_trade_decision(symbol) or {}
    exit_time = data.get("exit_time") or data.get("closed_at") or data.get("updated_at") or now_iso()
    entry_time = data.get("entry_time") or decision.get("entry_time") or data.get("buy_date") or data.get("created_at")
    profit_amount = data.get("profit_amount")
    profit_percent = data.get("profit_percent")
    row = {
        "symbol": symbol,
        "setup_type": data.get("setup_type") or decision.get("setup_type") or "UNKNOWN",
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": data.get("entry_price") or data.get("buy_price") or decision.get("entry_price"),
        "exit_price": data.get("exit_price") or data.get("current_price"),
        "exit_reason": data.get("exit_reason") or data.get("reason"),
        "exit_engine": data.get("exit_engine") or data.get("source_module") or "database.close_position",
        "hold_minutes": data.get("hold_minutes") or learning_analytics.hold_minutes(entry_time, exit_time),
        "profit_amount": profit_amount,
        "profit_percent": profit_percent,
        "max_favorable_excursion": data.get("max_favorable_excursion") or data.get("mfe"),
        "max_adverse_excursion": data.get("max_adverse_excursion") or data.get("mae"),
        "trailing_stop_used": 1 if data.get("trailing_stop_used") else 0,
        "force_exit_used": 1 if data.get("force_exit_used") else 0,
        "rvol": data.get("rvol") or decision.get("rvol"),
        "rvol_range": learning_analytics.rvol_range(data.get("rvol") or decision.get("rvol")),
        "market_regime": data.get("market_regime") or decision.get("market_regime"),
        "sector": data.get("sector") or decision.get("sector"),
        "time_of_day": data.get("time_of_day") or learning_analytics.time_of_day(entry_time),
        "raw_json": _analytics_json({"outcome": data, "decision": decision}),
        "created_at": now_iso(),
    }
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_OUTCOMES)
        await db.execute("""
            INSERT INTO trade_outcomes (
                symbol, setup_type, entry_time, exit_time, entry_price, exit_price, exit_reason, exit_engine,
                hold_minutes, profit_amount, profit_percent, max_favorable_excursion, max_adverse_excursion,
                trailing_stop_used, force_exit_used, rvol, rvol_range, market_regime, sector, time_of_day, raw_json, created_at
            ) VALUES (
                :symbol, :setup_type, :entry_time, :exit_time, :entry_price, :exit_price, :exit_reason, :exit_engine,
                :hold_minutes, :profit_amount, :profit_percent, :max_favorable_excursion, :max_adverse_excursion,
                :trailing_stop_used, :force_exit_used, :rvol, :rvol_range, :market_regime, :sector, :time_of_day, :raw_json, :created_at
            )
        """, row)
        await db.commit()
    await refresh_setup_performance()


async def refresh_setup_performance() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_SETUP_PERFORMANCE)
        await db.execute(CREATE_TRADE_OUTCOMES)
        await db.execute("DELETE FROM setup_performance")
        await db.execute("""
            INSERT INTO setup_performance (
                setup_type, total_trades, wins, losses, win_rate, avg_profit_amount,
                avg_profit_percent, total_profit_amount, avg_hold_minutes, updated_at
            )
            SELECT
                COALESCE(setup_type, 'UNKNOWN') AS setup_type,
                COUNT(*) AS total_trades,
                SUM(CASE WHEN COALESCE(profit_amount, 0) > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN COALESCE(profit_amount, 0) < 0 THEN 1 ELSE 0 END) AS losses,
                ROUND(100.0 * SUM(CASE WHEN COALESCE(profit_amount, 0) > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS win_rate,
                ROUND(AVG(COALESCE(profit_amount, 0)), 2) AS avg_profit_amount,
                ROUND(AVG(COALESCE(profit_percent, 0)), 2) AS avg_profit_percent,
                ROUND(SUM(COALESCE(profit_amount, 0)), 2) AS total_profit_amount,
                ROUND(AVG(COALESCE(hold_minutes, 0)), 2) AS avg_hold_minutes,
                ?
            FROM trade_outcomes
            GROUP BY COALESCE(setup_type, 'UNKNOWN')
        """, (now_iso(),))
        await db.commit()
    return await get_setup_performance()


async def get_rejected_setups(limit: int = 200) -> list[dict]:
    limit = max(1, min(int(limit or 200), 1000))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_REJECTED_SETUPS)
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM rejected_setups ORDER BY timestamp DESC, id DESC LIMIT ?", (limit,)) as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def get_trade_outcomes(limit: int = 200) -> list[dict]:
    limit = max(1, min(int(limit or 200), 1000))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_OUTCOMES)
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM trade_outcomes ORDER BY exit_time DESC, id DESC LIMIT ?", (limit,)) as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]

def _safe_json_dict(value) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {"value": decoded}
    except Exception:
        return {"raw": str(value)}


def _first_present(*values):
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _review_summary(row: dict, entry_indicators: dict, exit_indicators: dict) -> str:
    symbol = row.get("symbol") or "UNKNOWN"
    setup = row.get("setup_type") or "UNKNOWN"
    entry_reason = row.get("entry_reason") or "No entry reason recorded"
    exit_reason = row.get("exit_reason") or "No exit reason recorded"
    exit_engine = row.get("exit_engine") or "unknown engine"
    pnl = float(row.get("profit_amount") or 0)
    pnl_pct = row.get("profit_percent")
    hold = row.get("hold_minutes")
    followed_plan = "unknown because plan metadata was incomplete"
    if row.get("entry_reason") and row.get("exit_reason"):
        followed_plan = "reviewable from recorded entry and exit rationale"
    worked = "positive realized PnL" if pnl > 0 else "risk controls limited the recorded outcome" if pnl == 0 else "nothing obvious in the final PnL"
    failed = "exit or entry timing needs review" if pnl < 0 else "no major failure captured in the outcome data"
    risk_reward = "profitable" if pnl > 0 else "flat" if pnl == 0 else "unprofitable"
    hold_result = f"held for {hold:.1f} minutes" if hold is not None else "hold time unavailable"
    entry_context = ", ".join(
        f"{key}={value}" for key, value in entry_indicators.items()
        if value not in (None, "", {}) and key in {"rvol", "vwap_status", "breakout_status", "momentum_score", "market_regime", "strategy_mode"}
    ) or "no indicator context recorded"
    return (
        f"{symbol} {setup} trade review. Entered because: {entry_reason}. "
        f"Entry context: {entry_context}. Exited because: {exit_reason} via {exit_engine}. "
        f"Setup followed plan: {followed_plan}. What worked: {worked}. "
        f"What failed: {failed}. Risk/reward result: {risk_reward} "
        f"({pnl:.2f}{'' if pnl_pct is None else f', {float(pnl_pct):.2f}%'}). "
        f"Hold time result: {hold_result}."
    )


def _review_lessons(row: dict, entry_indicators: dict, exit_indicators: dict) -> list[dict]:
    pnl = float(row.get("profit_amount") or 0)
    lessons = []
    lessons.append({
        "topic": "entry_quality",
        "lesson": "Entry reason and indicator context were captured" if row.get("entry_reason") else "Entry rationale was missing; improve decision logging",
    })
    lessons.append({
        "topic": "exit_quality",
        "lesson": "Exit reason was captured" if row.get("exit_reason") else "Exit rationale was missing; improve close logging",
    })
    lessons.append({
        "topic": "risk_reward",
        "lesson": "Repeatable setup candidate" if pnl > 0 else "Review sizing, stop distance, and exit trigger before repeating",
    })
    if row.get("hold_minutes") is None:
        lessons.append({"topic": "data_quality", "lesson": "Hold time could not be computed because entry or exit time was missing"})
    if not entry_indicators:
        lessons.append({"topic": "data_quality", "lesson": "Entry indicators were unavailable for replay"})
    if not exit_indicators:
        lessons.append({"topic": "data_quality", "lesson": "Exit indicators or execution context were unavailable for replay"})
    return lessons


async def _latest_trade_outcome_for_review(db: aiosqlite.Connection, symbol: str, position: dict) -> dict | None:
    await db.execute(CREATE_TRADE_OUTCOMES)
    candidates = []
    async with db.execute(
        """
        SELECT * FROM trade_outcomes
        WHERE symbol = ?
        ORDER BY exit_time DESC, id DESC
        LIMIT 10
        """,
        (symbol,),
    ) as cursor:
        candidates = [dict(row) for row in await cursor.fetchall()]
    if not candidates:
        return None
    closed_at = learning_analytics.parse_dt(position.get("closed_at") or position.get("updated_at"))
    if not closed_at:
        return candidates[0]
    def distance(outcome: dict) -> float:
        exit_dt = learning_analytics.parse_dt(outcome.get("exit_time"))
        return abs((exit_dt - closed_at).total_seconds()) if exit_dt else 10**12
    return sorted(candidates, key=distance)[0]


async def _latest_decision_for_review(db: aiosqlite.Connection, symbol: str, entry_time: str | None) -> dict | None:
    await db.execute(CREATE_TRADE_DECISIONS)
    if entry_time:
        async with db.execute(
            """
            SELECT * FROM trade_decisions
            WHERE symbol = ? AND COALESCE(entry_time, created_at) <= ?
            ORDER BY entry_time DESC, id DESC
            LIMIT 1
            """,
            (symbol, entry_time),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
    async with db.execute(
        "SELECT * FROM trade_decisions WHERE symbol = ? ORDER BY entry_time DESC, id DESC LIMIT 1",
        (symbol,),
    ) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _executions_for_review(db: aiosqlite.Connection, symbol: str, entry_time: str | None, exit_time: str | None) -> list[dict]:
    await db.execute(CREATE_EXECUTIONS)
    params: list = [symbol]
    where = ["symbol = ?"]
    if entry_time:
        where.append("COALESCE(time, created_at) >= ?")
        params.append(entry_time)
    if exit_time:
        where.append("COALESCE(time, created_at) <= ?")
        params.append(exit_time)
    sql = f"SELECT * FROM executions WHERE {' AND '.join(where)} ORDER BY COALESCE(time, created_at) ASC LIMIT 50"
    async with db.execute(sql, params) as cursor:
        rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def build_trade_review(position: dict) -> dict | None:
    symbol = _symbol(position.get("symbol"))
    if not symbol or str(position.get("status") or "").upper() != "CLOSED":
        return None

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_REVIEWS)
        await db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
        await db.execute(CREATE_ORDERS)
        await db.execute(CREATE_EXECUTIONS_V2)
        await db.execute(CREATE_RECONCILIATION_EVENTS)
        db.row_factory = aiosqlite.Row
        outcome = await _latest_trade_outcome_for_review(db, symbol, position) or {}
        entry_time = _first_present(outcome.get("entry_time"), position.get("buy_date"), position.get("created_at"))
        exit_time = _first_present(outcome.get("exit_time"), position.get("closed_at"), position.get("updated_at"))
        decision = await _latest_decision_for_review(db, symbol, entry_time) or {}
        executions = await _executions_for_review(db, symbol, entry_time, exit_time)

    entry_indicators = {
        "decision_id": decision.get("id"),
        "setup_type": decision.get("setup_type"),
        "score_breakdown": _safe_json_dict(decision.get("score_breakdown")),
        "rvol": decision.get("rvol"),
        "vwap_status": decision.get("vwap_status"),
        "breakout_status": decision.get("breakout_status"),
        "momentum_score": decision.get("momentum_score"),
        "market_regime": decision.get("market_regime"),
        "sector": decision.get("sector"),
        "strategy_mode": decision.get("strategy_mode"),
    }
    exit_indicators = {
        "outcome_id": outcome.get("id"),
        "trailing_stop_used": outcome.get("trailing_stop_used"),
        "force_exit_used": outcome.get("force_exit_used"),
        "rvol_range": outcome.get("rvol_range"),
        "market_regime": outcome.get("market_regime"),
        "sector": outcome.get("sector"),
        "time_of_day": outcome.get("time_of_day"),
        "executions": executions,
        "outcome_raw_json": _safe_json_dict(outcome.get("raw_json")),
    }
    entry_score_payload = entry_indicators.get("score_breakdown") or {}
    entry_score = _first_present(
        entry_score_payload.get("score"),
        entry_score_payload.get("weekly_score"),
        entry_score_payload.get("intraday_technical_score"),
        decision.get("momentum_score"),
    )
    row = {
        "symbol": symbol,
        "position_id": position.get("id"),
        "setup_type": _first_present(outcome.get("setup_type"), decision.get("setup_type"), "UNKNOWN"),
        "entry_time": _first_present(entry_time, decision.get("entry_time")),
        "entry_price": _first_present(outcome.get("entry_price"), decision.get("entry_price"), position.get("buy_price")),
        "entry_reason": _first_present(decision.get("entry_reason"), position.get("reason"), "No entry reason recorded"),
        "entry_score": entry_score,
        "entry_indicators_json": _analytics_json(entry_indicators),
        "exit_time": exit_time,
        "exit_price": _first_present(outcome.get("exit_price"), position.get("current_price")),
        "exit_reason": _first_present(outcome.get("exit_reason"), position.get("reason"), "No exit reason recorded"),
        "exit_engine": _first_present(outcome.get("exit_engine"), "database.close_position"),
        "exit_indicators_json": _analytics_json(exit_indicators),
        "profit_amount": _first_present(outcome.get("profit_amount"), position.get("profit_amount")),
        "profit_percent": _first_present(outcome.get("profit_percent"), position.get("profit_percent")),
        "hold_minutes": _first_present(outcome.get("hold_minutes"), learning_analytics.hold_minutes(entry_time, exit_time)),
        "max_favorable_excursion": outcome.get("max_favorable_excursion"),
        "max_adverse_excursion": outcome.get("max_adverse_excursion"),
        "created_at": now_iso(),
    }
    row["review_summary"] = _review_summary(row, entry_indicators, exit_indicators)
    row["lessons_json"] = _analytics_json(_review_lessons(row, entry_indicators, exit_indicators))
    return row


async def upsert_trade_review_for_position(position: dict) -> dict | None:
    row = await build_trade_review(position)
    if not row:
        return None
    sql = """
    INSERT INTO trade_reviews (
        symbol, position_id, setup_type, entry_time, entry_price, entry_reason, entry_score,
        entry_indicators_json, exit_time, exit_price, exit_reason, exit_engine, exit_indicators_json,
        profit_amount, profit_percent, hold_minutes, max_favorable_excursion, max_adverse_excursion,
        review_summary, lessons_json, created_at
    ) VALUES (
        :symbol, :position_id, :setup_type, :entry_time, :entry_price, :entry_reason, :entry_score,
        :entry_indicators_json, :exit_time, :exit_price, :exit_reason, :exit_engine, :exit_indicators_json,
        :profit_amount, :profit_percent, :hold_minutes, :max_favorable_excursion, :max_adverse_excursion,
        :review_summary, :lessons_json, :created_at
    )
    ON CONFLICT(position_id) DO UPDATE SET
        symbol = excluded.symbol,
        setup_type = excluded.setup_type,
        entry_time = excluded.entry_time,
        entry_price = excluded.entry_price,
        entry_reason = excluded.entry_reason,
        entry_score = excluded.entry_score,
        entry_indicators_json = excluded.entry_indicators_json,
        exit_time = excluded.exit_time,
        exit_price = excluded.exit_price,
        exit_reason = excluded.exit_reason,
        exit_engine = excluded.exit_engine,
        exit_indicators_json = excluded.exit_indicators_json,
        profit_amount = excluded.profit_amount,
        profit_percent = excluded.profit_percent,
        hold_minutes = excluded.hold_minutes,
        max_favorable_excursion = excluded.max_favorable_excursion,
        max_adverse_excursion = excluded.max_adverse_excursion,
        review_summary = excluded.review_summary,
        lessons_json = excluded.lessons_json
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_REVIEWS)
        await db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
        await db.execute(CREATE_ORDERS)
        await db.execute(CREATE_EXECUTIONS_V2)
        await db.execute(CREATE_RECONCILIATION_EVENTS)
        await db.execute(sql, row)
        await db.commit()
    return row


async def safe_upsert_trade_review_for_position(position: dict) -> None:
    try:
        await upsert_trade_review_for_position(position)
    except Exception as exc:
        log.warning("Trade review upsert failed: %s", exc)


async def rebuild_trade_reviews() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_POSITIONS)
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM positions WHERE UPPER(TRIM(COALESCE(status, ''))) = 'CLOSED' ORDER BY closed_at DESC, updated_at DESC") as cursor:
            positions = [dict(row) for row in await cursor.fetchall()]
    rebuilt = 0
    failed = 0
    errors: list[str] = []
    for position in positions:
        try:
            review = await upsert_trade_review_for_position(position)
            if review:
                rebuilt += 1
        except Exception as exc:
            failed += 1
            msg = f"{position.get('symbol')}: {exc}"
            errors.append(msg)
            log.warning("Trade review rebuild failed for %s: %s", position.get("symbol"), exc)
    return {"rebuilt": rebuilt, "failed": failed, "errors": errors[:10], "informational_only": True}


def _trade_review_row(row) -> dict:
    data = dict(row)
    for key in ("entry_indicators_json", "exit_indicators_json", "lessons_json"):
        try:
            data[key.replace("_json", "")] = json.loads(data.get(key) or ("[]" if key == "lessons_json" else "{}"))
        except Exception:
            data[key.replace("_json", "")] = [] if key == "lessons_json" else {}
    return data


async def get_trade_reviews(limit: int = 200, symbol: str | None = None) -> list[dict]:
    limit = max(1, min(int(limit or 200), 1000))
    params: tuple
    if symbol:
        sql = """
        SELECT * FROM trade_reviews
        WHERE symbol = ?
        ORDER BY exit_time DESC, id DESC
        LIMIT ?
        """
        params = (symbol.strip().upper(), limit)
    else:
        sql = """
        SELECT * FROM trade_reviews
        ORDER BY exit_time DESC, id DESC
        LIMIT ?
        """
        params = (limit,)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_REVIEWS)
        await db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
        await db.execute(CREATE_ORDERS)
        await db.execute(CREATE_EXECUTIONS_V2)
        await db.execute(CREATE_RECONCILIATION_EVENTS)
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
    return [_trade_review_row(row) for row in rows]


async def get_setup_performance() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_SETUP_PERFORMANCE)
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM setup_performance ORDER BY win_rate DESC, avg_profit_amount DESC") as cursor:
            rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def get_learning_summary() -> dict:
    await refresh_setup_performance()
    performance = await get_setup_performance()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADE_OUTCOMES)
        await db.execute(CREATE_REJECTED_SETUPS)
        db.row_factory = aiosqlite.Row
        queries = {
            "best_rejection_reasons": """
                SELECT rs.rejection_reason, COUNT(*) AS rejection_count,
                       ROUND(AVG(CASE WHEN t.profit_amount > 0 THEN 1.0 ELSE 0 END) * 100, 2) AS later_win_rate
                FROM rejected_setups rs LEFT JOIN trade_outcomes t ON t.symbol = rs.symbol AND t.exit_time >= rs.timestamp
                GROUP BY rs.rejection_reason ORDER BY later_win_rate ASC, rejection_count DESC LIMIT 10
            """,
            "worst_rejection_reasons": """
                SELECT rejection_reason, COUNT(*) AS rejection_count FROM rejected_setups
                GROUP BY rejection_reason ORDER BY rejection_count DESC LIMIT 10
            """,
            "rvol_ranges": """
                SELECT rvol_range, COUNT(*) AS total_trades, ROUND(AVG(profit_amount), 2) AS avg_profit_amount,
                       ROUND(100.0 * SUM(CASE WHEN profit_amount > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS win_rate
                FROM trade_outcomes GROUP BY rvol_range ORDER BY avg_profit_amount DESC LIMIT 10
            """,
            "intraday_hours": """
                SELECT time_of_day, COUNT(*) AS total_trades, ROUND(AVG(profit_amount), 2) AS avg_profit_amount,
                       ROUND(100.0 * SUM(CASE WHEN profit_amount > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS win_rate
                FROM trade_outcomes GROUP BY time_of_day ORDER BY avg_profit_amount DESC LIMIT 10
            """,
            "sectors": """
                SELECT COALESCE(sector, 'UNKNOWN') AS sector, COUNT(*) AS total_trades, ROUND(AVG(profit_amount), 2) AS avg_profit_amount,
                       ROUND(100.0 * SUM(CASE WHEN profit_amount > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS win_rate
                FROM trade_outcomes GROUP BY COALESCE(sector, 'UNKNOWN') ORDER BY avg_profit_amount DESC LIMIT 10
            """,
            "market_regimes": """
                SELECT COALESCE(market_regime, 'UNKNOWN') AS market_regime, COUNT(*) AS total_trades, ROUND(AVG(profit_amount), 2) AS avg_profit_amount,
                       ROUND(100.0 * SUM(CASE WHEN profit_amount > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS win_rate
                FROM trade_outcomes GROUP BY COALESCE(market_regime, 'UNKNOWN') ORDER BY avg_profit_amount DESC LIMIT 10
            """,
            "loss_reasons": """
                SELECT COALESCE(exit_reason, 'UNKNOWN') AS exit_reason, COUNT(*) AS losses
                FROM trade_outcomes WHERE COALESCE(profit_amount, 0) < 0
                GROUP BY COALESCE(exit_reason, 'UNKNOWN') ORDER BY losses DESC LIMIT 10
            """,
        }
        results = {}
        for name, sql in queries.items():
            async with db.execute(sql) as cursor:
                results[name] = [dict(row) for row in await cursor.fetchall()]
        async with db.execute("SELECT ROUND(AVG(COALESCE(hold_minutes, 0)), 2) AS average_hold_time FROM trade_outcomes") as cursor:
            hold = await cursor.fetchone()
    breakout_vs_reversal = [row for row in performance if row.get("setup_type") in {"BREAKOUT", "REVERSAL"}]
    return {
        "informational_only": True,
        "safety": "Analytics layer records and summarizes data only; it does not place orders or change strategy automatically.",
        "win_rate_by_setup_type": performance,
        "avg_profit_by_setup_type": performance,
        "best_rejection_reasons": results["best_rejection_reasons"],
        "worst_rejection_reasons": results["worst_rejection_reasons"],
        "breakout_vs_reversal_performance": breakout_vs_reversal,
        "best_rvol_ranges": results["rvol_ranges"],
        "best_intraday_hours": results["intraday_hours"],
        "best_sectors": results["sectors"],
        "market_regime_performance": results["market_regimes"],
        "average_hold_time": (dict(hold).get("average_hold_time") if hold else 0) or 0,
        "most_common_loss_reasons": results["loss_reasons"],
    }

CREATE_BROKER_SYNC_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS broker_sync_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    synced_at TEXT,
    ok INTEGER,
    connected INTEGER,
    account TEXT,
    net_liquidation REAL,
    total_cash REAL,
    available_funds REAL,
    buying_power REAL,
    positions_json TEXT,
    open_orders_json TEXT,
    executions_json TEXT,
    errors_json TEXT
)
"""

CREATE_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_order_id INTEGER,
    broker_perm_id INTEGER,
    symbol TEXT,
    action TEXT,
    order_type TEXT,
    quantity REAL,
    filled_quantity REAL,
    remaining_quantity REAL,
    limit_price REAL,
    stop_price REAL,
    status TEXT,
    source TEXT,
    reason TEXT,
    created_at TEXT,
    updated_at TEXT,
    submitted_at TEXT,
    filled_at TEXT,
    cancelled_at TEXT,
    rejected_at TEXT
)
"""
CREATE_EXECUTIONS_V2 = """
CREATE TABLE IF NOT EXISTS executions_v2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT UNIQUE,
    broker_order_id INTEGER,
    broker_perm_id INTEGER,
    symbol TEXT,
    side TEXT,
    shares REAL,
    price REAL,
    commission REAL,
    execution_time TEXT,
    account TEXT,
    created_at TEXT
)
"""
CREATE_RECONCILIATION_EVENTS = """
CREATE TABLE IF NOT EXISTS reconciliation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT,
    severity TEXT,
    symbol TEXT,
    details_json TEXT,
    status TEXT,
    created_at TEXT,
    resolved_at TEXT
)
"""

async def save_broker_sync_snapshot(snapshot: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
        eq=snapshot.get('equity') or {}
        await db.execute("""INSERT INTO broker_sync_snapshots (synced_at,ok,connected,account,net_liquidation,total_cash,available_funds,buying_power,positions_json,open_orders_json,executions_json,errors_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
            snapshot.get('synced_at'), 1 if snapshot.get('ok') else 0, 1 if snapshot.get('connected') else 0, snapshot.get('account'), eq.get('net_liquidation'), eq.get('total_cash'), eq.get('available_funds'), eq.get('buying_power'), json.dumps(snapshot.get('positions',[])), json.dumps(snapshot.get('open_orders',[])), json.dumps(snapshot.get('executions',[])), json.dumps(snapshot.get('errors',[]))
        ))
        await db.commit()

async def get_latest_broker_sync_snapshot() -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row
        await db.execute(CREATE_BROKER_SYNC_SNAPSHOTS)
        async with db.execute("SELECT * FROM broker_sync_snapshots ORDER BY id DESC LIMIT 1") as c:
            r=await c.fetchone()
    return dict(r) if r else None

async def upsert_position(data: dict) -> dict:
    return await add_position(data, max_open_positions=999999)

async def insert_reconciliation_event(event_type: str, severity: str, symbol: str | None, details: dict, status: str='OPEN') -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_RECONCILIATION_EVENTS)
        await db.execute("INSERT INTO reconciliation_events (event_type,severity,symbol,details_json,status,created_at) VALUES (?,?,?,?,?,?)", (event_type,severity,symbol,json.dumps(details),status,now_iso()))
        await db.commit()

async def get_open_reconciliation_issues() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row
        await db.execute(CREATE_RECONCILIATION_EVENTS)
        async with db.execute("SELECT * FROM reconciliation_events WHERE status='OPEN' ORDER BY id DESC LIMIT 500") as c:
            rows=await c.fetchall()
    return [dict(r) for r in rows]

async def reconcile_orders_and_executions(snapshot: dict) -> dict:
    events=[]
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory=aiosqlite.Row
        await db.execute(CREATE_ORDERS); await db.execute(CREATE_EXECUTIONS_V2)
        for o in snapshot.get('open_orders',[]):
            cur=await db.execute("SELECT id FROM orders WHERE broker_order_id=? OR broker_perm_id=?", (o.get('order_id'), o.get('perm_id')))
            if not await cur.fetchone():
                await db.execute("INSERT INTO orders (broker_order_id,broker_perm_id,symbol,action,order_type,quantity,filled_quantity,remaining_quantity,limit_price,stop_price,status,source,reason,created_at,updated_at,submitted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (o.get('order_id'),o.get('perm_id'),o.get('symbol'),o.get('action'),o.get('order_type'),o.get('quantity'),o.get('filled_quantity'),o.get('remaining_quantity'),o.get('limit_price'),o.get('stop_price'),o.get('status'),'IBKR_OPEN_ORDER_ADOPTED','IBKR_OPEN_ORDER_ADOPTED',now_iso(),now_iso(),now_iso()))
                events.append({'event_type':'IBKR_OPEN_ORDER_ADOPTED','severity':'INFO','symbol':o.get('symbol')})
        for ex in snapshot.get('executions',[]):
            await db.execute("INSERT OR IGNORE INTO executions_v2 (execution_id,broker_order_id,broker_perm_id,symbol,side,shares,price,commission,execution_time,account,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (ex.get('execution_id'),ex.get('order_id'),ex.get('perm_id'),ex.get('symbol'),ex.get('side'),ex.get('shares'),ex.get('price'),ex.get('commission'),ex.get('time'),ex.get('account'),now_iso()))
        await db.commit()
    return {'events':events}

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Any

import aiosqlite

import config
import database
from circuit_breaker import auto_clear_recoverable_circuit_breaker, get_circuit_breaker_state, trip_circuit_breaker
from telegram_notifier import send_watchdog_alert
from tws_connection_manager import disconnect_ib_sync, get_ib_sync, is_ib_connected

log = logging.getLogger(__name__)

WATCHDOG_STATUS_KEY = "watchdog_status"
WATCHDOG_ALERT_STATE_PREFIX = "watchdog_alert_sent:"
WATCHDOG_CIRCUIT_SOURCE = "watchdog"
LAST_MARKET_DATA_AT_KEY = "last_market_data_at"
LAST_MARKET_DATA_REFRESH_SOURCE_KEY = "last_market_data_refresh_source"
DEFAULT_STALE_SECONDS = 120
DEFAULT_DISCONNECT_CIRCUIT_SECONDS = 180
DEFAULT_ALERT_COOLDOWN_SECONDS = 300
DEFAULT_RECONNECT_BACKOFF_SECONDS = 30
MAX_RECONNECT_BACKOFF_SECONDS = 300
MARKET_DATA_STALE_REASON_PREFIX = "Market data stale"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _age_seconds(value: Any) -> int | None:
    dt = parse_dt(value)
    if not dt:
        return None
    return max(0, int((now_utc() - dt).total_seconds()))


def _json(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _config_int(name: str, default: int) -> int:
    try:
        return int(getattr(config, name, default))
    except Exception:
        return default


def _thresholds() -> dict:
    return {
        "mirror_stale_seconds": _config_int("WATCHDOG_TWS_MIRROR_STALE_SECONDS", DEFAULT_STALE_SECONDS),
        "execution_stale_seconds": _config_int("WATCHDOG_EXECUTION_SYNC_STALE_SECONDS", DEFAULT_STALE_SECONDS),
        "market_data_stale_seconds": _config_int("WATCHDOG_MARKET_DATA_STALE_SECONDS", 300),
        "position_tracking_stale_seconds": _config_int("WATCHDOG_POSITION_TRACKING_STALE_SECONDS", max(60, _config_int("POSITION_TRACK_INTERVAL_SECONDS", 15) * 4)),
        "disconnect_circuit_seconds": _config_int("WATCHDOG_DISCONNECT_CIRCUIT_SECONDS", DEFAULT_DISCONNECT_CIRCUIT_SECONDS),
        "alert_cooldown_seconds": _config_int("WATCHDOG_ALERT_COOLDOWN_SECONDS", DEFAULT_ALERT_COOLDOWN_SECONDS),
        "reconnect_backoff_seconds": _config_int("WATCHDOG_RECONNECT_BACKOFF_SECONDS", DEFAULT_RECONNECT_BACKOFF_SECONDS),
    }


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


async def _get_tws_heartbeat(db: aiosqlite.Connection) -> dict:
    if not await _table_exists(db, "tws_heartbeat"):
        return {"found": False, "connected": False, "last_sync_at": None, "age_seconds": None, "error": "No TWS heartbeat table"}

    async with db.execute(
        "SELECT connected, account, last_sync_at, error FROM tws_heartbeat WHERE id = 1"
    ) as cursor:
        row = await cursor.fetchone()

    if not row:
        return {"found": False, "connected": False, "last_sync_at": None, "age_seconds": None, "error": "No TWS heartbeat row"}

    return {
        "found": True,
        "connected": bool(row[0]),
        "account": row[1],
        "last_sync_at": row[2],
        "age_seconds": _age_seconds(row[2]),
        "error": row[3],
    }


async def _latest_daily_candidate_timestamp(db: aiosqlite.Connection) -> str | None:
    if not await _table_exists(db, "daily_candidates"):
        return None
    async with db.execute("SELECT MAX(created_at) FROM daily_candidates") as cursor:
        row = await cursor.fetchone()
    return row[0] if row and row[0] else None


def _market_data_refresh_payload(source: str, refreshed_at: str, *, symbol: str | None = None, metadata: dict | None = None) -> dict:
    payload = {"source": source, "refreshed_at": refreshed_at}
    if symbol:
        payload["symbol"] = symbol
    if metadata:
        payload["metadata"] = metadata
    return payload


def _apply_market_data_refresh_to_status(status: dict, payload: dict, refreshed_at: str) -> tuple[dict, bool]:
    stale_data = dict(status.get("stale_data") or {})
    reasons = list(status.get("blocking_reasons") or [])
    had_stale_market_data = bool(stale_data.get("market_data")) or any(
        str(reason).startswith(MARKET_DATA_STALE_REASON_PREFIX)
        for reason in reasons
    )

    reasons = [
        reason
        for reason in reasons
        if not str(reason).startswith(MARKET_DATA_STALE_REASON_PREFIX)
    ]
    stale_data["market_data"] = False

    status.update({
        "last_market_data_at": refreshed_at,
        "last_market_data_age_seconds": 0,
        "last_market_data_refresh_source": payload,
        "market_data_feed_active": True,
        "stale_data": stale_data,
        "blocking_reasons": reasons,
        "trading_blocked": len(reasons) > 0,
    })
    status["healthy"] = bool(status.get("tws_connected")) and not status["trading_blocked"]
    return status, had_stale_market_data


async def _refresh_cached_watchdog_status(payload: dict, refreshed_at: str) -> bool:
    raw = await database.get_app_state(WATCHDOG_STATUS_KEY)
    if not raw:
        return False
    try:
        status = json.loads(raw)
    except Exception:
        return False

    status, stale_transition_cleared = _apply_market_data_refresh_to_status(status, payload, refreshed_at)
    await database.set_app_state(WATCHDOG_STATUS_KEY, _json(status))
    if stale_transition_cleared:
        await _clear_alert("stale_market_data")
    return stale_transition_cleared


def _refresh_cached_watchdog_status_sync(payload: dict, refreshed_at: str) -> bool:
    with closing(sqlite3.connect(config.DB_PATH)) as db:
        db.execute(database.CREATE_APP_STATE)
        row = db.execute("SELECT value FROM app_state WHERE key = ?", (WATCHDOG_STATUS_KEY,)).fetchone()
        if not row:
            return False
        try:
            status = json.loads(row[0])
        except Exception:
            return False

        status, stale_transition_cleared = _apply_market_data_refresh_to_status(status, payload, refreshed_at)
        db.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (WATCHDOG_STATUS_KEY, _json(status)),
        )
        if stale_transition_cleared:
            db.execute("DELETE FROM app_state WHERE key = ?", (f"{WATCHDOG_ALERT_STATE_PREFIX}stale_market_data",))
        db.commit()
    return stale_transition_cleared


async def refresh_market_data_timestamp(source: str, *, symbol: str | None = None, metadata: dict | None = None) -> str:
    refreshed_at = now_iso()
    payload = _market_data_refresh_payload(source, refreshed_at, symbol=symbol, metadata=metadata)
    await database.set_app_state(LAST_MARKET_DATA_AT_KEY, refreshed_at)
    await database.set_app_state(LAST_MARKET_DATA_REFRESH_SOURCE_KEY, _json(payload))
    stale_transition_cleared = await _refresh_cached_watchdog_status(payload, refreshed_at)
    log.info(
        "MARKET DATA refresh | source=%s symbol=%s refreshed_at=%s stale_transition_cleared=%s",
        source,
        symbol,
        refreshed_at,
        stale_transition_cleared,
    )
    if stale_transition_cleared:
        log.info("WATCHDOG stale market-data transition cleared by source=%s refreshed_at=%s", source, refreshed_at)
    return refreshed_at


def refresh_market_data_timestamp_sync(source: str, *, symbol: str | None = None, metadata: dict | None = None) -> str:
    refreshed_at = now_iso()
    payload = _market_data_refresh_payload(source, refreshed_at, symbol=symbol, metadata=metadata)
    database.set_app_state_sync(LAST_MARKET_DATA_AT_KEY, refreshed_at)
    database.set_app_state_sync(LAST_MARKET_DATA_REFRESH_SOURCE_KEY, _json(payload))
    stale_transition_cleared = _refresh_cached_watchdog_status_sync(payload, refreshed_at)
    log.info(
        "MARKET DATA refresh | source=%s symbol=%s refreshed_at=%s stale_transition_cleared=%s",
        source,
        symbol,
        refreshed_at,
        stale_transition_cleared,
    )
    if stale_transition_cleared:
        log.info("WATCHDOG stale market-data transition cleared by source=%s refreshed_at=%s", source, refreshed_at)
    return refreshed_at


async def _read_market_data_timestamp(db: aiosqlite.Connection) -> tuple[str | None, dict | None]:
    explicit_at = await database.get_app_state(LAST_MARKET_DATA_AT_KEY)
    source_raw = await database.get_app_state(LAST_MARKET_DATA_REFRESH_SOURCE_KEY)
    source_payload = None
    if source_raw:
        try:
            source_payload = json.loads(source_raw)
        except Exception:
            source_payload = {"source": str(source_raw)}
    candidate_at = await _latest_daily_candidate_timestamp(db)

    values = [v for v in (explicit_at, candidate_at) if parse_dt(v)]
    latest_at = max(values, key=lambda v: parse_dt(v)) if values else None
    if latest_at and latest_at != explicit_at and candidate_at == latest_at:
        source_payload = {"source": "daily_candidates", "refreshed_at": latest_at}
    return latest_at, source_payload


async def _read_live_position_tracking_state() -> dict:
    raw = await database.get_app_state("live_position_tracker_status")
    open_positions = await database.get_open_positions()
    if not raw:
        return {
            "open_position_count": len(open_positions),
            "tracked_count": 0,
            "tracked_symbols": [],
            "last_refresh_at": None,
            "last_refresh_age_seconds": None,
            "healthy": len(open_positions) == 0,
            "source": "live_position_tracker",
        }
    try:
        status = json.loads(raw)
    except Exception as exc:
        return {
            "open_position_count": len(open_positions),
            "tracked_count": 0,
            "tracked_symbols": [],
            "last_refresh_at": None,
            "last_refresh_age_seconds": None,
            "healthy": False,
            "source": "live_position_tracker",
            "error": f"status unreadable: {exc}",
        }
    status["open_position_count"] = len(open_positions)
    status["last_refresh_age_seconds"] = _age_seconds(status.get("last_refresh_at"))
    status.setdefault("source", "live_position_tracker")
    return status


async def _read_observed_state() -> dict:
    async with aiosqlite.connect(config.DB_PATH) as db:
        heartbeat = await _get_tws_heartbeat(db)
        market_data_at, market_data_source = await _read_market_data_timestamp(db)

    last_mirror_success_at = await database.get_app_state("tws_mirror_last_success_at")
    last_execution_success_at = await database.get_app_state("execution_sync_last_success_at")
    live_position_tracking = await _read_live_position_tracking_state()

    return {
        "tws_connected": bool(is_ib_connected() or heartbeat.get("connected")),
        "shared_ib_connected": bool(is_ib_connected()),
        "heartbeat": heartbeat,
        "last_tws_mirror_sync_at": last_mirror_success_at,
        "last_tws_mirror_sync_age_seconds": _age_seconds(last_mirror_success_at),
        "last_execution_sync_at": last_execution_success_at,
        "last_execution_sync_age_seconds": _age_seconds(last_execution_success_at),
        "last_market_data_at": market_data_at,
        "last_market_data_age_seconds": _age_seconds(market_data_at),
        "last_market_data_refresh_source": market_data_source,
        "market_data_feed_active": market_data_at is not None and (_age_seconds(market_data_at) is None or _age_seconds(market_data_at) <= _thresholds()["market_data_stale_seconds"]),
        "live_position_tracking": live_position_tracking,
        "last_position_tracking_at": live_position_tracking.get("last_refresh_at"),
        "last_position_tracking_age_seconds": live_position_tracking.get("last_refresh_age_seconds"),
    }


async def _load_previous_status() -> dict:
    raw = await database.get_app_state(WATCHDOG_STATUS_KEY)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


async def _send_alert_once(alert_key: str, message: str, *, cooldown_seconds: int) -> None:
    key = f"{WATCHDOG_ALERT_STATE_PREFIX}{alert_key}"
    raw = await database.get_app_state(key)
    if raw:
        try:
            payload = json.loads(raw)
            age = _age_seconds(payload.get("sent_at"))
            if age is not None and age < cooldown_seconds:
                return
        except Exception:
            pass

    sent = await asyncio.to_thread(send_watchdog_alert, message)
    await database.set_app_state(key, _json({"sent_at": now_iso(), "message": message, "sent": sent}))


async def _clear_alert(alert_key: str) -> None:
    await database.delete_app_state(f"{WATCHDOG_ALERT_STATE_PREFIX}{alert_key}")


def _next_backoff(previous: dict, thresholds: dict) -> int:
    last = previous.get("reconnect_backoff_seconds")
    try:
        last = int(last) if last else 0
    except Exception:
        last = 0
    base = max(1, int(thresholds["reconnect_backoff_seconds"]))
    return min(MAX_RECONNECT_BACKOFF_SECONDS, base if last <= 0 else max(base, last * 2))


def _reconnect_due(previous: dict, thresholds: dict) -> bool:
    last_attempt_age = _age_seconds(previous.get("last_reconnect_attempt_at"))
    if last_attempt_age is None:
        return True
    backoff = int(previous.get("reconnect_backoff_seconds") or thresholds["reconnect_backoff_seconds"])
    return last_attempt_age >= backoff


def _attempt_reconnect_sync() -> dict:
    try:
        disconnect_ib_sync()
    except Exception:
        pass

    try:
        ib = get_ib_sync()
        connected = bool(ib.isConnected())
        return {"ok": connected, "result": "connected" if connected else "not_connected", "error": None}
    except Exception as exc:
        return {"ok": False, "result": "failed", "error": str(exc)}


async def _maybe_reconnect(status: dict, previous: dict, thresholds: dict) -> dict:
    if status["tws_connected"]:
        status["reconnect_backoff_seconds"] = thresholds["reconnect_backoff_seconds"]
        return status

    if not _reconnect_due(previous, thresholds):
        status["last_reconnect_attempt_at"] = previous.get("last_reconnect_attempt_at")
        status["last_reconnect_result"] = previous.get("last_reconnect_result")
        status["reconnect_backoff_seconds"] = previous.get("reconnect_backoff_seconds") or thresholds["reconnect_backoff_seconds"]
        return status

    status["last_reconnect_attempt_at"] = now_iso()
    status["reconnect_backoff_seconds"] = _next_backoff(previous, thresholds)
    log.warning("WATCHDOG reconnect attempt started")
    result = await asyncio.to_thread(_attempt_reconnect_sync)
    status["last_reconnect_result"] = result
    status["tws_connected"] = bool(result.get("ok") or is_ib_connected())
    status["shared_ib_connected"] = bool(is_ib_connected())
    log.warning("WATCHDOG reconnect attempt result: %s", result)
    return status


def _blocking_reasons(status: dict, thresholds: dict) -> list[str]:
    reasons: list[str] = []
    if not status.get("tws_connected"):
        reasons.append("TWS/API disconnected")

    mirror_age = status.get("last_tws_mirror_sync_age_seconds")
    if status.get("last_tws_mirror_sync_at") is None:
        reasons.append("No successful TWS mirror sync timestamp")
    elif mirror_age is not None and mirror_age > thresholds["mirror_stale_seconds"]:
        reasons.append(f"TWS mirror sync stale ({mirror_age}s)")

    execution_age = status.get("last_execution_sync_age_seconds")
    if status.get("last_execution_sync_at") is None:
        reasons.append("No successful execution sync timestamp")
    elif execution_age is not None and execution_age > thresholds["execution_stale_seconds"]:
        reasons.append(f"Execution sync stale ({execution_age}s)")

    market_age = status.get("last_market_data_age_seconds")
    if status.get("last_market_data_at") is not None and market_age is not None and market_age > thresholds["market_data_stale_seconds"]:
        reasons.append(f"{MARKET_DATA_STALE_REASON_PREFIX} ({market_age}s)")

    live_tracking = status.get("live_position_tracking") or {}
    open_count = int(live_tracking.get("open_position_count") or 0)
    tracked_count = int(live_tracking.get("tracked_count") or 0)
    tracking_age = live_tracking.get("last_refresh_age_seconds")
    if open_count > 0:
        if not live_tracking.get("last_refresh_at"):
            reasons.append("No live position tracking refresh for open positions")
        elif tracking_age is not None and tracking_age > thresholds["position_tracking_stale_seconds"]:
            reasons.append(f"Live position tracking stale ({tracking_age}s)")
        if tracked_count < open_count:
            reasons.append(f"Live position tracker missing open positions ({tracked_count}/{open_count})")
        if live_tracking.get("healthy") is False:
            reasons.append("Live position tracking unhealthy")

    return reasons


async def _apply_circuit_breaker(status: dict, thresholds: dict) -> None:
    disconnected_since = status.get("disconnected_since")
    disconnected_age = _age_seconds(disconnected_since)
    if disconnected_since and disconnected_age is not None and disconnected_age >= thresholds["disconnect_circuit_seconds"]:
        circuit = await get_circuit_breaker_state()
        if not circuit.get("tripped") or circuit.get("source") != WATCHDOG_CIRCUIT_SOURCE:
            await trip_circuit_breaker(
                f"Prolonged TWS/API disconnect ({disconnected_age}s)",
                source=WATCHDOG_CIRCUIT_SOURCE,
                details={"disconnected_since": disconnected_since, "disconnect_age_seconds": disconnected_age},
            )
            status["circuit_breaker_tripped"] = True
            status["circuit_breaker_reason"] = f"Prolonged TWS/API disconnect ({disconnected_age}s)"
            status["circuit_breaker_tripped_at"] = now_iso()
        return

    if status.get("healthy"):
        circuit = await get_circuit_breaker_state()
        if circuit.get("tripped") and circuit.get("source") == WATCHDOG_CIRCUIT_SOURCE:
            await auto_clear_recoverable_circuit_breaker(
                "Watchdog healthy after reconnect and sync validation",
                source="watchdog.run_watchdog_once",
                force=True,
            )
            status["circuit_breaker_auto_recovered"] = True


async def run_watchdog_once() -> dict:
    thresholds = _thresholds()
    previous = await _load_previous_status()
    status = await _read_observed_state()
    status.update({
        "last_heartbeat_at": now_iso(),
        "thresholds": thresholds,
    })

    if status.get("tws_connected"):
        status["disconnected_since"] = None
        await _clear_alert("tws_disconnected")
    else:
        status["disconnected_since"] = previous.get("disconnected_since") or now_iso()
        await _send_alert_once(
            "tws_disconnected",
            f"🔴 TWS/API disconnect detected at {status['disconnected_since']}",
            cooldown_seconds=thresholds["alert_cooldown_seconds"],
        )

    status = await _maybe_reconnect(status, previous, thresholds)

    if status.get("tws_connected") and not previous.get("tws_connected"):
        await _send_alert_once(
            "reconnect_succeeded",
            f"🟢 TWS/API reconnect succeeded at {now_iso()}",
            cooldown_seconds=thresholds["alert_cooldown_seconds"],
        )
    if status.get("tws_connected"):
        await _clear_alert("tws_disconnected")

    status["blocking_reasons"] = _blocking_reasons(status, thresholds)
    status["trading_blocked"] = len(status["blocking_reasons"]) > 0
    status["stale_data"] = {
        "tws_mirror": any("TWS mirror sync stale" in r or "No successful TWS mirror" in r for r in status["blocking_reasons"]),
        "execution_sync": any("Execution sync stale" in r or "No successful execution" in r for r in status["blocking_reasons"]),
        "market_data": any("Market data stale" in r for r in status["blocking_reasons"]),
        "live_position_tracking": any("live position" in r.lower() for r in status["blocking_reasons"]),
    }
    status["market_data_feed_active"] = not status["stale_data"]["market_data"] and status.get("last_market_data_at") is not None
    status.setdefault("circuit_breaker_auto_recovered", False)
    status["healthy"] = bool(status.get("tws_connected")) and not status["trading_blocked"]

    if status["stale_data"]["market_data"]:
        reason = next((r for r in status["blocking_reasons"] if "Market data stale" in r), "Market data stale")
        await _send_alert_once(
            "stale_market_data",
            f"🟠 Watchdog blocked trading: {reason}",
            cooldown_seconds=thresholds["alert_cooldown_seconds"],
        )
    else:
        await _clear_alert("stale_market_data")

    await _apply_circuit_breaker(status, thresholds)
    circuit = await get_circuit_breaker_state()
    status["circuit_breaker"] = circuit
    await database.set_app_state(WATCHDOG_STATUS_KEY, _json(status))

    log.info(
        "WATCHDOG heartbeat | connected=%s blocked=%s reasons=%s",
        status.get("tws_connected"),
        status.get("trading_blocked"),
        status.get("blocking_reasons"),
    )
    return status


async def get_watchdog_status() -> dict:
    raw = await database.get_app_state(WATCHDOG_STATUS_KEY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return await run_watchdog_once()


def _sync_table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def get_watchdog_status_sync() -> dict:
    with closing(sqlite3.connect(config.DB_PATH)) as db:
        if not _sync_table_exists(db, "app_state"):
            return {"trading_blocked": False, "blocking_reasons": [], "status_unavailable": True}
        row = db.execute("SELECT value FROM app_state WHERE key = ?", (WATCHDOG_STATUS_KEY,)).fetchone()
    if not row:
        return {"trading_blocked": True, "blocking_reasons": ["Watchdog has not reported status"]}
    try:
        return json.loads(row[0])
    except Exception as exc:
        return {"trading_blocked": True, "blocking_reasons": [f"Watchdog status unreadable: {exc}"]}


def require_watchdog_order_allowed(action: str = "Trading") -> None:
    status = get_watchdog_status_sync()
    if not status.get("trading_blocked"):
        return
    reasons = status.get("blocking_reasons") or ["Watchdog blocks trading"]
    raise RuntimeError(f"{action} blocked by watchdog: {'; '.join(str(r) for r in reasons)}")

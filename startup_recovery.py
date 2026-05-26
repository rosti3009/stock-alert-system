from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import account_sync
import database
import execution_sync
import broker_sync
import reconciliation_engine
import reconciliation_lifecycle
import watchdog
from execution_quality import evaluate_execution_quality
from circuit_breaker import (
    auto_clear_recoverable_circuit_breaker,
    get_circuit_breaker_state,
    is_auto_recoverable_trip,
    reset_ibkr_error_count,
    trip_circuit_breaker,
    validate_buying_power,
    validate_drawdown,
    validate_equity,
)
from reconciliation import adopt_tws_positions_as_baseline, close_db_positions_flat_in_tws, run_reconciliation_once
from tws_mirror import run_tws_mirror_once

log = logging.getLogger(__name__)

STARTUP_RECOVERY_STATUS_KEY = "startup_recovery_status"
STARTUP_RECOVERY_PASSED_KEY = "startup_recovery_passed"
CRITICAL_RECONCILIATION_SEVERITIES = {"HIGH", "CRITICAL"}
STEP_TIMEOUT_SECONDS = 12


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_payload(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _account_value(rows: list[dict], tag: str) -> float | None:
    for row in rows:
        if row.get("tag") == tag:
            try:
                return float(row.get("value"))
            except Exception:
                return None
    return None


def _critical_issues(reconciliation: dict) -> list[dict]:
    return [
        issue for issue in reconciliation.get("issues", [])
        if str(issue.get("severity", "")).upper() in CRITICAL_RECONCILIATION_SEVERITIES
    ]




def _is_timeout_broker_sync_step(step_item: dict) -> bool:
    if (step_item or {}).get("name") != "broker_source_of_truth_sync":
        return False
    error = str((step_item or {}).get("error") or "")
    return "timed out" in error.lower() or "timeout" in error.lower()


def _structured_timeout_error(step_item: dict, timeout_seconds: int) -> dict:
    return {
        "error_type": "timeout",
        "message": str((step_item or {}).get("error") or f"broker sync timed out after {timeout_seconds}s"),
        "timeout_seconds": timeout_seconds,
    }
async def save_startup_recovery_status(status: dict) -> None:
    await database.set_app_state(STARTUP_RECOVERY_STATUS_KEY, _json_payload(status))
    await database.set_app_state(STARTUP_RECOVERY_PASSED_KEY, "true" if status.get("ok") else "false")


async def get_startup_recovery_status() -> dict:
    raw = await database.get_app_state(STARTUP_RECOVERY_STATUS_KEY)
    if raw:
        try:
            status = json.loads(raw)
            status["circuit_breaker"] = await get_circuit_breaker_state()
            return status
        except Exception:
            pass
    return {
        "ok": False,
        "state": "NOT_RUN",
        "reason": "Startup recovery has not run",
        "steps": [],
        "checked_at": None,
        "circuit_breaker": await get_circuit_breaker_state(),
    }


async def startup_recovery_passed() -> bool:
    return str(await database.get_app_state(STARTUP_RECOVERY_PASSED_KEY, "false")).lower() == "true"




async def build_timeout_status(timeout_seconds: int) -> dict:
    try:
        circuit = await get_circuit_breaker_state()
    except Exception:
        circuit = {"tripped": False}
    status = {
        "ok": False,
        "state": "FAILED",
        "reason": f"startup recovery timed out after {timeout_seconds}s",
        "steps": [],
        "checked_at": now_iso(),
        "timeout": True,
        "circuit_breaker": circuit,
    }
    try:
        await save_startup_recovery_status(status)
    except Exception:
        pass
    return status

async def run_startup_recovery() -> dict:
    """Run the blocking startup recovery sequence required before auto trading."""
    await database.init_db()
    steps: list[dict] = []

    async def step(name: str, fn):
        started = now_iso()
        try:
            result = await asyncio.wait_for(fn(), timeout=STEP_TIMEOUT_SECONDS)
            item = {"name": name, "ok": True, "started_at": started, "finished_at": now_iso(), "result": result}
            steps.append(item)
            return result
        except Exception as exc:
            item = {"name": name, "ok": False, "started_at": started, "finished_at": now_iso(), "error": str(exc)}
            steps.append(item)
            raise

    status = {"ok": False, "state": "RUNNING", "reason": None, "steps": steps, "checked_at": now_iso()}
    await save_startup_recovery_status(status)

    try:
        broker_snapshot = await step("broker_source_of_truth_sync", broker_sync.run_broker_sync_once)
        await step("save_broker_snapshot", lambda: database.save_broker_sync_snapshot(broker_snapshot))
        if not broker_snapshot.get("connected"):
            raise RuntimeError(f"TWS connection failed: {(broker_snapshot.get('errors') or [None])[0]}")

        account_snapshot = await step("sync_account_open_orders_executions", account_sync.run_account_sync_once)
        if not account_snapshot.get("connected"):
            raise RuntimeError(f"Account sync failed: {account_snapshot.get('error')}")

        execution_result = await step("sync_executions_and_commissions", execution_sync.sync_executions)
        await step("adopt_missing_tws_positions", adopt_tws_positions_as_baseline)
        await step("close_stale_db_positions", lambda: close_db_positions_flat_in_tws(dry_run=False))
        reconciliation = await step("reconcile_db", lambda: reconciliation_engine.run_reconciliation(broker_snapshot))

        buying_power = (account_snapshot.get("equity") or {}).get("buying_power")
        if buying_power is None:
            buying_power = _account_value(account_snapshot.get("account_summary", []), "BuyingPower")
        equity = (account_snapshot.get("equity") or {}).get("net_liquidation")
        if equity is None:
            equity = _account_value(account_snapshot.get("account_summary", []), "NetLiquidation")

        await step("validate_buying_power", lambda: validate_buying_power(buying_power, source="startup_recovery"))
        await step("validate_equity", lambda: validate_equity(equity, source="startup_recovery"))
        await step("validate_drawdown", lambda: validate_drawdown(source="startup_recovery"))

        candidates = await step("validate_startup_candidates", _validate_startup_candidates)

        critical = _critical_issues(reconciliation)
        if critical:
            await trip_circuit_breaker(
                f"Critical reconciliation mismatches: {len(critical)}",
                source="startup_recovery",
                details={"critical_issues": critical},
            )
            raise RuntimeError(f"Critical reconciliation mismatches: {len(critical)}")

        circuit = await get_circuit_breaker_state()
        if circuit.get("tripped") and is_auto_recoverable_trip(circuit):
            recovery = await auto_clear_recoverable_circuit_breaker(
                "Startup recovery passed with fresh account, execution, mirror, and reconciliation state",
                source="startup_recovery.run_startup_recovery",
            )
            if recovery.get("cleared"):
                circuit = await get_circuit_breaker_state()

        if circuit.get("tripped"):
            raise RuntimeError(circuit.get("reason") or "Circuit breaker is tripped")

        await reset_ibkr_error_count()
        status = {
            "ok": True,
            "state": "PASSED",
            "reason": None,
            "steps": steps,
            "execution_sync": execution_result,
            "reconciliation": reconciliation,
            "startup_candidate_validation": candidates,
            "buying_power": buying_power,
            "equity": equity,
            "checked_at": now_iso(),
            "circuit_breaker": circuit,
        }
        await save_startup_recovery_status(status)
        log.info("Startup recovery passed; auto trading may run")
        return status

    except Exception as exc:
        broker_step = next((x for x in steps if x.get("name") == "broker_source_of_truth_sync"), None)
        watchdog_status = await watchdog.get_watchdog_status()
        reconciliation = await reconciliation_lifecycle.get_reconciliation_status()
        use_cached_fallback = False
        if _is_timeout_broker_sync_step(broker_step):
            last_snapshot = await database.get_latest_broker_sync_snapshot() or {}
            if watchdog_status.get("tws_connected") and bool(last_snapshot):
                use_cached_fallback = True
                broker_step["fallback"] = "cached_snapshot"
                broker_step["timeout_error"] = _structured_timeout_error(broker_step, STEP_TIMEOUT_SECONDS)

        if use_cached_fallback and int(reconciliation.get("issues_count") or 0) == 0:
            circuit = await get_circuit_breaker_state()
        else:
            circuit = await trip_circuit_breaker(
                str(exc),
                source="startup_recovery",
                details={"steps": steps},
            )
        status = {
            "ok": False,
            "state": "FAILED",
            "reason": str(exc),
            "broker_source_of_truth_sync_error": _structured_timeout_error(broker_step, STEP_TIMEOUT_SECONDS) if _is_timeout_broker_sync_step(broker_step or {}) else None,
            "steps": steps,
            "checked_at": now_iso(),
            "circuit_breaker": circuit,
        }
        if use_cached_fallback and int(reconciliation.get("issues_count") or 0) == 0:
            status.update({"ok": True, "state": "PASSED_WITH_FALLBACK", "reason": "broker sync timed out; used cached broker snapshot"})
        await save_startup_recovery_status(status)
        log.warning("Startup recovery failed: %s", exc)
        return status


async def _validate_startup_candidates(limit: int = 200) -> dict:
    """Validate latest startup candidates without tripping startup recovery."""
    rows = await database.get_latest_candidates(limit=limit)
    if not rows:
        return {"ok": True, "validated": 0, "soft_rejected": 0, "warnings": []}

    soft_rejected = 0
    fallback_events = 0
    warnings: list[str] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper() or None
        result = evaluate_execution_quality(row=row, symbol=symbol)

        for event_type in result.get("journal_events") or []:
            if event_type in {"EXECUTION_VOLUME_FALLBACK_USED", "EXECUTION_DOLLAR_VOLUME_COMPUTED"}:
                fallback_events += 1
                await database.safe_record_trade_journal_event({
                    "symbol": symbol,
                    "event_type": "STARTUP_RECOVERY_VOLUME_FALLBACK_USED",
                    "decision": "WARNING",
                    "reason": event_type,
                    "source_module": "startup_recovery",
                    "raw_payload": {"execution_quality": result},
                })

        low_liquidity = "low_liquidity" in (result.get("block_categories") or [])
        if low_liquidity:
            soft_rejected += 1
            reason = result.get("blocked_buy_reason") or "Startup recovery liquidity soft reject"
            warnings.append(f"{symbol}: {reason}")
            await database.safe_record_trade_journal_event({
                "symbol": symbol,
                "event_type": "STARTUP_RECOVERY_SOFT_REJECT",
                "decision": "SOFT_REJECTED",
                "reason": reason,
                "source_module": "startup_recovery",
                "raw_payload": {"execution_quality": result},
            })

    return {
        "ok": True,
        "validated": len(rows),
        "soft_rejected": soft_rejected,
        "fallback_events": fallback_events,
        "warnings": warnings[:20],
    }

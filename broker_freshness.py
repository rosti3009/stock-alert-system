from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import config


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def broker_snapshot_freshness(snapshot: dict[str, Any] | None, *, max_age_seconds: int | None = None) -> dict[str, Any]:
    snapshot = snapshot or {}
    max_age = int(max_age_seconds or getattr(config, "BROKER_SNAPSHOT_MAX_AGE_SECONDS", 60))
    synced_at = _parse_dt(snapshot.get("synced_at") or snapshot.get("received_at"))
    age = None
    if synced_at:
        age = max(0.0, (datetime.now(timezone.utc) - synced_at).total_seconds())
    connected = bool(snapshot.get("connected"))
    fresh = bool(snapshot) and connected and age is not None and age <= max_age
    reasons: list[str] = []
    if not snapshot:
        reasons.append("No broker snapshot has been pushed by the local gateway")
    if snapshot and not connected:
        reasons.append("Broker snapshot reports disconnected")
    if snapshot and age is None:
        reasons.append("Broker snapshot is missing synced_at")
    if age is not None and age > max_age:
        reasons.append(f"Broker snapshot is stale ({age:.1f}s > {max_age}s)")
    return {
        "fresh": fresh,
        "connected": connected,
        "age_seconds": age,
        "max_age_seconds": max_age,
        "reasons": reasons,
    }


def is_fresh_local_gateway_snapshot(snapshot: dict[str, Any] | None, *, max_age_seconds: int | None = None) -> bool:
    snapshot = snapshot or {}
    return bool(
        str(snapshot.get("source") or "").upper() == "LOCAL_GATEWAY_PUSH"
        and broker_snapshot_freshness(snapshot, max_age_seconds=max_age_seconds).get("fresh")
    )


def evaluate_broker_freshness(watchdog_status: dict[str, Any] | None, broker_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    watchdog_status = watchdog_status or {}
    broker_snapshot = broker_snapshot or {}
    snapshot_freshness = broker_snapshot_freshness(broker_snapshot)
    stale_data = watchdog_status.get("stale_data") or {}

    broker_sync_connected = bool(snapshot_freshness.get("connected"))
    broker_sync_fresh = bool(snapshot_freshness.get("fresh"))
    local_gateway_connected = is_fresh_local_gateway_snapshot(broker_snapshot)
    direct_connected = bool(
        watchdog_status.get("shared_ib_connected")
        or (watchdog_status.get("heartbeat") or {}).get("connected")
        or (watchdog_status.get("tws_connected") and not local_gateway_connected)
    )
    effective_connected = bool(direct_connected or local_gateway_connected or broker_sync_fresh)

    tws_mirror_fresh = (not bool(stale_data.get("tws_mirror"))) or broker_sync_fresh
    execution_sync_fresh = (not bool(stale_data.get("execution_sync"))) or broker_sync_fresh

    if local_gateway_connected:
        freshness_source = "LOCAL_GATEWAY_PUSH"
    elif broker_sync_connected and broker_sync_fresh:
        freshness_source = "broker_sync_source_of_truth"
    elif direct_connected:
        freshness_source = "DIRECT_IBKR"
    else:
        freshness_source = "DISCONNECTED"

    return {
        "broker_sync_connected": broker_sync_connected,
        "broker_sync_fresh": broker_sync_fresh,
        "local_gateway_connected": local_gateway_connected,
        "direct_connected": direct_connected,
        "effective_connected": effective_connected,
        "broker_snapshot_age_seconds": snapshot_freshness.get("age_seconds"),
        "broker_snapshot_reasons": snapshot_freshness.get("reasons") or [],
        "tws_mirror_fresh": tws_mirror_fresh,
        "execution_sync_fresh": execution_sync_fresh,
        "effective_connection_healthy": effective_connected and tws_mirror_fresh and execution_sync_fresh,
        "freshness_source": freshness_source,
    }

from __future__ import annotations

from typing import Any


def evaluate_broker_freshness(watchdog_status: dict[str, Any] | None, broker_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    watchdog_status = watchdog_status or {}
    broker_snapshot = broker_snapshot or {}
    stale_data = watchdog_status.get("stale_data") or {}

    broker_sync_connected = bool(broker_snapshot.get("connected"))
    broker_sync_fresh = bool(broker_snapshot.get("synced_at")) and not bool(stale_data.get("broker_sync"))
    tws_mirror_fresh = (not bool(stale_data.get("tws_mirror"))) or (broker_sync_connected and broker_sync_fresh)
    execution_sync_fresh = (not bool(stale_data.get("execution_sync"))) or (broker_sync_connected and broker_sync_fresh)

    freshness_source = "watchdog"
    if broker_sync_connected and broker_sync_fresh:
        freshness_source = "broker_sync_source_of_truth"

    return {
        "broker_sync_connected": broker_sync_connected,
        "broker_sync_fresh": broker_sync_fresh,
        "tws_mirror_fresh": tws_mirror_fresh,
        "execution_sync_fresh": execution_sync_fresh,
        "effective_connection_healthy": broker_sync_connected and broker_sync_fresh and tws_mirror_fresh and execution_sync_fresh,
        "freshness_source": freshness_source,
    }

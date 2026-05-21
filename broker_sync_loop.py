from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import broker_sync
import database
import reconciliation_engine


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BrokerSyncLoop:
    def __init__(self, interval_seconds: int = 15, max_failures: int = 3):
        self.interval_seconds = max(10, min(20, int(interval_seconds)))
        self.max_failures = max_failures
        self.failure_count = 0
        self.running = False
        self._sync_lock = asyncio.Lock()

    async def sync_once(self) -> dict:
        if self._sync_lock.locked():
            return {"ok": False, "skipped": True, "reason": "broker sync already running"}

        async with self._sync_lock:
            started = time.perf_counter()
            snap = await broker_sync.run_broker_sync_once()
            latency_ms = round((time.perf_counter()-started)*1000,2)
            await database.set_app_state('broker_sync_heartbeat', now_iso())
            await database.set_app_state('broker_sync_latency_ms', str(latency_ms))
            if not snap.get('ok'):
                self.failure_count += 1
                await database.safe_record_trade_journal_event({'symbol':'SYSTEM','event_type':'BROKER_SYNC_FAILURE','decision':'FAILED','reason':';'.join(snap.get('errors',[]) or ['sync_failed']),'source_module':'broker_sync_loop','raw_payload':snap})
                if self.failure_count >= self.max_failures:
                    await database.set_app_state('auto_trading_enabled','false')
                return {'ok': False, 'latency_ms': latency_ms, 'snapshot': snap}
            self.failure_count = 0
            await reconciliation_engine.run_reconciliation(snap)
            await database.set_app_state('auto_trading_enabled','true')
            return {'ok': True, 'latency_ms': latency_ms, 'snapshot': snap}

    async def run(self) -> None:
        self.running = True
        while self.running:
            try:
                await self.sync_once()
            except Exception as exc:
                await database.safe_record_trade_journal_event({'symbol':'SYSTEM','event_type':'BROKER_SYNC_LOOP_CRASH_SAFE','decision':'RECOVERED','reason':str(exc),'source_module':'broker_sync_loop'})
            await asyncio.sleep(self.interval_seconds)

    def stop(self) -> None:
        self.running = False

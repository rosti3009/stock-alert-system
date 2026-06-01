from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

from ibkr_asyncio_compat import ensure_event_loop

ensure_event_loop()

import ib_insync

import config

log = logging.getLogger(__name__)

T = TypeVar("T")

_ib: Any | None = None
_lock = threading.RLock()
_last_heartbeat_at: datetime | None = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _create_ib() -> Any:
    ib = ib_insync.IB()

    def _on_connected() -> None:
        global _last_heartbeat_at
        _last_heartbeat_at = _now_utc()
        log.info("Shared IBKR session connected event")

    def _on_disconnected() -> None:
        log.warning("Shared IBKR session disconnected event")

    try:
        ib.connectedEvent += _on_connected
        ib.disconnectedEvent += _on_disconnected
    except Exception:
        pass

    return ib


def get_ib_sync(readonly: bool = False) -> Any:
    global _ib, _last_heartbeat_at
    ensure_event_loop()
    with _lock:
        if _ib is not None and _ib.__class__ is not ib_insync.IB:
            _ib = None

        if _ib is None:
            _ib = _create_ib()

        if not _ib.isConnected():
            log.info("Connecting shared IBKR session...")
            _ib.connect(
                config.IBKR_HOST,
                int(config.IBKR_PORT),
                clientId=int(config.IBKR_CLIENT_ID),
                timeout=15,
                readonly=readonly,
            )
            _last_heartbeat_at = _now_utc()
            log.info("Shared IBKR session connected")

        return _ib


def with_shared_ib_sync(fn: Callable[[Any], T], readonly: bool = False) -> T:
    ib = get_ib_sync(readonly=readonly)
    try:
        result = fn(ib)
        mark_heartbeat()
        return result
    except Exception:
        # one reconnect attempt
        reconnect_ib_sync(readonly=readonly)
        ib = get_ib_sync(readonly=readonly)
        result = fn(ib)
        mark_heartbeat()
        return result


def reconnect_ib_sync(readonly: bool = False) -> Any:
    with _lock:
        if _ib and _ib.isConnected():
            try:
                _ib.disconnect()
            except Exception:
                pass
        return get_ib_sync(readonly=readonly)


def mark_heartbeat() -> None:
    global _last_heartbeat_at
    _last_heartbeat_at = _now_utc()


def heartbeat_age_seconds() -> float | None:
    if _last_heartbeat_at is None:
        return None
    return (_now_utc() - _last_heartbeat_at).total_seconds()


def disconnect_ib_sync() -> None:
    global _ib
    with _lock:
        if _ib and _ib.isConnected():
            log.info("Disconnecting shared IBKR session...")
            _ib.disconnect()
            log.info("Shared IBKR session disconnected")


def is_ib_connected() -> bool:
    return bool(_ib and _ib.isConnected())


async def ensure_ib_connected_async(readonly: bool = False) -> bool:
    await asyncio.to_thread(get_ib_sync, readonly)
    return is_ib_connected()

from __future__ import annotations

import logging
import threading

from ib_insync import IB

import config

log = logging.getLogger(__name__)

_ib: IB | None = None
_lock = threading.RLock()


def get_ib_sync() -> IB:
    global _ib

    with _lock:
        if _ib is None:
            _ib = IB()

        if not _ib.isConnected():
            log.info("Connecting shared IBKR session...")

            _ib.connect(
                config.IBKR_HOST,
                int(config.IBKR_PORT),
                clientId=int(config.IBKR_CLIENT_ID),
                timeout=15,
                readonly=False,
            )

            log.info("Shared IBKR session connected")

        return _ib


def disconnect_ib_sync() -> None:
    global _ib

    with _lock:
        if _ib and _ib.isConnected():
            log.info("Disconnecting shared IBKR session...")
            _ib.disconnect()
            log.info("Shared IBKR session disconnected")


def is_ib_connected() -> bool:
    global _ib

    return bool(_ib and _ib.isConnected())
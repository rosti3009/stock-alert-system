from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ibkr_asyncio_compat import ensure_event_loop

ensure_event_loop()

_IBKR_MANUAL_TESTS = {
    "test_account.py",
    "test_buy.py",
    "test_ib.py",
    "test_ibkr_client.py",
    "test_market_data.py",
    "test_order.py",
    "test_trade_engine.py",
}


def pytest_ignore_collect(collection_path: Any, config: Any) -> bool:
    """Skip manual IBKR/TWS smoke scripts during offline unit test runs."""
    if os.getenv("RUN_IBKR_INTEGRATION_TESTS") == "1":
        return False

    return Path(str(collection_path)).name in _IBKR_MANUAL_TESTS

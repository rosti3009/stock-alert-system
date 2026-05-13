from __future__ import annotations

import asyncio


def ensure_event_loop() -> None:
    """Ensure libraries that expect a default asyncio loop can import safely."""
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

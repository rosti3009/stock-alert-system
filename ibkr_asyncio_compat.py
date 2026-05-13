from __future__ import annotations

import asyncio


def ensure_event_loop() -> asyncio.AbstractEventLoop:
    """Ensure the current thread has an open default asyncio event loop.

    ib_insync asks asyncio for the current thread's default event loop from
    synchronous code paths. Python 3.14 no longer creates that loop
    implicitly, so worker threads must install one before constructing or using
    IBKR clients.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    else:
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

    return loop

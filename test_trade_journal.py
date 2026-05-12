from __future__ import annotations

import asyncio
import os
import tempfile

import database


async def main() -> None:
    original_db_path = database.DB_PATH
    fd, path = tempfile.mkstemp(prefix="trade_journal_", suffix=".db")
    os.close(fd)

    try:
        database.DB_PATH = path
        await database.init_db()

        await database.safe_record_trade_journal_event({
            "symbol": "aapl",
            "event_type": "BUY_CANDIDATE_ACCEPTED",
            "decision": "ACCEPTED",
            "reason": "test insert",
            "source_module": "test_trade_journal",
            "signal_score": 88,
            "weekly_score": 91,
            "market_regime": "Bullish",
            "price": 123.45,
            "quantity": 2,
            "stop_loss": 120,
            "take_profit_1": 130,
            "take_profit_2": 140,
            "risk_percent": 2.5,
            "raw_payload": {"ok": True},
        })

        rows = await database.get_trade_journal(limit=10)

        assert len(rows) == 1, rows
        assert rows[0]["symbol"] == "AAPL", rows[0]
        assert rows[0]["event_type"] == "BUY_CANDIDATE_ACCEPTED", rows[0]
        assert rows[0]["decision"] == "ACCEPTED", rows[0]

        filtered = await database.get_trade_journal(limit=10, symbol="AAPL")
        assert len(filtered) == 1, filtered

        print("trade journal smoke test passed")

    finally:
        database.DB_PATH = original_db_path
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    asyncio.run(main())

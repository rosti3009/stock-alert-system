from __future__ import annotations

import asyncio
import os
import tempfile
from contextlib import contextmanager
from unittest.mock import patch

from fastapi.testclient import TestClient

import database
import main


@contextmanager
def temp_database():
    original_db_path = database.DB_PATH
    fd, path = tempfile.mkstemp(prefix="trade_reviews_", suffix=".db")
    os.close(fd)
    database.DB_PATH = path
    try:
        yield path
    finally:
        database.DB_PATH = original_db_path
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_closed_trade_creates_review():
    async def run():
        with temp_database():
            await database.init_db()
            await database.record_trade_decision({
                "symbol": "AAPL",
                "setup_type": "BREAKOUT",
                "entry_time": "2026-05-15T14:30:00+00:00",
                "entry_price": 100,
                "entry_reason": "BUY candidate passed auto-trading filters",
                "score": 92,
                "rvol": 2.2,
                "vwap_status": "ABOVE_VWAP",
                "breakout_status": "CONFIRMED",
                "market_regime": "Bullish",
            })
            position = await database.add_position({
                "symbol": "AAPL",
                "buy_price": 100,
                "quantity": 2,
                "buy_date": "2026-05-15T14:30:00+00:00",
                "current_price": 110,
            })
            await database.update_position("AAPL", {
                "current_price": 110,
                "profit_amount": 20,
                "profit_percent": 10,
            })
            await database.close_position("AAPL", "TAKE_PROFIT_1")
            reviews = await database.get_trade_reviews(symbol="AAPL")
            return position, reviews

    position, reviews = asyncio.run(run())
    assert len(reviews) == 1
    review = reviews[0]
    assert review["position_id"] == position["id"]
    assert review["symbol"] == "AAPL"
    assert review["entry_reason"] == "BUY candidate passed auto-trading filters"
    assert review["exit_reason"] == "TAKE_PROFIT_1"
    assert review["profit_amount"] == 20
    assert review["hold_minutes"] is not None
    assert "Entered because" in review["review_summary"]
    assert review["lessons"]


def test_rebuild_creates_missing_reviews():
    async def run():
        with temp_database():
            await database.init_db()
            await database.add_position({
                "symbol": "MSFT",
                "buy_price": 200,
                "quantity": 1,
                "buy_date": "2026-05-15T14:00:00+00:00",
                "current_price": 204,
            })
            await database.update_position("MSFT", {"current_price": 204, "profit_amount": 4, "profit_percent": 2})
            await database.close_position("MSFT", "MANUAL_REVIEW_EXIT")
            async with database.aiosqlite.connect(database.DB_PATH) as db:
                await db.execute("DELETE FROM trade_reviews")
                await db.commit()
            result = await database.rebuild_trade_reviews()
            reviews = await database.get_trade_reviews(symbol="MSFT")
            return result, reviews

    result, reviews = asyncio.run(run())
    assert result["rebuilt"] == 1
    assert result["failed"] == 0
    assert len(reviews) == 1
    assert reviews[0]["symbol"] == "MSFT"


def test_missing_analytics_data_does_not_fail():
    async def run():
        with temp_database():
            await database.init_db()
            await database.add_position({
                "symbol": "TSLA",
                "buy_price": 250,
                "quantity": 1,
                "current_price": 245,
            })
            await database.update_position("TSLA", {"current_price": 245, "profit_amount": -5, "profit_percent": -2})
            await database.close_position("TSLA", "STOP_LOSS_HIT")
            return await database.get_trade_reviews(symbol="TSLA")

    reviews = asyncio.run(run())
    assert len(reviews) == 1
    assert reviews[0]["setup_type"] == "UNKNOWN"
    assert reviews[0]["entry_indicators"] is not None
    assert "Entry" in reviews[0]["lessons"][0]["lesson"]


def test_trade_review_endpoints_return_expected_fields():
    with temp_database():
        asyncio.run(database.init_db())
        asyncio.run(database.add_position({"symbol": "NVDA", "buy_price": 100, "quantity": 1, "current_price": 103}))
        asyncio.run(database.update_position("NVDA", {"current_price": 103, "profit_amount": 3, "profit_percent": 3}))
        asyncio.run(database.close_position("NVDA", "TAKE_PROFIT_1"))

        client = TestClient(main.app)
        rows = client.get("/api/trade-reviews").json()
        symbol_rows = client.get("/api/trade-reviews/NVDA").json()
        rebuild = client.post("/api/trade-reviews/rebuild").json()

    assert rows
    assert symbol_rows
    assert rebuild["informational_only"] is True
    assert {
        "symbol",
        "position_id",
        "entry_reason",
        "exit_reason",
        "profit_amount",
        "hold_minutes",
        "lessons",
        "review_summary",
    } <= set(rows[0])


def test_review_layer_does_not_affect_execution_close_flow():
    async def run():
        with temp_database():
            await database.init_db()
            await database.add_position({"symbol": "AMD", "buy_price": 100, "quantity": 1, "current_price": 99})
            with patch.object(database, "upsert_trade_review_for_position", side_effect=RuntimeError("review db unavailable")):
                closed = await database.close_position("AMD", "STOP_LOSS_HIT")
            position = await database.get_position("AMD")
            return closed, position

    closed, position = asyncio.run(run())
    assert closed["status"] == "CLOSED"
    assert position["status"] == "CLOSED"

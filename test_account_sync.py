from __future__ import annotations

import asyncio
import os
import tempfile

import account_sync
import config
import database


async def main() -> None:
    original_config_db_path = config.DB_PATH
    original_database_db_path = database.DB_PATH
    fd, path = tempfile.mkstemp(prefix="account_sync_", suffix=".db")
    os.close(fd)

    try:
        config.DB_PATH = path
        database.DB_PATH = path

        await database.init_db()
        await account_sync.init_account_sync_db()

        snapshot = {
            "connected": True,
            "account": "DU12345",
            "account_summary": [
                {
                    "tag": "NetLiquidation",
                    "value": "100000.50",
                    "currency": "USD",
                    "account": "DU12345",
                },
                {
                    "tag": "BuyingPower",
                    "value": "250000.00",
                    "currency": "USD",
                    "account": "DU12345",
                },
            ],
            "open_orders": [
                {
                    "order_id": 101,
                    "perm_id": 9001,
                    "symbol": "AAPL",
                    "action": "BUY",
                    "order_type": "LMT",
                    "total_quantity": 5,
                    "limit_price": 180.25,
                    "aux_price": 0,
                    "status": "Submitted",
                    "filled": 0,
                    "remaining": 5,
                    "avg_fill_price": 0,
                    "account": "DU12345",
                    "raw_json": "{}",
                }
            ],
            "execution_history": [
                {
                    "exec_id": "E1",
                    "symbol": "MSFT",
                    "side": "BOT",
                    "quantity": 1,
                    "price": 300.0,
                    "order_id": 102,
                    "perm_id": 9002,
                    "account": "DU12345",
                    "exchange": "NYSE",
                    "time": "2026-05-12T00:00:00+00:00",
                    "commission": 0.0,
                    "realized_pnl": 0.0,
                    "raw_json": "{}",
                }
            ],
            "equity": {
                "timestamp": "2026-05-12T00:00:00+00:00",
                "account": "DU12345",
                "net_liquidation": 100000.50,
                "total_cash": 50000.25,
                "buying_power": 250000.00,
                "unrealized_pnl": 12.34,
                "realized_pnl": 56.78,
            },
            "synced_at": "2026-05-12T00:00:00+00:00",
        }

        await account_sync.save_account_snapshot(snapshot)

        summary = await account_sync.get_account_summary()
        orders = await account_sync.get_open_orders()
        executions = await account_sync.get_executions()
        equity = await account_sync.get_equity_curve()

        assert len(summary) == 2, summary
        assert summary[0]["tag"] == "BuyingPower", summary
        assert len(orders) == 1, orders
        assert orders[0]["symbol"] == "AAPL", orders
        assert len(executions) == 1, executions
        assert executions[0]["exec_id"] == "E1", executions
        assert len(equity) == 1, equity
        assert equity[0]["net_liquidation"] == 100000.50, equity

        await database.add_position({
            "symbol": "MSFT",
            "buy_price": 300.0,
            "quantity": 2,
            "reason": "test position",
        })

        status = await account_sync.run_reconciliation_status_check()
        assert status["ok"] is False, status
        assert status["mismatch_count"] == 1, status
        assert status["mismatches"][0]["symbol"] == "MSFT", status

        journal = await database.get_trade_journal(limit=10, symbol="MSFT")
        assert len(journal) == 1, journal
        assert journal[0]["event_type"] == "RECONCILIATION_MISMATCH", journal
        assert journal[0]["source_module"] == "account_sync.run_reconciliation_status_check", journal

        cached = await account_sync.get_reconciliation_status()
        assert cached["mismatch_count"] == 1, cached

        print("account sync smoke test passed")

    finally:
        config.DB_PATH = original_config_db_path
        database.DB_PATH = original_database_db_path
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import asyncio
import importlib.util
import os
import tempfile

import config
import database


async def main() -> None:
    if importlib.util.find_spec("ib_insync") is None:
        print("account sync smoke test skipped: ib_insync is not installed")
        return

    from account_sync import (
        get_execution_history,
        get_latest_account_summary,
        get_open_orders,
        init_account_sync_db,
        run_account_reconciliation_once,
        save_account_snapshot,
    )

    original_config_db_path = config.DB_PATH
    original_database_db_path = database.DB_PATH
    fd, path = tempfile.mkstemp(prefix="account_sync_", suffix=".db")
    os.close(fd)

    try:
        config.DB_PATH = path
        database.DB_PATH = path

        await database.init_db()
        await init_account_sync_db()
        await database.add_position({
            "symbol": "AAPL",
            "buy_price": 100,
            "quantity": 1,
        })

        await save_account_snapshot({
            "timestamp": "2026-05-12T00:00:00+00:00",
            "account": "DU123",
            "account_summary": {
                "timestamp": "2026-05-12T00:00:00+00:00",
                "account": "DU123",
                "net_liquidation": 10000,
                "total_cash_value": 8000,
                "buying_power": 20000,
                "available_funds": 7500,
                "gross_position_value": 2000,
                "excess_liquidity": 9000,
                "maint_margin_req": 1000,
                "unrealized_pnl": 50,
                "realized_pnl": 25,
            },
            "open_orders": [{
                "order_id": 1,
                "perm_id": 11,
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "total_quantity": 10,
                "limit_price": 99,
                "aux_price": 0,
                "status": "Submitted",
                "filled": 0,
                "remaining": 10,
                "avg_fill_price": 0,
                "account": "DU123",
                "submitted_at": "2026-05-12T00:00:00+00:00",
            }],
            "executions": [{
                "exec_id": "E1",
                "symbol": "AAPL",
                "side": "BOT",
                "quantity": 1,
                "fill_price": 100,
                "commission": 1,
                "realized_pnl": 0,
                "order_id": 1,
                "perm_id": 11,
                "account": "DU123",
                "exchange": "SMART",
                "execution_timestamp": "2026-05-12T00:00:01+00:00",
            }],
        })

        summary = await get_latest_account_summary()
        assert summary["net_liquidation"] == 10000, summary

        orders = await get_open_orders()
        assert len(orders) == 1, orders
        assert orders[0]["status"] == "Submitted", orders[0]

        executions = await get_execution_history()
        assert len(executions) == 1, executions
        assert executions[0]["fill_price"] == 100, executions[0]

        reconciliation = await run_account_reconciliation_once()
        assert reconciliation["ok"] is False, reconciliation
        assert reconciliation["issues_count"] >= 1, reconciliation

        journal = await database.get_trade_journal(limit=10)
        assert any(row["event_type"] == "RECONCILIATION_MISMATCH" for row in journal), journal

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

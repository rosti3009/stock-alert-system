from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import ib_insync
from ib_insync import IB  # compatibility for tests patching broker_sync.IB

import config
from tws_connection_manager import with_shared_ib_sync


BROKER_SYNC_CLIENT_ID_OFFSET = 700


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(v, d=0.0):
    try:
        return float(v) if v is not None else d
    except Exception:
        return d


def _i(v, d=0):
    try:
        return int(v) if v is not None else d
    except Exception:
        return d


def _s(v):
    return "" if v is None else str(v)


def fetch_broker_snapshot_sync() -> dict:
    errors = []
    snapshot = {
        "ok": False,
        "connected": False,
        "account": None,
        "synced_at": now_iso(),
        "equity": {
            "net_liquidation": 0.0,
            "total_cash": 0.0,
            "available_funds": 0.0,
            "buying_power": 0.0,
            "currency": "USD",
        },
        "positions": [],
        "open_orders": [],
        "executions": [],
        "errors": errors,
    }

    def _fetch(ib):
        snapshot["connected"] = ib.isConnected()

        if not snapshot["connected"]:
            errors.append("IBKR connect returned disconnected state")
            return snapshot

        accounts = ib.managedAccounts()
        snapshot["account"] = accounts[0] if accounts else None

        summary = ib.accountSummary() or []
        tags = {row.tag: row for row in summary if getattr(row, "tag", None)}

        for key, tag in (
            ("net_liquidation", "NetLiquidation"),
            ("total_cash", "TotalCashValue"),
            ("available_funds", "AvailableFunds"),
            ("buying_power", "BuyingPower"),
        ):
            row = tags.get(tag)
            snapshot["equity"][key] = _f(getattr(row, "value", None), 0.0)

            if row and getattr(row, "currency", None):
                snapshot["equity"]["currency"] = _s(row.currency)

        portfolio_by_symbol = {}

        for portfolio_item in ib.portfolio() or []:
            symbol = _s(getattr(portfolio_item.contract, "symbol", None)).upper()
            if symbol:
                portfolio_by_symbol[symbol] = portfolio_item

        for position in ib.positions() or []:
            symbol = _s(getattr(position.contract, "symbol", None)).upper()
            if not symbol:
                continue

            portfolio_item = portfolio_by_symbol.get(symbol)

            snapshot["positions"].append(
                {
                    "symbol": symbol,
                    "quantity": _f(position.position),
                    "avg_cost": _f(position.avgCost),
                    "market_price": _f(getattr(portfolio_item, "marketPrice", None)),
                    "market_value": _f(getattr(portfolio_item, "marketValue", None)),
                    "unrealized_pnl": _f(getattr(portfolio_item, "unrealizedPNL", None)),
                    "realized_pnl": _f(getattr(portfolio_item, "realizedPNL", None)),
                    "account": _s(getattr(position, "account", None) or snapshot["account"]),
                }
            )

        ib.reqAllOpenOrders()
        ib.sleep(0.5)

        for trade in ib.openTrades() or []:
            contract = trade.contract
            order = trade.order
            status = trade.orderStatus

            snapshot["open_orders"].append(
                {
                    "order_id": _i(order.orderId),
                    "perm_id": _i(order.permId),
                    "symbol": _s(contract.symbol).upper(),
                    "action": _s(order.action).upper(),
                    "order_type": _s(order.orderType),
                    "quantity": _f(order.totalQuantity),
                    "filled_quantity": _f(status.filled),
                    "remaining_quantity": _f(status.remaining),
                    "limit_price": _f(order.lmtPrice),
                    "stop_price": _f(order.auxPrice),
                    "status": _s(status.status),
                    "account": _s(order.account or snapshot["account"]),
                }
            )

        for fill in ib.fills() or []:
            execution = fill.execution
            commission_report = getattr(fill, "commissionReport", None)

            snapshot["executions"].append(
                {
                    "execution_id": _s(execution.execId),
                    "order_id": _i(execution.orderId),
                    "perm_id": _i(execution.permId),
                    "symbol": _s(getattr(execution, "symbol", "")).upper(),
                    "side": _s(execution.side).upper(),
                    "shares": _f(execution.shares),
                    "price": _f(execution.price),
                    "time": _s(execution.time),
                    "account": _s(execution.acctNumber or snapshot["account"]),
                    "commission": _f(getattr(commission_report, "commission", None)),
                }
            )

        snapshot["ok"] = True
        return snapshot

    try:
        # If tests monkeypatch broker_sync.IB, use the patched class directly.
        if IB is not ib_insync.IB:
            ib = IB()
            try:
                ib.connect(
                    config.IBKR_HOST,
                    int(config.IBKR_PORT),
                    clientId=int(config.IBKR_CLIENT_ID),
                    timeout=15,
                    readonly=True,
                )
                return _fetch(ib)
            finally:
                try:
                    ib.disconnect()
                except Exception:
                    pass

        return with_shared_ib_sync(_fetch, readonly=True)

    except Exception:
        ib = None
        try:
            ib = IB()
            ib.connect(
                config.IBKR_HOST,
                int(config.IBKR_PORT),
                clientId=int(config.IBKR_CLIENT_ID) + BROKER_SYNC_CLIENT_ID_OFFSET,
                readonly=True,
                timeout=5,
            )
            return _fetch(ib)
        except Exception as exc:
            errors.append(str(exc))
            return snapshot
        finally:
            if ib is not None:
                try:
                    ib.disconnect()
                except Exception:
                    pass
            snapshot["synced_at"] = now_iso()

    finally:
        snapshot["synced_at"] = now_iso()


async def run_broker_sync_once() -> dict:
    return await asyncio.to_thread(fetch_broker_snapshot_sync)
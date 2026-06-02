from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from ib_insync import IB


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None and value != "" else default
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None and value != "" else default
    except (TypeError, ValueError):
        return default


def _s(value: Any) -> str:
    return "" if value is None else str(value)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_snapshot(ib: IB) -> dict:
    errors: list[str] = []
    snapshot = {
        "ok": False,
        "connected": bool(ib.isConnected()),
        "account": None,
        "synced_at": now_iso(),
        "source": "LOCAL_GATEWAY_PUSH",
        "pushed_by": "local_ibkr_gateway",
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
    if not snapshot["connected"]:
        errors.append("IBKR/TWS is disconnected")
        return snapshot

    accounts = ib.managedAccounts() or []
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
    for item in ib.portfolio() or []:
        symbol = _s(getattr(item.contract, "symbol", None)).upper()
        if symbol:
            portfolio_by_symbol[symbol] = item

    for position in ib.positions() or []:
        symbol = _s(getattr(position.contract, "symbol", None)).upper()
        if not symbol:
            continue
        item = portfolio_by_symbol.get(symbol)
        market_price = _f(getattr(item, "marketPrice", None), _f(position.avgCost))
        quantity = _f(position.position)
        avg_cost = _f(position.avgCost)
        snapshot["positions"].append({
            "symbol": symbol,
            "quantity": quantity,
            "avg_cost": avg_cost,
            "market_price": market_price,
            "market_value": _f(getattr(item, "marketValue", None), quantity * market_price),
            "unrealized_pnl": _f(getattr(item, "unrealizedPNL", None), (market_price - avg_cost) * quantity),
            "realized_pnl": _f(getattr(item, "realizedPNL", None)),
            "account": _s(getattr(position, "account", None) or snapshot["account"]),
        })

    ib.reqAllOpenOrders()
    ib.sleep(0.5)
    for trade in ib.openTrades() or []:
        contract = trade.contract
        order = trade.order
        status = trade.orderStatus
        snapshot["open_orders"].append({
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
        })

    for fill in ib.fills() or []:
        execution = fill.execution
        commission_report = getattr(fill, "commissionReport", None)
        snapshot["executions"].append({
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
        })

    snapshot["ok"] = True
    snapshot["synced_at"] = now_iso()
    return snapshot


def post_snapshot(snapshot: dict, api_url: str, token: str, timeout: float = 10.0) -> dict:
    url = api_url.rstrip("/") + "/api/broker/push-snapshot"
    response = requests.post(
        url,
        json=snapshot,
        headers={"Authorization": f"Bearer {token}", "X-Broker-Push-Token": token},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def run_gateway() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Push local IBKR/TWS broker snapshots to the Render backend.")
    parser.add_argument("--host", default=os.getenv("IBKR_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=_env_int("IBKR_PORT", 7497))
    parser.add_argument("--client-id", type=int, default=_env_int("IBKR_CLIENT_ID", 17))
    parser.add_argument("--api-url", default=os.getenv("RENDER_API_URL", "https://stock-alert-system-moit.onrender.com"))
    parser.add_argument("--token", default=os.getenv("BROKER_PUSH_TOKEN", "change-me"))
    parser.add_argument("--interval", type=float, default=_env_float("BROKER_PUSH_INTERVAL_SECONDS", 15.0))
    args = parser.parse_args()

    interval = min(30.0, max(10.0, args.interval))
    stopping = False

    def _stop(signum, frame):  # noqa: ARG001
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    ib = IB()
    print(f"Local IBKR gateway starting: {args.host}:{args.port} clientId={args.client_id} -> {args.api_url}", flush=True)
    while not stopping:
        try:
            if not ib.isConnected():
                ib.connect(args.host, args.port, clientId=args.client_id, readonly=True, timeout=10)
            snapshot = build_snapshot(ib)
            result = post_snapshot(snapshot, args.api_url, args.token)
            print(
                f"{now_iso()} pushed ok={result.get('ok')} connected={snapshot.get('connected')} "
                f"positions={len(snapshot.get('positions') or [])} account={snapshot.get('account')}",
                flush=True,
            )
        except Exception as exc:
            print(f"{now_iso()} gateway error: {exc}", file=sys.stderr, flush=True)
            try:
                if ib.isConnected():
                    ib.disconnect()
            except Exception:
                pass
        time.sleep(interval)

    try:
        if ib.isConnected():
            ib.disconnect()
    except Exception:
        pass
    print("Local IBKR gateway stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_gateway())

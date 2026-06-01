from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import config
from ibkr_asyncio_compat import ensure_event_loop
from ibkr_client import IBKRClient

log = logging.getLogger(__name__)

PAPER_ONLY_MODE = "PAPER_ONLY"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_true(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"true", "1", "yes", "y", "on"}


def _env_false(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"false", "0", "no", "n", "off"}


def validate_paper_only_environment() -> None:
    """Reject broker actions unless explicit IBKR paper-only env flags are safe."""
    if not _env_true("IBKR_PAPER_TRADING"):
        raise PermissionError("IBKR_PAPER_TRADING=true is required for paper broker actions")
    if not _env_false("IBKR_ENABLE_REAL_TRADING"):
        raise PermissionError("IBKR_ENABLE_REAL_TRADING=false is required for paper broker actions")
    if bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)):
        raise PermissionError("Real trading is enabled in config; broker action blocked")
    if not bool(getattr(config, "IBKR_PAPER_TRADING", False)):
        raise PermissionError("Paper trading is disabled in config; broker action blocked")


def paper_only_status() -> dict:
    return {
        "mode": PAPER_ONLY_MODE,
        "ibkr_paper_trading_env": os.getenv("IBKR_PAPER_TRADING"),
        "ibkr_enable_real_trading_env": os.getenv("IBKR_ENABLE_REAL_TRADING"),
        "config_paper_trading": bool(getattr(config, "IBKR_PAPER_TRADING", False)),
        "config_real_trading_enabled": bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)),
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _account_value(summary: list[dict], tag: str) -> float:
    for row in summary:
        if row.get("tag") == tag:
            return _safe_float(row.get("value"))
    return 0.0


class BrokerAdapter:
    """Small paper-only IBKR adapter with safe stubs around existing client code."""

    def __init__(self, client: Any | None = None, client_id_offset: int = 550):
        self._external_client = client is not None
        self.client = client or IBKRClient(client_id=int(config.IBKR_CLIENT_ID) + client_id_offset)

    def _connect(self) -> None:
        ensure_event_loop()
        if hasattr(self.client, "is_connected") and self.client.is_connected():
            return
        if hasattr(self.client, "connect") and self.client.connect():
            return
        if getattr(self.client, "ib", None) is not None:
            return
        raise RuntimeError("Unable to connect to IBKR Paper Trading")

    def close(self) -> None:
        if not self._external_client and hasattr(self.client, "disconnect"):
            self.client.disconnect()

    def get_account_snapshot(self) -> dict:
        self._connect()
        summary_rows = self.client.get_account_summary() if hasattr(self.client, "get_account_summary") else []
        summary: list[dict] = []
        for row in summary_rows or []:
            summary.append({
                "tag": _safe_str(getattr(row, "tag", None) or (row.get("tag") if isinstance(row, dict) else "")),
                "value": _safe_str(getattr(row, "value", None) or (row.get("value") if isinstance(row, dict) else "")),
                "currency": _safe_str(getattr(row, "currency", None) or (row.get("currency") if isinstance(row, dict) else "")),
                "account": _safe_str(getattr(row, "account", None) or (row.get("account") if isinstance(row, dict) else "")),
            })
        return {
            "ok": True,
            "mode": PAPER_ONLY_MODE,
            "synced_at": _now_iso(),
            "net_liquidation": _account_value(summary, "NetLiquidation"),
            "cash": _account_value(summary, "TotalCashValue") or _account_value(summary, "CashBalance"),
            "available_funds": _account_value(summary, "AvailableFunds"),
            "buying_power": _account_value(summary, "BuyingPower"),
            "unrealized_pnl": _account_value(summary, "UnrealizedPnL"),
            "summary": summary,
        }

    def get_positions(self) -> list[dict]:
        self._connect()
        raw_positions = self.client.get_positions() if hasattr(self.client, "get_positions") else []
        positions: list[dict] = []
        for item in raw_positions or []:
            contract = getattr(item, "contract", None) or (item.get("contract") if isinstance(item, dict) else None)
            symbol = _safe_str(getattr(contract, "symbol", None) or (item.get("symbol") if isinstance(item, dict) else "")).upper()
            quantity = _safe_float(getattr(item, "position", None) if not isinstance(item, dict) else item.get("position", item.get("quantity")))
            avg_cost = _safe_float(getattr(item, "avgCost", None) if not isinstance(item, dict) else item.get("avgCost", item.get("avg_cost")))
            market_price = _safe_float(item.get("market_price") if isinstance(item, dict) else None, avg_cost)
            market_value = _safe_float(item.get("market_value") if isinstance(item, dict) else None, quantity * market_price)
            unrealized_pnl = _safe_float(item.get("unrealized_pnl") if isinstance(item, dict) else None, (market_price - avg_cost) * quantity)
            if symbol:
                positions.append({
                    "symbol": symbol,
                    "quantity": quantity,
                    "avg_cost": avg_cost,
                    "market_price": market_price,
                    "market_value": market_value,
                    "unrealized_pnl": unrealized_pnl,
                    "raw_json": json.dumps(str(item), ensure_ascii=False, default=str),
                })
        return positions

    def place_market_order(self, symbol: str, side: str, quantity: float, paper_only: bool = True) -> dict:
        if not paper_only:
            raise PermissionError("BrokerAdapter only supports paper_only=True orders")
        validate_paper_only_environment()
        self._connect()
        ensure_event_loop()
        from ib_insync import MarketOrder, Stock

        symbol = str(symbol or "").strip().upper()
        side = str(side or "").strip().upper()
        quantity = abs(_safe_float(quantity))
        if not symbol or side not in {"BUY", "SELL"} or quantity <= 0:
            raise ValueError("symbol, side BUY/SELL, and positive quantity are required")
        log.warning("PAPER_ONLY broker order submit | symbol=%s side=%s quantity=%s", symbol, side, quantity)
        contract = Stock(symbol, "SMART", "USD")
        ib = self.client.ib
        if hasattr(ib, "qualifyContracts"):
            ib.qualifyContracts(contract)
        trade = ib.placeOrder(contract, MarketOrder(side, quantity, tif="DAY"))
        if hasattr(ib, "sleep"):
            ib.sleep(1)
        status = getattr(getattr(trade, "orderStatus", None), "status", "Submitted")
        order = getattr(trade, "order", None)
        order_status = getattr(trade, "orderStatus", None)
        return {
            "broker_order_id": getattr(order, "orderId", None),
            "broker_perm_id": getattr(order, "permId", None),
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "order_type": "MKT",
            "status": status,
            "filled_quantity": _safe_float(getattr(order_status, "filled", 0)),
            "avg_fill_price": _safe_float(getattr(order_status, "avgFillPrice", 0)),
            "raw_json": {"status": status, "paper_only": True},
        }

    def get_orders(self) -> list[dict]:
        self._connect()
        ib = getattr(self.client, "ib", None)
        if ib is None:
            return []
        if hasattr(ib, "reqAllOpenOrders"):
            ib.reqAllOpenOrders()
        if hasattr(ib, "sleep"):
            ib.sleep(1)
        rows = []
        for trade in ib.openTrades() if hasattr(ib, "openTrades") else []:
            order = getattr(trade, "order", None)
            contract = getattr(trade, "contract", None)
            status = getattr(trade, "orderStatus", None)
            rows.append({
                "broker_order_id": getattr(order, "orderId", None),
                "broker_perm_id": getattr(order, "permId", None),
                "symbol": _safe_str(getattr(contract, "symbol", "")).upper(),
                "side": _safe_str(getattr(order, "action", "")).upper(),
                "quantity": _safe_float(getattr(order, "totalQuantity", 0)),
                "order_type": _safe_str(getattr(order, "orderType", "")),
                "status": _safe_str(getattr(status, "status", "")),
                "filled_quantity": _safe_float(getattr(status, "filled", 0)),
                "avg_fill_price": _safe_float(getattr(status, "avgFillPrice", 0)),
            })
        return rows

    def cancel_order(self, order_id: int | str) -> dict:
        validate_paper_only_environment()
        self._connect()
        ib = getattr(self.client, "ib", None)
        order_id_int = int(order_id)
        try:
            if hasattr(ib, "reqAllOpenOrders"):
                ib.reqAllOpenOrders()
                ib.sleep(1)
        except Exception as exc:
            log.warning("Failed refreshing open orders before cancel: %s", exc)
        for trade in ib.openTrades() if hasattr(ib, "openTrades") else []:
            order = getattr(trade, "order", None)
            if int(getattr(order, "orderId", -1) or -1) == order_id_int:
                log.warning("PAPER_ONLY broker order cancel | order_id=%s", order_id_int)
                ib.cancelOrder(order)
                if hasattr(ib, "sleep"):
                    ib.sleep(1)
                status = _safe_str(getattr(getattr(trade, "orderStatus", None), "status", "CancelSubmitted"))
                return {"ok": True, "broker_order_id": order_id_int, "status": status, "mode": PAPER_ONLY_MODE}
        return {"ok": False, "broker_order_id": order_id_int, "status": "NOT_FOUND", "mode": PAPER_ONLY_MODE}


def get_account_snapshot() -> dict:
    adapter = BrokerAdapter()
    try:
        return adapter.get_account_snapshot()
    finally:
        adapter.close()


def get_positions() -> list[dict]:
    adapter = BrokerAdapter()
    try:
        return adapter.get_positions()
    finally:
        adapter.close()


def place_market_order(symbol: str, side: str, quantity: float, paper_only: bool = True) -> dict:
    adapter = BrokerAdapter()
    try:
        return adapter.place_market_order(symbol, side, quantity, paper_only=paper_only)
    finally:
        adapter.close()


def get_orders() -> list[dict]:
    adapter = BrokerAdapter()
    try:
        return adapter.get_orders()
    finally:
        adapter.close()


def cancel_order(order_id: int | str) -> dict:
    adapter = BrokerAdapter()
    try:
        return adapter.cancel_order(order_id)
    finally:
        adapter.close()

from __future__ import annotations

import config
import database
from ibkr_client import IBKRClient
from recovery_manager import require_buy_allowed
from trading_safety import require_paper_auto_trading_allowed


class TradeEngine:
    def __init__(self):
        self.client = IBKRClient()

    def connect(self) -> bool:
        return self.client.connect()

    def disconnect(self) -> None:
        self.client.disconnect()

    def _validate_trading_permissions(self) -> None:
        require_paper_auto_trading_allowed("Trading")
        require_buy_allowed("trade_engine")

    def _validate_order_risk(
        self,
        symbol: str,
        quantity: float,
        limit_price: float,
    ) -> dict:
        symbol = str(symbol).strip().upper()

        if not symbol:
            raise ValueError("Missing symbol")

        if quantity <= 0:
            raise ValueError("Invalid quantity")

        if limit_price <= 0:
            raise ValueError("Invalid limit price")

        order_value = quantity * limit_price
        max_position_value = (
            float(config.effective_virtual_trading_capital())
            * float(config.MAX_POSITION_PERCENT)
            / 100
        )

        if order_value > max_position_value:
            raise RuntimeError(
                f"Risk block: order value ${order_value:.2f} "
                f"is above max position value ${max_position_value:.2f}"
            )

        if order_value < float(config.MIN_TRADE_USD):
            raise RuntimeError(
                f"Risk block: order value ${order_value:.2f} "
                f"is below minimum trade ${float(config.MIN_TRADE_USD):.2f}"
            )

        return {
            "symbol": symbol,
            "quantity": quantity,
            "limit_price": limit_price,
            "order_value": round(order_value, 2),
            "max_position_value": round(max_position_value, 2),
        }

    def execute_limit_buy(
        self,
        symbol: str,
        quantity: float,
        limit_price: float,
    ) -> dict | None:
        try:
            self._validate_trading_permissions()
        except RuntimeError as exc:
            database.safe_record_trade_journal_event_sync({
                "symbol": symbol,
                "event_type": "BUY_BLOCKED_BY_SAFETY_GATE",
                "decision": "BLOCKED",
                "reason": str(exc),
                "source_module": "trade_engine",
                "price": limit_price,
                "quantity": quantity,
                "raw_payload": {
                    "symbol": symbol,
                    "quantity": quantity,
                    "limit_price": limit_price,
                },
            })
            raise

        risk = self._validate_order_risk(
            symbol=symbol,
            quantity=float(quantity),
            limit_price=float(limit_price),
        )

        print(f"\n🚀 BUY SIGNAL: {risk['symbol']}")
        print(f"Quantity: {risk['quantity']}")
        print(f"Limit Price: {risk['limit_price']}")
        print(f"Order Value: ${risk['order_value']}")

        if config.REQUIRE_MANUAL_CONFIRMATION:
            confirm = input(
                f"Confirm BUY {risk['symbol']} "
                f"{risk['quantity']} shares at {risk['limit_price']}? (yes/no): "
            )

            if confirm.lower() != "yes":
                print("❌ Order cancelled by user")
                return None

        result = self.client.place_limit_buy_order(
            symbol=risk["symbol"],
            quantity=risk["quantity"],
            limit_price=risk["limit_price"],
        )

        print("✅ Order Result:")
        print(result)

        return result

    def execute_buy_signal(
        self,
        symbol: str,
        quantity: float,
        limit_price: float,
    ) -> dict | None:
        return self.execute_limit_buy(
            symbol=symbol,
            quantity=quantity,
            limit_price=limit_price,
        )

from datetime import datetime, timezone


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _paper_gate() -> tuple[bool, str | None]:
    if not bool(getattr(config, "IBKR_PAPER_TRADING", False)):
        return False, "IBKR_PAPER_TRADING must be true"
    if bool(getattr(config, "IBKR_ENABLE_REAL_TRADING", False)):
        return False, "IBKR_ENABLE_REAL_TRADING must be false"
    if str(getattr(config, "TRADING_MODE", "OFF")).upper() in {"OFF", "LIVE"}:
        return False, "TRADING_MODE must allow paper execution"
    return True, None


def connect_ibkr() -> dict:
    ok, err = _paper_gate()
    if not ok:
        return {"ok": False, "connected": False, "account": None, "error": err, "timestamp": _ts()}
    engine = TradeEngine()
    connected = bool(engine.connect())
    return {"ok": connected, "connected": connected, "account": None, "error": None if connected else "connect_failed", "timestamp": _ts()}


def submit_buy_order(symbol, quantity, limit_price=None, reason=None, metadata=None):
    ok, err = _paper_gate()
    if not ok:
        return {"ok": False, "symbol": symbol, "action": "BUY", "order_id": None, "perm_id": None, "status": "REJECTED", "submitted_price": limit_price, "quantity": quantity, "error": err, "timestamp": _ts()}
    if limit_price is None:
        return {"ok": False, "symbol": symbol, "action": "BUY", "order_id": None, "perm_id": None, "status": "REJECTED", "submitted_price": None, "quantity": quantity, "error": "limit_price_required", "timestamp": _ts()}
    try:
        result = TradeEngine().execute_limit_buy(symbol, quantity, limit_price) or {}
        return {"ok": True, "symbol": symbol, "action": "BUY", "order_id": result.get("order_id"), "perm_id": result.get("perm_id"), "status": result.get("status", "SUBMITTED"), "submitted_price": limit_price, "quantity": quantity, "error": None, "timestamp": _ts()}
    except Exception as exc:
        return {"ok": False, "symbol": symbol, "action": "BUY", "order_id": None, "perm_id": None, "status": "REJECTED", "submitted_price": limit_price, "quantity": quantity, "error": str(exc), "timestamp": _ts()}


def submit_sell_order(symbol, quantity, limit_price=None, reason=None, metadata=None):
    return {"ok": False, "symbol": symbol, "action": "SELL", "order_id": None, "perm_id": None, "status": "REJECTED", "submitted_price": limit_price, "quantity": quantity, "error": "not_implemented", "timestamp": _ts()}


def flatten_all(reason="Emergency flatten all"):
    return {"ok": True, "closed": [], "reason": reason, "timestamp": _ts()}


ORDER_STATUS_MAP = {
    "PENDINGSUBMIT": "PENDING",
    "PRESUBMITTED": "SUBMITTED",
    "SUBMITTED": "SUBMITTED",
    "PARTIAL": "PARTIAL",
    "FILLED": "FILLED",
    "CANCELLED": "CANCELLED",
    "APICANCELLED": "CANCELLED",
    "INACTIVE": "REJECTED",
    "REJECTED": "REJECTED",
    "EXPIRED": "EXPIRED",
    "CLOSED": "CLOSED",
}


def normalize_order_status(status: str | None, filled: float = 0.0, remaining: float = 0.0) -> str:
    key = str(status or "").strip().upper()
    mapped = ORDER_STATUS_MAP.get(key, "PENDING")
    if filled > 0 and remaining > 0:
        return "PARTIAL"
    if filled > 0 and remaining <= 0:
        return "FILLED"
    return mapped


def poll_open_orders(client: IBKRClient | None = None) -> list[dict]:
    engine = TradeEngine()
    c = client or engine.client
    if hasattr(c, 'get_open_orders'):
        return list(c.get_open_orders() or [])
    return []


def detect_partial_fills(order: dict) -> dict:
    filled = float(order.get('filled_quantity') or order.get('filled') or 0.0)
    total = float(order.get('quantity') or order.get('total_quantity') or 0.0)
    remaining = float(order.get('remaining_quantity') or order.get('remaining') or max(total-filled,0.0))
    is_partial = filled > 0 and remaining > 0
    return {**order, 'filled_quantity': filled, 'remaining_quantity': remaining, 'partial_fill_quantity': filled if is_partial else 0.0, 'status': normalize_order_status(order.get('status'), filled, remaining)}


def sync_order_statuses(open_orders: list[dict]) -> list[dict]:
    synced=[]
    for o in open_orders:
        normalized = detect_partial_fills(o)
        database.safe_record_trade_journal_event_sync({
            'symbol': normalized.get('symbol'), 'event_type': 'ORDER_STATUS_SYNC', 'decision': normalized.get('status'),
            'reason': 'broker_source_of_truth_sync', 'source_module': 'trade_engine', 'price': normalized.get('avg_fill_price') or normalized.get('limit_price'),
            'quantity': normalized.get('filled_quantity') or normalized.get('quantity'), 'raw_payload': normalized,
        })
        synced.append(normalized)
    return synced


def sync_executions(executions: list[dict]) -> list[dict]:
    out=[]
    for ex in executions:
        row = dict(ex)
        row['execution_timestamp'] = row.get('time') or _ts()
        row['average_fill_price'] = float(row.get('price') or 0.0)
        database.safe_record_trade_journal_event_sync({
            'symbol': row.get('symbol'), 'event_type': 'EXECUTION_SYNC', 'decision': 'FILLED', 'reason': 'broker_execution_sync', 'source_module': 'trade_engine',
            'price': row.get('average_fill_price'), 'quantity': row.get('shares') or row.get('quantity'), 'raw_payload': row,
        })
        out.append(row)
    return out


def reconcile_open_orders(open_orders: list[dict], stale_after_seconds: int = 300) -> dict:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    reconciled = sync_order_statuses(open_orders)
    stale=[]; rejected=[]; cancelled=[]; partial=[]
    for o in reconciled:
        if o.get('status') == 'PARTIAL':
            partial.append(o)
        if o.get('status') == 'REJECTED':
            rejected.append(o)
        if o.get('status') == 'CANCELLED':
            cancelled.append(o)
        ts = o.get('updated_at') or o.get('timestamp')
        if ts:
            try:
                age=(now-datetime.fromisoformat(str(ts).replace('Z','+00:00'))).total_seconds()
                if age > stale_after_seconds and o.get('status') in {'PENDING','SUBMITTED','PARTIAL'}:
                    stale.append(o)
            except Exception:
                pass
    return {'open_orders': reconciled, 'stale_orders': stale, 'rejected_orders': rejected, 'cancelled_orders': cancelled, 'partial_fills': partial}

from ibkr_asyncio_compat import ensure_event_loop

ensure_event_loop()

from ib_insync import *

import config
import database
import order_lifecycle
import watchdog
from trading_safety import require_paper_auto_trading_allowed


class IBKRClient:
    def __init__(
        self,
        host=None,
        port=None,
        client_id=None,
    ):
        self.host = host or config.IBKR_HOST
        self.port = port or config.IBKR_PORT
        self.client_id = client_id or config.IBKR_CLIENT_ID
        ensure_event_loop()
        self.ib = IB()

    def connect(self):
        if not self.ib.isConnected():
            self.ib.connect(
                self.host,
                self.port,
                clientId=self.client_id,
            )

        return self.ib.isConnected()

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    def is_connected(self):
        return self.ib.isConnected()

    def get_accounts(self):
        return self.ib.managedAccounts()

    def get_positions(self):
        return self.ib.positions()

    def get_account_summary(self):
        return self.ib.accountSummary()

    def get_stock_price(self, symbol):
        contract = Stock(symbol, "SMART", "USD")
        self.ib.qualifyContracts(contract)

        self.ib.reqMarketDataType(config.IBKR_MARKET_DATA_TYPE)

        ticker = self.ib.reqMktData(
            contract,
            "",
            False,
            False,
        )

        self.ib.sleep(5)

        result = {
            "symbol": symbol,
            "bid": ticker.bid,
            "ask": ticker.ask,
            "last": ticker.last,
            "close": ticker.close,
            "market_price": ticker.marketPrice(),
        }

        self.ib.cancelMktData(contract)

        if any(result.get(key) for key in ("bid", "ask", "last", "close", "market_price")):
            watchdog.refresh_market_data_timestamp_sync(
                "quote_fetch",
                symbol=symbol,
                metadata={
                    "bid": result.get("bid"),
                    "ask": result.get("ask"),
                    "last": result.get("last"),
                    "market_price": result.get("market_price"),
                },
            )

        return result

    def _safety_check(self):
        require_paper_auto_trading_allowed("IBKR order")

    def place_limit_buy_order(self, symbol, quantity, limit_price):
        symbol = str(symbol).strip().upper()
        try:
            self._safety_check()
        except RuntimeError as exc:
            payload = {
                "symbol": symbol,
                "quantity": quantity,
                "limit_price": limit_price,
            }
            database.safe_record_trade_journal_event_sync({
                "symbol": symbol,
                "event_type": "BUY_BLOCKED_BY_SAFETY_GATE",
                "decision": "BLOCKED",
                "reason": str(exc),
                "source_module": "ibkr_client",
                "price": limit_price,
                "quantity": quantity,
                "raw_payload": payload,
            })
            order_lifecycle.safe_record_order_lifecycle_event_sync({
                "symbol": symbol,
                "side": "BUY",
                "quantity": quantity,
                "price": limit_price,
                "client_id": self.client_id,
                "source_module": "ibkr_client.place_limit_buy_order",
                "state": order_lifecycle.OrderState.FAILED,
                "reason": str(exc),
                "raw_payload": payload,
            })
            raise

        order_lifecycle.safe_record_order_lifecycle_event_sync({
            "symbol": symbol,
            "side": "BUY",
            "quantity": quantity,
            "price": limit_price,
            "client_id": self.client_id,
            "source_module": "ibkr_client.place_limit_buy_order",
            "state": order_lifecycle.OrderState.CREATED,
            "reason": "Limit BUY order created locally before TWS submission",
            "raw_payload": {"symbol": symbol, "quantity": quantity, "limit_price": limit_price},
        })

        contract = Stock(symbol, "SMART", "USD")
        self.ib.qualifyContracts(contract)

        order = LimitOrder(
            "BUY",
            quantity,
            limit_price,
            tif="DAY",
        )

        try:
            trade = self.ib.placeOrder(contract, order)

            order_lifecycle.safe_record_order_lifecycle_event_sync({
                "symbol": symbol,
                "side": "BUY",
                "quantity": quantity,
                "price": limit_price,
                "order_id": trade.order.orderId,
                "perm_id": getattr(trade.order, "permId", None),
                "client_id": self.client_id,
                "source_module": "ibkr_client.place_limit_buy_order",
                "state": order_lifecycle.OrderState.SUBMITTED,
                "reason": "Limit BUY order submitted to TWS",
                "raw_payload": {"order": getattr(trade.order, "__dict__", {})},
            })

            self.ib.sleep(3)

            result = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "perm_id": getattr(trade.order, "permId", None),
                "action": trade.order.action,
                "order_type": trade.order.orderType,
                "limit_price": trade.order.lmtPrice,
                "status": trade.orderStatus.status,
                "filled": trade.orderStatus.filled,
                "remaining": trade.orderStatus.remaining,
                "avg_fill_price": trade.orderStatus.avgFillPrice,
            }

            order_lifecycle.safe_record_order_lifecycle_event_sync({
                "symbol": symbol,
                "side": "BUY",
                "quantity": quantity,
                "price": limit_price,
                "order_id": result["order_id"],
                "perm_id": result["perm_id"],
                "client_id": self.client_id,
                "source_module": "ibkr_client.place_limit_buy_order",
                "state": order_lifecycle.map_ibkr_status_to_state(result["status"], result["filled"], result["remaining"]),
                "reason": f"TWS order status after submission: {result['status']}",
                "raw_payload": result,
            })

            return result

        except Exception as exc:
            order_lifecycle.safe_record_order_lifecycle_event_sync({
                "symbol": symbol,
                "side": "BUY",
                "quantity": quantity,
                "price": limit_price,
                "client_id": self.client_id,
                "source_module": "ibkr_client.place_limit_buy_order",
                "state": order_lifecycle.OrderState.FAILED,
                "reason": str(exc),
                "raw_payload": {"symbol": symbol, "quantity": quantity, "limit_price": limit_price, "error": str(exc)},
            })
            raise

    def cancel_order(self, trade):
        self.ib.cancelOrder(trade.order)
        self.ib.sleep(2)
        return trade.orderStatus.status
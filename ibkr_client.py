from ib_insync import *

import config


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

        return result

    def _safety_check(self):
        if not config.IBKR_PAPER_TRADING:
            raise RuntimeError("IBKR safety block: Paper trading is not enabled")

        if config.IBKR_ENABLE_REAL_TRADING:
            raise RuntimeError("IBKR safety block: Real trading is enabled")

        if int(config.IBKR_PORT) != 7497:
            raise RuntimeError("IBKR safety block: Not connected to Paper TWS port 7497")

    def place_limit_buy_order(self, symbol, quantity, limit_price):
        self._safety_check()

        contract = Stock(symbol, "SMART", "USD")
        self.ib.qualifyContracts(contract)

        order = LimitOrder(
            "BUY",
            quantity,
            limit_price,
            tif="DAY",
        )

        trade = self.ib.placeOrder(contract, order)

        self.ib.sleep(3)

        return {
            "symbol": symbol,
            "order_id": trade.order.orderId,
            "action": trade.order.action,
            "order_type": trade.order.orderType,
            "limit_price": trade.order.lmtPrice,
            "status": trade.orderStatus.status,
            "filled": trade.orderStatus.filled,
            "remaining": trade.orderStatus.remaining,
            "avg_fill_price": trade.orderStatus.avgFillPrice,
        }

    def cancel_order(self, trade):
        self.ib.cancelOrder(trade.order)
        self.ib.sleep(2)
        return trade.orderStatus.status
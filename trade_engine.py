from __future__ import annotations

import config
from ibkr_client import IBKRClient


class TradeEngine:
    def __init__(self):
        self.client = IBKRClient()

    def connect(self) -> bool:
        return self.client.connect()

    def disconnect(self) -> None:
        self.client.disconnect()

    def _validate_trading_permissions(self) -> None:
        if config.TRADING_MODE == "OFF":
            raise RuntimeError("Trading blocked: TRADING_MODE is OFF")

        if not config.AUTO_SEND_ORDERS:
            raise RuntimeError("Trading blocked: AUTO_SEND_ORDERS is false")

        if not config.IBKR_PAPER_TRADING:
            raise RuntimeError("Trading blocked: IBKR_PAPER_TRADING is false")

        if config.IBKR_ENABLE_REAL_TRADING:
            raise RuntimeError("Trading blocked: LIVE trading is enabled")

        if int(config.IBKR_PORT) != 7497:
            raise RuntimeError("Trading blocked: IBKR port is not Paper port 7497")

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
            float(config.ACCOUNT_BALANCE)
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
        self._validate_trading_permissions()

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
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
            float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0))
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
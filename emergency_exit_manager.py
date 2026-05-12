from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from ib_insync import IB, MarketOrder

import config
import database
from trading_safety import require_paper_auto_trading_allowed

log = logging.getLogger(__name__)

EMERGENCY_TIMEOUT_SECONDS = 60
EMERGENCY_CLIENT_ID_OFFSET = 300

OPEN_ORDER_STATUSES = {
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
}


class EmergencyExitManager:

    def __init__(self):
        self.ib = IB()
        self.loop = None

    def connect(self) -> bool:
        try:
            require_paper_auto_trading_allowed("Emergency exit")

            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            client_id = int(config.IBKR_CLIENT_ID) + EMERGENCY_CLIENT_ID_OFFSET

            self.ib.connect(
                config.IBKR_HOST,
                int(config.IBKR_PORT),
                clientId=client_id,
                timeout=10,
            )

            return self.ib.isConnected()

        except Exception as e:
            log.exception(
                "EmergencyExitManager connect failed: %s",
                e,
            )

            return False

    def disconnect(self):
        try:
            if self.ib.isConnected():
                self.ib.disconnect()
        except Exception:
            pass

        try:
            if self.loop:
                self.loop.close()
        except Exception:
            pass

    def process_emergency_exits(self):
        try:
            self.ib.reqAllOpenOrders()
            self.ib.sleep(1)

            open_trades = self.ib.openTrades()
            now = datetime.now(timezone.utc)

            for trade in open_trades:
                try:
                    order = trade.order
                    contract = trade.contract
                    status = str(trade.orderStatus.status or "")

                    if str(order.action or "").upper() != "SELL":
                        continue

                    if status not in OPEN_ORDER_STATUSES:
                        continue

                    order_time = None

                    if trade.log:
                        order_time = trade.log[0].time

                    if not order_time:
                        continue

                    if order_time.tzinfo is None:
                        order_time = order_time.replace(tzinfo=timezone.utc)

                    age = now - order_time

                    if age < timedelta(seconds=EMERGENCY_TIMEOUT_SECONDS):
                        continue

                    symbol = getattr(
                        contract,
                        "symbol",
                        "UNKNOWN",
                    )

                    remaining = float(
                        trade.orderStatus.remaining or 0
                    )

                    if remaining <= 0:
                        continue

                    log.warning(
                        "EMERGENCY EXIT ACTIVATED | %s | remaining=%s | age=%s",
                        symbol,
                        remaining,
                        age,
                    )

                    database.safe_record_trade_journal_event_sync({
                        "symbol": symbol,
                        "event_type": "EMERGENCY_EXIT_TRIGGERED",
                        "decision": "REPLACE_WITH_MARKET_SELL",
                        "reason": f"SELL order stale for {age}",
                        "source_module": "emergency_exit_manager",
                        "quantity": remaining,
                        "raw_payload": {
                            "status": status,
                            "remaining": remaining,
                            "age": str(age),
                            "order_id": getattr(order, "orderId", None),
                            "perm_id": getattr(order, "permId", None),
                        },
                    })

                    self.ib.cancelOrder(order)
                    self.ib.sleep(1)

                    market_order = MarketOrder(
                        "SELL",
                        remaining,
                    )

                    new_trade = self.ib.placeOrder(
                        contract,
                        market_order,
                    )

                    self.ib.sleep(2)

                    log.warning(
                        "EMERGENCY MARKET SELL SENT | %s | qty=%s | status=%s",
                        symbol,
                        remaining,
                        new_trade.orderStatus.status,
                    )

                except Exception as e:
                    log.exception(
                        "FAILED TO PROCESS EMERGENCY EXIT: %s",
                        e,
                    )

        except Exception as e:
            log.exception(
                "process_emergency_exits failed: %s",
                e,
            )
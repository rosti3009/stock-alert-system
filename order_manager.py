from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from ib_insync import IB

import config
from trade_protection import get_live_quote
from trading_safety import require_paper_auto_trading_allowed

log = logging.getLogger(__name__)

STALE_MINUTES = 5
ORDER_MANAGER_CLIENT_ID_OFFSET = 200

OPEN_ORDER_STATUSES = {
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
}


class OrderManager:

    def __init__(self):
        self.ib = IB()
        self.loop = None

    def connect(self) -> bool:
        try:
            require_paper_auto_trading_allowed("Order manager")

            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            client_id = (
                int(config.IBKR_CLIENT_ID)
                + ORDER_MANAGER_CLIENT_ID_OFFSET
            )

            self.ib.connect(
                config.IBKR_HOST,
                int(config.IBKR_PORT),
                clientId=client_id,
                timeout=10,
            )

            return self.ib.isConnected()

        except Exception as e:
            log.exception(
                "ORDER MANAGER CONNECT FAILED: %s",
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

    def cancel_stale_orders(self):
        try:
            self.ib.reqAllOpenOrders()
            self.ib.sleep(1)

            open_trades = self.ib.openTrades()
            now = datetime.now(timezone.utc)

            for trade in open_trades:
                try:
                    status = str(
                        trade.orderStatus.status or ""
                    )

                    if status not in OPEN_ORDER_STATUSES:
                        continue

                    action = str(
                        trade.order.action or ""
                    ).upper()

                    if action != "BUY":
                        continue

                    symbol = getattr(
                        trade.contract,
                        "symbol",
                        "UNKNOWN",
                    )

                    # ==========================================
                    # PROTECTION 1 — CANCEL BUY IF NO BID/ASK
                    # ==========================================

                    try:
                        quote = get_live_quote(
                            self.ib,
                            symbol,
                        )

                        bid = float(
                            quote.get("bid") or 0
                        )

                        ask = float(
                            quote.get("ask") or 0
                        )

                        if bid <= 0 or ask <= 0:
                            log.warning(
                                "CANCEL BUY ORDER — invalid bid/ask, possible halt | %s | bid=%s ask=%s",
                                symbol,
                                bid,
                                ask,
                            )

                            self.ib.cancelOrder(
                                trade.order
                            )

                            self.ib.sleep(1)

                            continue

                    except Exception as e:
                        log.warning(
                            "Quote protection failed for %s: %s",
                            symbol,
                            e,
                        )

                    # ==========================================
                    # PROTECTION 2 — CANCEL STALE BUY
                    # ==========================================

                    order_time = None

                    if trade.log:
                        order_time = trade.log[0].time

                    if not order_time:
                        continue

                    if order_time.tzinfo is None:
                        order_time = order_time.replace(
                            tzinfo=timezone.utc
                        )

                    age = now - order_time

                    if age < timedelta(
                        minutes=STALE_MINUTES
                    ):
                        continue

                    log.warning(
                        "CANCEL STALE BUY ORDER | %s | status=%s | age=%s",
                        symbol,
                        status,
                        age,
                    )

                    self.ib.cancelOrder(
                        trade.order
                    )

                    self.ib.sleep(1)

                except Exception as e:
                    log.exception(
                        "FAILED TO HANDLE STALE ORDER: %s",
                        e,
                    )

        except Exception as e:
            log.exception(
                "cancel_stale_orders failed: %s",
                e,
            )
from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Any

from ibkr_asyncio_compat import ensure_event_loop

ensure_event_loop()

from ib_insync import MarketOrder

import config
import database
from ibkr_client import IBKRClient
from trading_safety import require_paper_auto_trading_allowed

LIQUIDATION_CLIENT_ID_OFFSET = 400
LIQUIDATION_ORDER_TYPE = "MKT"


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _contract_symbol(contract: Any) -> str:
    return str(getattr(contract, "symbol", "UNKNOWN") or "UNKNOWN").strip().upper()


def _position_quantity(position: Any) -> float:
    return float(getattr(position, "position", 0) or 0)


def _trade_status(trade: Any) -> str:
    return str(
        getattr(getattr(trade, "orderStatus", None), "status", "SUBMITTED")
        or "SUBMITTED"
    )


def _trade_order_id(trade: Any) -> int | None:
    return getattr(getattr(trade, "order", None), "orderId", None)


def _record_liquidation_attempt(
    *,
    symbol: str,
    quantity: float,
    status: str,
    reason: str,
    order_type: str = LIQUIDATION_ORDER_TYPE,
    order_id: int | None = None,
) -> dict:
    timestamp = _utc_timestamp()
    payload = {
        "timestamp": timestamp,
        "symbol": symbol,
        "quantity": quantity,
        "order_type": order_type,
        "status": status,
        "reason": reason,
        "order_id": order_id,
    }
    database.safe_record_trade_journal_event_sync(
        {
            "symbol": symbol,
            "event_type": "PAPER_LIQUIDATION_ATTEMPT",
            "decision": status,
            "reason": reason,
            "source_module": "paper_liquidation",
            "quantity": quantity,
            "raw_payload": payload,
        }
    )
    return payload


def _set_auto_trading_enabled_sync(enabled: bool) -> None:
    with closing(sqlite3.connect(database.DB_PATH)) as db:
        db.execute(database.CREATE_APP_STATE)
        db.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES ('auto_trading_enabled', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("true" if enabled else "false",),
        )
        db.commit()


def liquidate_all_paper_positions(
    restart_auto_trading_after: bool = False,
    ibkr_client: Any | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Sell every current long TWS/IBKR paper position with a market order.

    When dry_run is true, return a preview of the current TWS positions that
    would be sold without qualifying contracts or submitting IBKR orders. The
    shared paper-only safety gate still runs before any TWS connection or order
    submission. Liquidation is blocked unless automated paper trading is enabled
    on the TWS paper port with live trading disabled.
    """
    try:
        require_paper_auto_trading_allowed("Paper liquidation")
    except RuntimeError as exc:
        _record_liquidation_attempt(
            symbol="ALL",
            quantity=0,
            status="BLOCKED",
            reason=str(exc),
        )
        raise

    ensure_event_loop()

    client_created = ibkr_client is None
    client = ibkr_client or IBKRClient(
        client_id=int(config.IBKR_CLIENT_ID) + LIQUIDATION_CLIENT_ID_OFFSET
    )
    connected = False
    attempts: list[dict] = []
    would_sell: list[dict] = []
    skipped_positions: list[dict] = []
    errors: list[dict] = []

    try:
        if hasattr(client, "is_connected") and client.is_connected():
            connected = True
        elif hasattr(client, "connect"):
            connected = bool(client.connect())
        else:
            connected = True

        if not connected:
            raise RuntimeError(
                "Paper liquidation blocked: unable to connect to IBKR TWS Paper Trading"
            )

        positions = list(client.get_positions())
        long_positions = []

        for position in positions:
            contract = getattr(position, "contract", None)
            symbol = _contract_symbol(contract)
            quantity = _position_quantity(position)

            if quantity > 0:
                long_positions.append(position)
                would_sell.append(
                    {
                        "symbol": symbol,
                        "quantity": quantity,
                        "action": "SELL",
                        "order_type": LIQUIDATION_ORDER_TYPE,
                    }
                )
            else:
                skipped_positions.append(
                    {
                        "symbol": symbol,
                        "quantity": quantity,
                        "reason": "Position is not long",
                    }
                )

        if not dry_run:
            for position in long_positions:
                contract = position.contract
                symbol = _contract_symbol(contract)
                quantity = _position_quantity(position)
                order = MarketOrder("SELL", quantity, tif="DAY")

                try:
                    if hasattr(client, "ib") and hasattr(client.ib, "qualifyContracts"):
                        client.ib.qualifyContracts(contract)

                    trade = client.ib.placeOrder(contract, order)

                    if hasattr(client, "ib") and hasattr(client.ib, "sleep"):
                        client.ib.sleep(1)

                    status = _trade_status(trade)
                    attempts.append(
                        _record_liquidation_attempt(
                            symbol=symbol,
                            quantity=quantity,
                            status=status,
                            reason="Submitted SELL market order to TWS Paper Trading",
                            order_id=_trade_order_id(trade),
                        )
                    )

                except Exception as exc:
                    error = {
                        "symbol": symbol,
                        "quantity": quantity,
                        "reason": str(exc),
                    }
                    errors.append(error)
                    attempts.append(
                        _record_liquidation_attempt(
                            symbol=symbol,
                            quantity=quantity,
                            status="FAILED",
                            reason=str(exc),
                        )
                    )

        if restart_auto_trading_after:
            _set_auto_trading_enabled_sync(True)

        return {
            "status": "completed",
            "paper_trading": True,
            "real_trading_enabled": False,
            "ibkr_port": int(config.IBKR_PORT),
            "restart_auto_trading_after": bool(restart_auto_trading_after),
            "dry_run": bool(dry_run),
            "positions_seen": len(positions),
            "positions_found": len(positions),
            "long_positions_liquidated": 0 if dry_run else len(long_positions),
            "would_sell": would_sell,
            "skipped_positions": skipped_positions,
            "errors": errors,
            "attempts": attempts,
        }

    finally:
        if client_created and hasattr(client, "disconnect"):
            client.disconnect()

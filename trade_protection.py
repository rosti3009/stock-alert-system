from __future__ import annotations

import logging
import math

from ib_insync import IB, Stock

log = logging.getLogger(__name__)

MIN_PRICE = 2.0
MAX_SPREAD_PERCENT = 3.0
MAX_ENTRY_DRIFT_PERCENT = 2.0


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default

        value = float(value)

        if math.isnan(value):
            return default

        return value

    except Exception:
        return default


def get_live_quote(
    ib: IB,
    symbol: str,
) -> dict:
    contract = Stock(
        symbol,
        "SMART",
        "USD",
    )

    ib.qualifyContracts(contract)

    ticker = ib.reqMktData(
        contract,
        "",
        False,
        False,
    )

    ib.sleep(2)

    bid = safe_float(ticker.bid)
    ask = safe_float(ticker.ask)
    last = safe_float(ticker.last)
    close = safe_float(ticker.close)

    market_price = safe_float(
        ticker.marketPrice()
    )

    ib.cancelMktData(contract)

    return {
        "symbol": symbol,
        "contract": contract,
        "bid": bid,
        "ask": ask,
        "last": last,
        "close": close,
        "market_price": market_price,
    }


def validate_buy_before_order(
    ib: IB,
    symbol: str,
    limit_price: float,
) -> dict:
    quote = get_live_quote(
        ib,
        symbol,
    )

    bid = quote["bid"]
    ask = quote["ask"]
    last = quote["last"]
    market_price = quote["market_price"]

    if limit_price <= 0:
        return {
            "allowed": False,
            "reason": "Invalid limit price",
            "quote": quote,
        }

    if limit_price < MIN_PRICE:
        return {
            "allowed": False,
            "reason": f"Price below minimum ${MIN_PRICE}",
            "quote": quote,
        }

    if bid <= 0 or ask <= 0:
        return {
            "allowed": False,
            "reason": "No valid bid/ask — possible halt, illiquid stock, or bad market data",
            "quote": quote,
        }

    if ask < bid:
        return {
            "allowed": False,
            "reason": "Invalid quote — ask below bid",
            "quote": quote,
        }

    mid = (bid + ask) / 2

    if mid <= 0:
        return {
            "allowed": False,
            "reason": "Invalid mid price",
            "quote": quote,
        }

    spread_percent = ((ask - bid) / mid) * 100

    if spread_percent > MAX_SPREAD_PERCENT:
        return {
            "allowed": False,
            "reason": f"Spread too wide {spread_percent:.2f}%",
            "quote": quote,
        }

    reference_price = market_price or last or mid

    if reference_price <= 0:
        return {
            "allowed": False,
            "reason": "No valid reference price",
            "quote": quote,
        }

    drift_percent = (
        (reference_price - limit_price)
        / limit_price
    ) * 100

    if drift_percent > MAX_ENTRY_DRIFT_PERCENT:
        return {
            "allowed": False,
            "reason": f"Price ran away {drift_percent:.2f}% above limit",
            "quote": quote,
        }

    return {
        "allowed": True,
        "reason": "OK",
        "quote": quote,
        "spread_percent": round(spread_percent, 4),
        "drift_percent": round(drift_percent, 4),
    }
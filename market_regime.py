from __future__ import annotations

import logging

from data_fetcher import fetch_stock_data
from indicators import compute_indicators

log = logging.getLogger(__name__)


def _is_bullish(ind: dict) -> bool:
    price = ind.get("price")
    ma50 = ind.get("ma50")
    ma200 = ind.get("ma200")
    trend = ind.get("trend")

    if price is None or ma50 is None:
        return False

    if ma200 is None:
        return price > ma50 and trend in ("Bullish", "Strong Bullish")

    return price > ma50 and price > ma200 and trend in ("Bullish", "Strong Bullish")


def get_market_regime() -> dict:
    symbols = ["SPY", "QQQ"]
    results = {}

    for symbol in symbols:
        raw = fetch_stock_data(symbol)

        if raw is None:
            results[symbol] = {
                "symbol": symbol,
                "ok": False,
                "reason": "No market data",
            }
            continue

        ind = compute_indicators(raw)
        ind["symbol"] = symbol

        bullish = _is_bullish(ind)

        results[symbol] = {
            "symbol": symbol,
            "ok": bullish,
            "price": ind.get("price"),
            "ma50": ind.get("ma50"),
            "ma200": ind.get("ma200"),
            "trend": ind.get("trend"),
            "reason": "Bullish market structure" if bullish else "Weak market structure",
        }

    spy_ok = results.get("SPY", {}).get("ok", False)
    qqq_ok = results.get("QQQ", {}).get("ok", False)

    if spy_ok and qqq_ok:
        regime = "RISK_ON"
        allow_new_buys = True
        min_score_override = 80
        position_size_factor = 1.0

    elif spy_ok or qqq_ok:
        regime = "CAUTION"
        allow_new_buys = True
        min_score_override = 90
        position_size_factor = 0.5

    else:
        regime = "RISK_OFF"
        allow_new_buys = False
        min_score_override = 999
        position_size_factor = 0.0

    return {
        "regime": regime,
        "allow_new_buys": allow_new_buys,
        "min_score_override": min_score_override,
        "position_size_factor": position_size_factor,
        "details": results,
    }
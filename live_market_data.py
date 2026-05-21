from __future__ import annotations

from datetime import datetime, timezone

from ibkr_client import IBKRClient


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _na(symbol: str, reason: str) -> dict:
    return {"ok": False, "symbol": symbol.upper(), "reason": reason, "timestamp": _now()}


def get_realtime_quote(symbol: str) -> dict:
    return _na(symbol, "realtime_quote_unavailable")


def get_bid_ask(symbol: str) -> dict:
    return _na(symbol, "bid_ask_unavailable")


def get_intraday_bars(symbol: str, timeframe: str) -> dict:
    return {**_na(symbol, "intraday_bars_unavailable"), "timeframe": timeframe}


def get_vwap(symbol: str) -> dict:
    return _na(symbol, "vwap_unavailable")


def get_relative_volume(symbol: str) -> dict:
    return _na(symbol, "relative_volume_unavailable")


def validate_liquidity(symbol: str) -> dict:
    bid_ask = get_bid_ask(symbol)
    return {
        "ok": bool(bid_ask.get("ok")),
        "symbol": symbol.upper(),
        "details": bid_ask,
        "timestamp": _now(),
    }

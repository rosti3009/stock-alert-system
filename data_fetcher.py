from __future__ import annotations

from datetime import date, timedelta
import time
import requests

from config import API_KEY, API_PROVIDER

MIN_BARS = 220
REQUEST_TIMEOUT = 30

_bad_symbols: set[str] = set()


def _clean_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _is_valid_symbol(symbol: str) -> bool:
    if not symbol:
        return False

    blocked_chars = ["^", "/", "\\", " ", "$"]

    if any(ch in symbol for ch in blocked_chars):
        return False

    if len(symbol) > 8:
        return False

    return True


def _build_payload(
    symbol: str,
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
) -> dict | None:
    if len(closes) < MIN_BARS:
        if symbol not in _bad_symbols:
            print(f"[data_fetcher] Not enough history for {symbol}: {len(closes)} bars")
            _bad_symbols.add(symbol)
        return None

    return {
        "symbol": symbol,
        "current_price": closes[-1],
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "volumes": volumes,
    }


def _alphavantage_fetch(symbol: str) -> dict | None:
    symbol = _clean_symbol(symbol)

    if not API_KEY:
        print("[AlphaVantage] Missing API_KEY")
        return None

    response = requests.get(
        "https://www.alphavantage.co/query",
        params={
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "full",
            "apikey": API_KEY,
        },
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()
    data = response.json()

    ts = data.get("Time Series (Daily)")
    if not ts:
        msg = data.get("Note") or data.get("Information") or data.get("Error Message") or "unknown error"
        print(f"[AlphaVantage] No data for {symbol}: {msg}")
        return None

    dates = sorted(ts.keys())

    opens = [float(ts[d]["1. open"]) for d in dates]
    highs = [float(ts[d]["2. high"]) for d in dates]
    lows = [float(ts[d]["3. low"]) for d in dates]
    closes = [float(ts[d]["4. close"]) for d in dates]
    volumes = [float(ts[d]["5. volume"]) for d in dates]

    return _build_payload(symbol, opens, highs, lows, closes, volumes)


def _polygon_fetch(symbol: str) -> dict | None:
    symbol = _clean_symbol(symbol)

    if not API_KEY:
        print("[Polygon] Missing API_KEY")
        return None

    today = date.today()
    start_date = today - timedelta(days=900)

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/"
        f"{symbol}/range/1/day/{start_date.isoformat()}/{today.isoformat()}"
    )

    response = requests.get(
        url,
        params={
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": API_KEY,
        },
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code in (401, 403):
        print(f"[Polygon] Auth error for {symbol}. Check API_KEY / plan.")
        return None

    if response.status_code == 429:
        print("[Polygon] Rate limit detected. Sleeping 10 seconds...")
        time.sleep(10)
        return None

    response.raise_for_status()
    data = response.json()

    if data.get("status") == "ERROR":
        print(f"[Polygon] API error for {symbol}: {data.get('error')}")
        return None

    results = data.get("results") or []

    if not results:
        if symbol not in _bad_symbols:
            print(f"[Polygon] No data for {symbol}: {data.get('message') or 'empty results'}")
            _bad_symbols.add(symbol)
        return None

    opens = [float(x["o"]) for x in results if "o" in x]
    highs = [float(x["h"]) for x in results if "h" in x]
    lows = [float(x["l"]) for x in results if "l" in x]
    closes = [float(x["c"]) for x in results if "c" in x]
    volumes = [float(x["v"]) for x in results if "v" in x]

    if not (len(opens) == len(highs) == len(lows) == len(closes) == len(volumes)):
        print(f"[data_fetcher] Bad OHLCV length mismatch for {symbol}")
        return None

    return _build_payload(symbol, opens, highs, lows, closes, volumes)


def fetch_stock_data(symbol: str) -> dict | None:
    symbol = _clean_symbol(symbol)

    if not _is_valid_symbol(symbol):
        return None

    if symbol in _bad_symbols:
        return None

    try:
        provider = API_PROVIDER.strip().lower()

        if provider == "alphavantage":
            return _alphavantage_fetch(symbol)

        if provider in ("massive", "polygon"):
            return _polygon_fetch(symbol)

        print(f"[data_fetcher] Unknown API_PROVIDER={API_PROVIDER}")
        return None

    except requests.exceptions.Timeout:
        print(f"[data_fetcher] Timeout fetching {symbol}")
        return None

    except requests.exceptions.RequestException as exc:
        print(f"[data_fetcher] Network/API error fetching {symbol}: {exc}")
        return None

    except Exception as exc:
        print(f"[data_fetcher] Unexpected error fetching {symbol}: {exc}")
        return None
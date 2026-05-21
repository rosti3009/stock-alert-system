from __future__ import annotations

import threading
import time

import requests

NASDAQ_LIST_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"

PRIORITY_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "AMD",
    "NFLX", "COST", "ADBE", "PEP", "CSCO", "QCOM", "INTC", "INTU", "AMGN", "TXN",
]

DOW_30 = [
    "AAPL", "AMGN", "AMZN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS",
    "GS", "HD", "HON", "IBM", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK",
    "MSFT", "NKE", "NVDA", "PG", "SHW", "TRV", "UNH", "V", "VZ", "WMT",
]


def _is_bad_symbol(symbol: str, name: str = "") -> bool:
    s = (symbol or "").upper().strip()
    n = (name or "").upper().strip()

    if not s:
        return True

    # Symbols that usually break APIs or are not regular common stocks
    if any(ch in s for ch in ["$", "^", "/", "\\", ".", " "]):
        return True

    # Too long is usually not a clean liquid common stock
    if len(s) > 5:
        return True

    # Nasdaq suffix patterns commonly used for special securities
    bad_suffixes = (
        "W",   # warrant
        "WS",  # warrant
        "WT",  # warrant
        "U",   # unit
        "R",   # right
        "P",   # preferred
        "Q",   # bankruptcy/other
        "Z",
    )

    if len(s) >= 5 and s.endswith(bad_suffixes):
        return True

    bad_markers = [
        "ETF",
        "ETN",
        "ETP",
        "WARRANT",
        "WARRANTS",
        "RIGHT",
        "RIGHTS",
        "UNIT",
        "UNITS",
        "PREFERRED",
        "PREF",
        "DEPOSITARY",
        "DEPOSITORY",
        "NOTES",
        "NOTE",
        "BOND",
        "TRUST",
        "FUND",
        "INDEX",
        "SPAC",
        "ACQUISITION",
        "MERGER",
        "INCOME",
        "SHARES OF BENEFICIAL INTEREST",
        "REIT",
        "BDC",
        "PARTNERSHIP",
        "LP",
        "L.P.",
        "PLC",
    ]

    if any(marker in n for marker in bad_markers):
        return True

    return False




_SYMBOL_CACHE: list[str] = []
_SYMBOL_CACHE_TS: float = 0.0
_SYMBOL_CACHE_TTL_SECONDS = 60 * 60
_CACHE_LOCK = threading.RLock()


def _load_symbols_uncached(limit: int | None = None) -> list[str]:
    symbols: list[str] = []

    try:
        response = requests.get(NASDAQ_LIST_URL, timeout=20)
        response.raise_for_status()

        for line in response.text.splitlines():
            if not line or line.startswith("Symbol|") or line.startswith("File Creation Time"):
                continue

            parts = line.split("|")
            if len(parts) < 2:
                continue

            symbol = parts[0].strip().upper()
            name = parts[1].strip()

            if not _is_bad_symbol(symbol, name):
                symbols.append(symbol)

    except Exception as exc:
        print(f"[symbol_loader] Failed loading Nasdaq symbols: {exc}")
        symbols = []

    clean: list[str] = []
    seen: set[str] = set()

    # Priority first
    for s in PRIORITY_SYMBOLS + DOW_30 + symbols:
        s = s.strip().upper()

        if not s or s in seen:
            continue

        if _is_bad_symbol(s):
            continue

        clean.append(s)
        seen.add(s)

    if limit:
        clean = clean[:limit]

    return clean

def get_cached_symbols(limit: int | None = None, force_refresh: bool = False, ttl_seconds: int | None = None) -> list[str]:
    ttl = _SYMBOL_CACHE_TTL_SECONDS if ttl_seconds is None else max(0, int(ttl_seconds))
    now = time.time()

    with _CACHE_LOCK:
        cache_fresh = bool(_SYMBOL_CACHE) and (ttl <= 0 or (now - _SYMBOL_CACHE_TS) < ttl)
        if not force_refresh and cache_fresh:
            symbols = list(_SYMBOL_CACHE)
            return symbols[:limit] if limit else symbols

        symbols = _load_symbols_uncached(limit=None)
        globals()["_SYMBOL_CACHE"] = list(symbols)
        globals()["_SYMBOL_CACHE_TS"] = now

    print(f"[symbol_loader] Loaded {len(symbols)} symbols")
    return symbols[:limit] if limit else list(symbols)


def load_nasdaq_symbols(limit: int | None = None, force_refresh: bool = False) -> list[str]:
    return get_cached_symbols(limit=limit, force_refresh=force_refresh)

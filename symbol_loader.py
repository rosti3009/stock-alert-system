from __future__ import annotations

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


def load_nasdaq_symbols(limit: int | None = None) -> list[str]:
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

    print(f"[symbol_loader] Loaded {len(clean)} symbols")
    return clean
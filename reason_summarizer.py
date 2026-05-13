from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any, Iterable

MAX_DASHBOARD_REASON_CHARS = 250
DEFAULT_TOP_REASON_CATEGORIES = 5

_CATEGORY_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Low liquidity", ("insufficient liquidity", "low average volume", "low relative volume", "relative volume", "average volume", "liquidity")),
    ("Dangerous volatility", ("dangerous volatility", "intraday volatility", "volatility", "atr", "candle expansion")),
    ("Concentration", ("extreme concentration", "concentration", "largest position", "sector exposure", "symbol exposure")),
    ("Drawdown", ("drawdown", "daily realized", "unrealized drawdown")),
    ("Spread risk", ("dangerous spread", "wide spread", "spread")),
    ("Halt/quote risk", ("halt", "invalid quote", "no valid bid", "bid/ask", "bad market data")),
    ("Crash protection", ("crash protection", "risk off", "market regime")),
    ("Capital", ("capital", "minimum trade size", "account utilization")),
    ("Invalid inputs", ("invalid price", "invalid", "no valid reference")),
)

_SYMBOL_PREFIX_RE = re.compile(r"^\s*([A-Z][A-Z0-9.\-]{0,9})\s*[:\-–—]\s*(.+)$")


def _clean_reason(reason: Any) -> str:
    return " ".join(str(reason or "").replace(";", " ").split())


def _split_reason(reason: Any) -> list[str]:
    text = _clean_reason(reason)
    if not text:
        return []
    parts = re.split(r"\s+[·;]\s+", text)
    return [_clean_reason(part) for part in parts if _clean_reason(part)]


def _extract_symbol(reason: str, fallback: str | None = None) -> tuple[str | None, str]:
    match = _SYMBOL_PREFIX_RE.match(reason)
    if match:
        return match.group(1).upper(), match.group(2).strip()
    normalized = str(fallback or "").strip().upper()
    return normalized or None, reason


def categorize_reason(reason: Any) -> str:
    """Return a stable dashboard category for a raw block/risk reason."""

    text = _clean_reason(reason).lower()
    for category, patterns in _CATEGORY_PATTERNS:
        if any(pattern in text for pattern in patterns):
            return category
    cleaned = _clean_reason(reason)
    if ":" in cleaned:
        return cleaned.split(":", 1)[0].strip() or "Other"
    sentence = re.split(r"[.!?]", cleaned, maxsplit=1)[0].strip()
    return sentence[:48] or "Other"


def summarize_reason_list(
    reasons: Iterable[Any],
    *,
    symbols: Iterable[str | None] | None = None,
    top_n: int = DEFAULT_TOP_REASON_CATEGORIES,
    max_chars: int = MAX_DASHBOARD_REASON_CHARS,
    default_text: str = "No active risk/block reasons.",
) -> dict[str, Any]:
    """Summarize verbose symbol-level reasons without discarding raw details.

    The return payload is intended for dashboards; callers should keep their raw
    reason arrays in API responses for debugging.
    """

    symbol_list = list(symbols or [])
    expanded: list[dict[str, str | None]] = []
    for index, reason in enumerate(reasons or []):
        fallback_symbol = symbol_list[index] if index < len(symbol_list) else None
        for part in _split_reason(reason):
            symbol, detail = _extract_symbol(part, fallback_symbol)
            expanded.append({"symbol": symbol, "reason": detail, "category": categorize_reason(detail)})

    if not expanded:
        return {
            "text": default_text,
            "primary_reason": default_text,
            "top_categories": [],
            "affected_symbol_count": 0,
            "raw_reason_count": 0,
            "truncated": False,
        }

    category_counts: Counter[str] = Counter(item["category"] or "Other" for item in expanded)
    category_symbols: dict[str, set[str]] = defaultdict(set)
    examples: dict[str, str] = {}
    for item in expanded:
        category = str(item["category"] or "Other")
        if item.get("symbol"):
            category_symbols[category].add(str(item["symbol"]))
        examples.setdefault(category, str(item.get("reason") or category))

    ordered = sorted(category_counts, key=lambda category: (-category_counts[category], category))[: max(1, top_n)]
    top_categories: list[dict[str, Any]] = []
    phrases: list[str] = []
    for category in ordered:
        symbol_count = len(category_symbols[category]) or category_counts[category]
        example = examples.get(category, category)
        top_categories.append({
            "category": category,
            "count": category_counts[category],
            "symbol_count": symbol_count,
            "example": example,
        })
        if category == "Concentration":
            phrases.append(example if example.lower().startswith("concentration") else f"{category}: {example}")
        else:
            suffix = "symbol" if symbol_count == 1 else "symbols"
            phrases.append(f"{category}: {symbol_count} {suffix}")

    text = " · ".join(phrases)
    if len(ordered) < len(category_counts):
        text = f"{text} · +{len(category_counts) - len(ordered)} more"

    truncated = len(text) > max_chars
    if truncated:
        note = " … details available in API"
        text = text[: max(0, max_chars - len(note))].rstrip(" ·,") + note
    elif len(expanded) > len(ordered):
        note = " · details available in API"
        if len(text) + len(note) <= max_chars:
            text += note
        else:
            truncated = True
            text = text[: max(0, max_chars - len(" … details available in API"))].rstrip(" ·,") + " … details available in API"

    affected_symbols = {str(item["symbol"]) for item in expanded if item.get("symbol")}
    return {
        "text": text,
        "primary_reason": top_categories[0]["example"],
        "top_categories": top_categories,
        "affected_symbol_count": len(affected_symbols) or len(expanded),
        "raw_reason_count": len(expanded),
        "truncated": truncated,
    }

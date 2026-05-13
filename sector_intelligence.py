from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import config
import database

UNKNOWN = "UNKNOWN"
ENRICHMENT_CACHE_KEY = "sector_intelligence_enrichment_cache"


@dataclass(frozen=True)
class SymbolClassification:
    symbol: str
    sector: str = UNKNOWN
    industry: str = UNKNOWN
    subsector: str = UNKNOWN
    theme: str = UNKNOWN
    volatility_group: str = "MEDIUM"
    correlation_cluster: str = UNKNOWN
    source: str = "fallback_unknown"

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "sector": self.sector,
            "industry": self.industry,
            "subsector": self.subsector,
            "theme": self.theme,
            "volatility_group": self.volatility_group,
            "correlation_cluster": self.correlation_cluster,
            "source": self.source,
        }


STATIC_CLASSIFICATIONS: dict[str, dict[str, str]] = {
    "AAPL": {"sector": "Technology", "industry": "Consumer Electronics", "subsector": "Hardware", "theme": "Mega Cap Tech", "volatility_group": "MEDIUM", "correlation_cluster": "MEGA_CAP_TECH"},
    "MSFT": {"sector": "Technology", "industry": "Software", "subsector": "Cloud Infrastructure", "theme": "Cloud AI", "volatility_group": "MEDIUM", "correlation_cluster": "MEGA_CAP_TECH"},
    "NVDA": {"sector": "Technology", "industry": "Semiconductors", "subsector": "AI Accelerators", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "AMD": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Processors", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "AVGO": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Connectivity Chips", "theme": "AI Infrastructure", "volatility_group": "MEDIUM", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "META": {"sector": "Communication Services", "industry": "Interactive Media", "subsector": "Social Platforms", "theme": "Digital Advertising", "volatility_group": "MEDIUM", "correlation_cluster": "DIGITAL_ADS"},
    "GOOG": {"sector": "Communication Services", "industry": "Interactive Media", "subsector": "Search Advertising", "theme": "Digital Advertising", "volatility_group": "MEDIUM", "correlation_cluster": "DIGITAL_ADS"},
    "GOOGL": {"sector": "Communication Services", "industry": "Interactive Media", "subsector": "Search Advertising", "theme": "Digital Advertising", "volatility_group": "MEDIUM", "correlation_cluster": "DIGITAL_ADS"},
    "NFLX": {"sector": "Communication Services", "industry": "Entertainment", "subsector": "Streaming", "theme": "Streaming Media", "volatility_group": "MEDIUM", "correlation_cluster": "STREAMING_MEDIA"},
    "AMZN": {"sector": "Consumer Discretionary", "industry": "Internet Retail", "subsector": "E-Commerce", "theme": "Cloud Commerce", "volatility_group": "MEDIUM", "correlation_cluster": "CLOUD_COMMERCE"},
    "TSLA": {"sector": "Consumer Discretionary", "industry": "Automobiles", "subsector": "Electric Vehicles", "theme": "Electric Vehicles", "volatility_group": "HIGH", "correlation_cluster": "EV_AUTOS"},
    "JPM": {"sector": "Financials", "industry": "Banks", "subsector": "Money Center Banks", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "BANKS"},
    "BAC": {"sector": "Financials", "industry": "Banks", "subsector": "Money Center Banks", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "BANKS"},
    "WFC": {"sector": "Financials", "industry": "Banks", "subsector": "Money Center Banks", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "BANKS"},
    "XOM": {"sector": "Energy", "industry": "Oil Gas & Consumable Fuels", "subsector": "Integrated Oil & Gas", "theme": "Energy Commodities", "volatility_group": "MEDIUM", "correlation_cluster": "ENERGY_OIL_GAS"},
    "CVX": {"sector": "Energy", "industry": "Oil Gas & Consumable Fuels", "subsector": "Integrated Oil & Gas", "theme": "Energy Commodities", "volatility_group": "MEDIUM", "correlation_cluster": "ENERGY_OIL_GAS"},
    "JNJ": {"sector": "Health Care", "industry": "Pharmaceuticals", "subsector": "Diversified Pharmaceuticals", "theme": "Defensive Health Care", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_HEALTHCARE"},
    "UNH": {"sector": "Health Care", "industry": "Health Care Providers", "subsector": "Managed Care", "theme": "Defensive Health Care", "volatility_group": "MEDIUM", "correlation_cluster": "DEFENSIVE_HEALTHCARE"},
    "PFE": {"sector": "Health Care", "industry": "Pharmaceuticals", "subsector": "Biopharma", "theme": "Defensive Health Care", "volatility_group": "MEDIUM", "correlation_cluster": "DEFENSIVE_HEALTHCARE"},
    "KO": {"sector": "Consumer Staples", "industry": "Beverages", "subsector": "Soft Drinks", "theme": "Defensive Staples", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_STAPLES"},
    "PEP": {"sector": "Consumer Staples", "industry": "Beverages", "subsector": "Beverages & Snacks", "theme": "Defensive Staples", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_STAPLES"},
    "WMT": {"sector": "Consumer Staples", "industry": "Consumer Staples Distribution", "subsector": "Discount Retail", "theme": "Defensive Staples", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_STAPLES"},
    "BA": {"sector": "Industrials", "industry": "Aerospace & Defense", "subsector": "Commercial Aerospace", "theme": "Industrial Cyclicals", "volatility_group": "HIGH", "correlation_cluster": "AEROSPACE_DEFENSE"},
    "CAT": {"sector": "Industrials", "industry": "Machinery", "subsector": "Construction Machinery", "theme": "Industrial Cyclicals", "volatility_group": "MEDIUM", "correlation_cluster": "INDUSTRIAL_CYCLICALS"},
    "GE": {"sector": "Industrials", "industry": "Industrial Conglomerates", "subsector": "Aerospace Power", "theme": "Industrial Cyclicals", "volatility_group": "MEDIUM", "correlation_cluster": "INDUSTRIAL_CYCLICALS"},
}

ETF_THEME_CLASSIFICATIONS: dict[str, dict[str, str]] = {
    "SPY": {"sector": "Multi-Sector", "industry": "Broad Market ETF", "subsector": "S&P 500", "theme": "US Large Cap", "volatility_group": "MEDIUM", "correlation_cluster": "BROAD_MARKET"},
    "QQQ": {"sector": "Multi-Sector", "industry": "Growth ETF", "subsector": "Nasdaq 100", "theme": "Mega Cap Growth", "volatility_group": "MEDIUM", "correlation_cluster": "GROWTH_INDEX"},
    "IWM": {"sector": "Multi-Sector", "industry": "Small Cap ETF", "subsector": "Russell 2000", "theme": "US Small Cap", "volatility_group": "HIGH", "correlation_cluster": "SMALL_CAP"},
    "XLK": {"sector": "Technology", "industry": "Sector ETF", "subsector": "Technology ETF", "theme": "Technology", "volatility_group": "MEDIUM", "correlation_cluster": "TECHNOLOGY_SECTOR"},
    "XLF": {"sector": "Financials", "industry": "Sector ETF", "subsector": "Financial ETF", "theme": "Financials", "volatility_group": "MEDIUM", "correlation_cluster": "FINANCIALS_SECTOR"},
    "XLE": {"sector": "Energy", "industry": "Sector ETF", "subsector": "Energy ETF", "theme": "Energy Commodities", "volatility_group": "HIGH", "correlation_cluster": "ENERGY_OIL_GAS"},
    "XLV": {"sector": "Health Care", "industry": "Sector ETF", "subsector": "Health Care ETF", "theme": "Defensive Health Care", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_HEALTHCARE"},
    "XLI": {"sector": "Industrials", "industry": "Sector ETF", "subsector": "Industrials ETF", "theme": "Industrial Cyclicals", "volatility_group": "MEDIUM", "correlation_cluster": "INDUSTRIAL_CYCLICALS"},
    "SMH": {"sector": "Technology", "industry": "Semiconductor ETF", "subsector": "Semiconductors", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "SOXX": {"sector": "Technology", "industry": "Semiconductor ETF", "subsector": "Semiconductors", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "ARKK": {"sector": "Multi-Sector", "industry": "Thematic ETF", "subsector": "Disruptive Innovation", "theme": "High Growth Innovation", "volatility_group": "HIGH", "correlation_cluster": "HIGH_BETA_GROWTH"},
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 4)


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _coerce_classification(symbol: str, values: dict[str, Any], source: str) -> SymbolClassification:
    return SymbolClassification(
        symbol=symbol,
        sector=str(values.get("sector") or UNKNOWN),
        industry=str(values.get("industry") or UNKNOWN),
        subsector=str(values.get("subsector") or UNKNOWN),
        theme=str(values.get("theme") or UNKNOWN),
        volatility_group=str(values.get("volatility_group") or values.get("volatility") or "MEDIUM").upper(),
        correlation_cluster=str(values.get("correlation_cluster") or UNKNOWN),
        source=source,
    )


def _config_cache() -> dict[str, dict[str, Any]]:
    cache = getattr(config, "SYMBOL_INTELLIGENCE_CACHE", {}) or getattr(config, "SYMBOL_CLASSIFICATION_CACHE", {}) or {}
    return {normalize_symbol(symbol): value for symbol, value in cache.items() if isinstance(value, dict)} if isinstance(cache, dict) else {}


def classify_symbol(symbol: str, cached_enrichment: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return SymbolClassification(symbol=UNKNOWN).as_dict()

    overrides = getattr(config, "SYMBOL_INTELLIGENCE_OVERRIDES", {}) or {}
    if isinstance(overrides, dict) and isinstance(overrides.get(normalized), dict):
        return _coerce_classification(normalized, overrides[normalized], "static_override").as_dict()

    sector_overrides = getattr(config, "SYMBOL_SECTOR_OVERRIDES", {}) or {}
    if isinstance(sector_overrides, dict) and normalized in sector_overrides:
        return _coerce_classification(normalized, {"sector": sector_overrides[normalized]}, "sector_override").as_dict()

    if normalized in STATIC_CLASSIFICATIONS:
        return _coerce_classification(normalized, STATIC_CLASSIFICATIONS[normalized], "static_mapping").as_dict()

    if normalized in ETF_THEME_CLASSIFICATIONS:
        return _coerce_classification(normalized, ETF_THEME_CLASSIFICATIONS[normalized], "etf_theme_inference").as_dict()

    merged_cache = _config_cache()
    if cached_enrichment:
        merged_cache.update({normalize_symbol(k): v for k, v in cached_enrichment.items() if isinstance(v, dict)})
    if normalized in merged_cache:
        return _coerce_classification(normalized, merged_cache[normalized], "cached_enrichment").as_dict()

    return SymbolClassification(symbol=normalized).as_dict()


def classify_symbols(symbols: list[str], cached_enrichment: dict[str, dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    return {normalize_symbol(symbol): classify_symbol(symbol, cached_enrichment) for symbol in symbols if normalize_symbol(symbol)}


def _position_value(position: dict[str, Any]) -> tuple[str, float, float, float]:
    symbol = normalize_symbol(position.get("symbol"))
    quantity = safe_float(position.get("quantity"))
    price = safe_float(position.get("current_price"), safe_float(position.get("buy_price") or position.get("entry_price")))
    return symbol, quantity, price, max(0.0, quantity * price)


def aggregate_exposure(positions: list[dict[str, Any]], group_key: str, account_equity: float, cached_enrichment: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    symbols: dict[str, list[str]] = {}
    for position in positions or []:
        if str(position.get("status") or "OPEN").upper() != "OPEN":
            continue
        symbol, _, _, value = _position_value(position)
        if not symbol or value <= 0:
            continue
        classification = classify_symbol(symbol, cached_enrichment)
        group = str(classification.get(group_key) or UNKNOWN)
        totals[group] = totals.get(group, 0.0) + value
        symbols.setdefault(group, []).append(symbol)

    return [
        {
            group_key: group,
            "market_value": round(value, 2),
            "exposure_percent": pct(value, account_equity),
            "symbols": sorted(set(symbols.get(group, []))),
        }
        for group, value in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]


def build_portfolio_summary(positions: list[dict[str, Any]], account_equity: float, cached_enrichment: dict[str, dict[str, Any]] | None = None, checked_at: str | None = None) -> dict[str, Any]:
    open_positions = [p for p in (positions or []) if str(p.get("status") or "OPEN").upper() == "OPEN"]
    symbols = [normalize_symbol(p.get("symbol")) for p in open_positions if normalize_symbol(p.get("symbol"))]
    classifications = classify_symbols(symbols, cached_enrichment)

    total_market_value = sum(_position_value(position)[3] for position in open_positions)
    exposure_by_sector = aggregate_exposure(open_positions, "sector", account_equity, cached_enrichment)
    exposure_by_industry = aggregate_exposure(open_positions, "industry", account_equity, cached_enrichment)
    exposure_by_theme = aggregate_exposure(open_positions, "theme", account_equity, cached_enrichment)
    correlation_clusters = aggregate_exposure(open_positions, "correlation_cluster", account_equity, cached_enrichment)

    largest_group_percent = max([safe_float(item.get("exposure_percent")) for item in exposure_by_sector + exposure_by_industry + correlation_clusters] or [0.0])
    total_exposure_percent = pct(total_market_value, account_equity)
    known_count = sum(1 for item in classifications.values() if item.get("sector") != UNKNOWN)
    classification_coverage_percent = pct(known_count, len(classifications)) if classifications else 100.0
    group_count = len([item for item in exposure_by_sector if safe_float(item.get("market_value")) > 0])
    concentration_percent = largest_group_percent
    diversification_score = round(max(0.0, min(100.0, 100.0 - concentration_percent + min(group_count, 6) * 3)), 2)

    return {
        "checked_at": checked_at or now_iso(),
        "read_only": True,
        "no_trading_actions": True,
        "classification_sources": ["static_mappings", "etf_theme_inference", "cached_enrichment", "fallback_unknown"],
        "classifications": classifications,
        "portfolio": {
            "account_equity": round(account_equity, 2),
            "total_market_value": round(total_market_value, 2),
            "total_exposure_percent": total_exposure_percent,
            "open_positions": len(open_positions),
            "classification_coverage_percent": classification_coverage_percent,
        },
        "exposure_by_sector": exposure_by_sector,
        "exposure_by_industry": exposure_by_industry,
        "exposure_by_theme": exposure_by_theme,
        "top_correlated_groups": correlation_clusters[:5],
        "correlation_clusters": correlation_clusters,
        "concentration_percent": round(concentration_percent, 4),
        "diversification_score": diversification_score,
    }


async def load_cached_enrichment() -> dict[str, dict[str, Any]]:
    raw = await database.get_app_state(ENRICHMENT_CACHE_KEY, "{}")
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        return {}
    return {normalize_symbol(symbol): value for symbol, value in parsed.items() if isinstance(value, dict)}


async def get_sector_intelligence(positions: list[dict[str, Any]] | None = None, account_equity: float | None = None) -> dict[str, Any]:
    if positions is None:
        positions = await database.get_open_positions()
    if account_equity is None:
        account_equity = safe_float(getattr(config, "VIRTUAL_TRADING_CAPITAL_USD", 5000.0), 5000.0)
    cached_enrichment = await load_cached_enrichment()
    return build_portfolio_summary(positions or [], account_equity, cached_enrichment)


async def get_symbol_intelligence(symbol: str) -> dict[str, Any]:
    cached_enrichment = await load_cached_enrichment()
    result = classify_symbol(symbol, cached_enrichment)
    result["read_only"] = True
    result["no_trading_actions"] = True
    return result

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
    classification_confidence: float = 0.0
    inferred_vs_static: str = "unknown"
    fallback_used: bool = True

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
            "classification_source": self.source,
            "confidence": round(self.classification_confidence, 4),
            "classification_confidence": round(self.classification_confidence, 4),
            "inferred_vs_static": self.inferred_vs_static,
            "fallback_used": self.fallback_used,
            "normalized_sector": normalize_sector(self.sector),
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


SECTOR_ALIASES = {
    "Health Care": "Healthcare",
    "Consumer Discretionary": "Consumer",
    "Consumer Staples": "Consumer",
    "Multi-Sector": "ETFs",
}

STATIC_CLASSIFICATIONS.update({
    # Technology / AI software and platforms
    "ORCL": {"sector": "Technology", "industry": "Software", "subsector": "Enterprise Software", "theme": "Cloud AI", "volatility_group": "MEDIUM", "correlation_cluster": "ENTERPRISE_SOFTWARE"},
    "CRM": {"sector": "Technology", "industry": "Software", "subsector": "Customer Software", "theme": "Enterprise SaaS", "volatility_group": "MEDIUM", "correlation_cluster": "ENTERPRISE_SOFTWARE"},
    "ADBE": {"sector": "Technology", "industry": "Software", "subsector": "Creative Software", "theme": "Enterprise SaaS", "volatility_group": "MEDIUM", "correlation_cluster": "ENTERPRISE_SOFTWARE"},
    "NOW": {"sector": "Technology", "industry": "Software", "subsector": "Workflow Automation", "theme": "Enterprise SaaS", "volatility_group": "MEDIUM", "correlation_cluster": "ENTERPRISE_SOFTWARE"},
    "PLTR": {"sector": "Technology", "industry": "Software", "subsector": "Data Analytics", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SOFTWARE"},
    "SNOW": {"sector": "Technology", "industry": "Software", "subsector": "Data Cloud", "theme": "Cloud AI", "volatility_group": "HIGH", "correlation_cluster": "AI_SOFTWARE"},
    "CRWD": {"sector": "Technology", "industry": "Software", "subsector": "Cybersecurity", "theme": "Cybersecurity", "volatility_group": "HIGH", "correlation_cluster": "CYBERSECURITY"},
    "PANW": {"sector": "Technology", "industry": "Software", "subsector": "Cybersecurity", "theme": "Cybersecurity", "volatility_group": "HIGH", "correlation_cluster": "CYBERSECURITY"},
    "ANET": {"sector": "Technology", "industry": "Communications Equipment", "subsector": "Cloud Networking", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_INFRASTRUCTURE"},
    "DELL": {"sector": "Technology", "industry": "Technology Hardware", "subsector": "Servers & PCs", "theme": "AI Infrastructure", "volatility_group": "MEDIUM", "correlation_cluster": "AI_INFRASTRUCTURE"},

    # Semiconductors
    "TSM": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Foundry", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "ASML": {"sector": "Technology", "industry": "Semiconductor Equipment", "subsector": "Lithography", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "SEMICAP_EQUIPMENT"},
    "AMAT": {"sector": "Technology", "industry": "Semiconductor Equipment", "subsector": "Wafer Fabrication", "theme": "AI Infrastructure", "volatility_group": "MEDIUM", "correlation_cluster": "SEMICAP_EQUIPMENT"},
    "LRCX": {"sector": "Technology", "industry": "Semiconductor Equipment", "subsector": "Etch & Deposition", "theme": "AI Infrastructure", "volatility_group": "MEDIUM", "correlation_cluster": "SEMICAP_EQUIPMENT"},
    "KLAC": {"sector": "Technology", "industry": "Semiconductor Equipment", "subsector": "Process Control", "theme": "AI Infrastructure", "volatility_group": "MEDIUM", "correlation_cluster": "SEMICAP_EQUIPMENT"},
    "MU": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Memory", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "QCOM": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Mobile Chips", "theme": "Connectivity", "volatility_group": "MEDIUM", "correlation_cluster": "SEMICONDUCTORS"},
    "INTC": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Processors", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "ARM": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Processor IP", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},
    "MRVL": {"sector": "Technology", "industry": "Semiconductors", "subsector": "Data Infrastructure Chips", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"},

    # Biotech / healthcare
    "LLY": {"sector": "Healthcare", "industry": "Pharmaceuticals", "subsector": "Metabolic Medicine", "theme": "Biopharma Growth", "volatility_group": "MEDIUM", "correlation_cluster": "BIOPHARMA"},
    "MRK": {"sector": "Healthcare", "industry": "Pharmaceuticals", "subsector": "Oncology", "theme": "Defensive Healthcare", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_HEALTHCARE"},
    "ABBV": {"sector": "Healthcare", "industry": "Biotechnology", "subsector": "Immunology", "theme": "Biopharma Growth", "volatility_group": "MEDIUM", "correlation_cluster": "BIOPHARMA"},
    "TMO": {"sector": "Healthcare", "industry": "Life Sciences Tools", "subsector": "Diagnostics & Tools", "theme": "Healthcare Tools", "volatility_group": "MEDIUM", "correlation_cluster": "LIFE_SCIENCES"},
    "DHR": {"sector": "Healthcare", "industry": "Life Sciences Tools", "subsector": "Diagnostics & Tools", "theme": "Healthcare Tools", "volatility_group": "MEDIUM", "correlation_cluster": "LIFE_SCIENCES"},
    "ISRG": {"sector": "Healthcare", "industry": "Health Care Equipment", "subsector": "Robotic Surgery", "theme": "Medical Technology", "volatility_group": "MEDIUM", "correlation_cluster": "MEDTECH"},
    "VRTX": {"sector": "Healthcare", "industry": "Biotechnology", "subsector": "Rare Disease", "theme": "Biotech", "volatility_group": "MEDIUM", "correlation_cluster": "BIOTECH"},
    "REGN": {"sector": "Healthcare", "industry": "Biotechnology", "subsector": "Biopharma", "theme": "Biotech", "volatility_group": "MEDIUM", "correlation_cluster": "BIOTECH"},
    "GILD": {"sector": "Healthcare", "industry": "Biotechnology", "subsector": "Antivirals & Oncology", "theme": "Biotech", "volatility_group": "MEDIUM", "correlation_cluster": "BIOTECH"},
    "BIIB": {"sector": "Healthcare", "industry": "Biotechnology", "subsector": "Neurology", "theme": "Biotech", "volatility_group": "HIGH", "correlation_cluster": "BIOTECH"},

    # Energy
    "COP": {"sector": "Energy", "industry": "Oil Gas & Consumable Fuels", "subsector": "Exploration & Production", "theme": "Energy Commodities", "volatility_group": "MEDIUM", "correlation_cluster": "ENERGY_OIL_GAS"},
    "SLB": {"sector": "Energy", "industry": "Energy Equipment & Services", "subsector": "Oilfield Services", "theme": "Energy Services", "volatility_group": "HIGH", "correlation_cluster": "ENERGY_SERVICES"},
    "EOG": {"sector": "Energy", "industry": "Oil Gas & Consumable Fuels", "subsector": "Exploration & Production", "theme": "Energy Commodities", "volatility_group": "MEDIUM", "correlation_cluster": "ENERGY_OIL_GAS"},
    "MPC": {"sector": "Energy", "industry": "Oil Gas & Consumable Fuels", "subsector": "Refining", "theme": "Energy Commodities", "volatility_group": "MEDIUM", "correlation_cluster": "ENERGY_REFINING"},

    # Financials
    "GS": {"sector": "Financials", "industry": "Capital Markets", "subsector": "Investment Banking", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "CAPITAL_MARKETS"},
    "MS": {"sector": "Financials", "industry": "Capital Markets", "subsector": "Investment Banking", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "CAPITAL_MARKETS"},
    "C": {"sector": "Financials", "industry": "Banks", "subsector": "Money Center Banks", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "BANKS"},
    "AXP": {"sector": "Financials", "industry": "Consumer Finance", "subsector": "Payment Cards", "theme": "Consumer Credit", "volatility_group": "MEDIUM", "correlation_cluster": "PAYMENTS"},
    "V": {"sector": "Financials", "industry": "Financial Services", "subsector": "Payment Networks", "theme": "Digital Payments", "volatility_group": "LOW", "correlation_cluster": "PAYMENTS"},
    "MA": {"sector": "Financials", "industry": "Financial Services", "subsector": "Payment Networks", "theme": "Digital Payments", "volatility_group": "LOW", "correlation_cluster": "PAYMENTS"},
    "BRK.B": {"sector": "Financials", "industry": "Diversified Financials", "subsector": "Insurance & Holdings", "theme": "Value Compounder", "volatility_group": "LOW", "correlation_cluster": "INSURANCE_FINANCIALS"},
    "SCHW": {"sector": "Financials", "industry": "Capital Markets", "subsector": "Brokerage", "theme": "Retail Brokerage", "volatility_group": "MEDIUM", "correlation_cluster": "CAPITAL_MARKETS"},

    # Industrials
    "RTX": {"sector": "Industrials", "industry": "Aerospace & Defense", "subsector": "Defense Systems", "theme": "Defense", "volatility_group": "MEDIUM", "correlation_cluster": "AEROSPACE_DEFENSE"},
    "LMT": {"sector": "Industrials", "industry": "Aerospace & Defense", "subsector": "Defense Prime", "theme": "Defense", "volatility_group": "LOW", "correlation_cluster": "AEROSPACE_DEFENSE"},
    "HON": {"sector": "Industrials", "industry": "Industrial Conglomerates", "subsector": "Automation & Aerospace", "theme": "Industrial Cyclicals", "volatility_group": "LOW", "correlation_cluster": "INDUSTRIAL_CYCLICALS"},
    "UNP": {"sector": "Industrials", "industry": "Transportation", "subsector": "Railroads", "theme": "Industrial Cyclicals", "volatility_group": "LOW", "correlation_cluster": "TRANSPORTATION"},
    "UPS": {"sector": "Industrials", "industry": "Transportation", "subsector": "Air Freight & Logistics", "theme": "Trade & Logistics", "volatility_group": "MEDIUM", "correlation_cluster": "TRANSPORTATION"},
    "DE": {"sector": "Industrials", "industry": "Machinery", "subsector": "Agricultural Machinery", "theme": "Industrial Cyclicals", "volatility_group": "MEDIUM", "correlation_cluster": "INDUSTRIAL_CYCLICALS"},

    # Consumer
    "HD": {"sector": "Consumer", "industry": "Specialty Retail", "subsector": "Home Improvement Retail", "theme": "Housing Consumer", "volatility_group": "LOW", "correlation_cluster": "CONSUMER_RETAIL"},
    "MCD": {"sector": "Consumer", "industry": "Restaurants", "subsector": "Quick Service", "theme": "Defensive Consumer", "volatility_group": "LOW", "correlation_cluster": "RESTAURANTS"},
    "NKE": {"sector": "Consumer", "industry": "Textiles Apparel & Luxury Goods", "subsector": "Athletic Apparel", "theme": "Consumer Brands", "volatility_group": "MEDIUM", "correlation_cluster": "CONSUMER_BRANDS"},
    "COST": {"sector": "Consumer", "industry": "Consumer Staples Distribution", "subsector": "Warehouse Retail", "theme": "Defensive Consumer", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_STAPLES"},
    "TGT": {"sector": "Consumer", "industry": "Consumer Staples Distribution", "subsector": "Discount Retail", "theme": "Defensive Consumer", "volatility_group": "MEDIUM", "correlation_cluster": "CONSUMER_RETAIL"},
    "LOW": {"sector": "Consumer", "industry": "Specialty Retail", "subsector": "Home Improvement Retail", "theme": "Housing Consumer", "volatility_group": "LOW", "correlation_cluster": "CONSUMER_RETAIL"},
    "SBUX": {"sector": "Consumer", "industry": "Restaurants", "subsector": "Coffee Chains", "theme": "Consumer Brands", "volatility_group": "MEDIUM", "correlation_cluster": "RESTAURANTS"},

    # Utilities / Materials / Communication Services
    "NEE": {"sector": "Utilities", "industry": "Electric Utilities", "subsector": "Renewable Electric", "theme": "Defensive Yield", "volatility_group": "LOW", "correlation_cluster": "UTILITIES_DEFENSIVE"},
    "DUK": {"sector": "Utilities", "industry": "Electric Utilities", "subsector": "Regulated Electric", "theme": "Defensive Yield", "volatility_group": "LOW", "correlation_cluster": "UTILITIES_DEFENSIVE"},
    "SO": {"sector": "Utilities", "industry": "Electric Utilities", "subsector": "Regulated Electric", "theme": "Defensive Yield", "volatility_group": "LOW", "correlation_cluster": "UTILITIES_DEFENSIVE"},
    "LIN": {"sector": "Materials", "industry": "Chemicals", "subsector": "Industrial Gases", "theme": "Industrial Materials", "volatility_group": "LOW", "correlation_cluster": "MATERIALS"},
    "APD": {"sector": "Materials", "industry": "Chemicals", "subsector": "Industrial Gases", "theme": "Industrial Materials", "volatility_group": "MEDIUM", "correlation_cluster": "MATERIALS"},
    "FCX": {"sector": "Materials", "industry": "Metals & Mining", "subsector": "Copper Mining", "theme": "Copper Electrification", "volatility_group": "HIGH", "correlation_cluster": "METALS_MINING"},
    "NEM": {"sector": "Materials", "industry": "Metals & Mining", "subsector": "Gold Mining", "theme": "Precious Metals", "volatility_group": "HIGH", "correlation_cluster": "GOLD_MINERS"},
    "DIS": {"sector": "Communication Services", "industry": "Entertainment", "subsector": "Media & Parks", "theme": "Streaming Media", "volatility_group": "MEDIUM", "correlation_cluster": "STREAMING_MEDIA"},
    "CMCSA": {"sector": "Communication Services", "industry": "Media", "subsector": "Cable & Broadband", "theme": "Connectivity Media", "volatility_group": "MEDIUM", "correlation_cluster": "TELECOM_MEDIA"},
    "T": {"sector": "Communication Services", "industry": "Telecommunication Services", "subsector": "Wireless & Broadband", "theme": "Defensive Connectivity", "volatility_group": "LOW", "correlation_cluster": "TELECOM_MEDIA"},
    "VZ": {"sector": "Communication Services", "industry": "Telecommunication Services", "subsector": "Wireless & Broadband", "theme": "Defensive Connectivity", "volatility_group": "LOW", "correlation_cluster": "TELECOM_MEDIA"},
})

ETF_THEME_CLASSIFICATIONS.update({
    "DIA": {"sector": "ETFs", "industry": "Broad Market ETF", "subsector": "Dow Jones Industrial Average", "theme": "US Large Cap", "volatility_group": "MEDIUM", "correlation_cluster": "BROAD_MARKET"},
    "VOO": {"sector": "ETFs", "industry": "Broad Market ETF", "subsector": "S&P 500", "theme": "US Large Cap", "volatility_group": "MEDIUM", "correlation_cluster": "BROAD_MARKET"},
    "VTI": {"sector": "ETFs", "industry": "Broad Market ETF", "subsector": "Total US Market", "theme": "US Broad Market", "volatility_group": "MEDIUM", "correlation_cluster": "BROAD_MARKET"},
    "IVV": {"sector": "ETFs", "industry": "Broad Market ETF", "subsector": "S&P 500", "theme": "US Large Cap", "volatility_group": "MEDIUM", "correlation_cluster": "BROAD_MARKET"},
    "XLY": {"sector": "Consumer", "industry": "Sector ETF", "subsector": "Consumer Discretionary ETF", "theme": "Consumer Cyclicals", "volatility_group": "MEDIUM", "correlation_cluster": "CONSUMER_RETAIL"},
    "XLP": {"sector": "Consumer", "industry": "Sector ETF", "subsector": "Consumer Staples ETF", "theme": "Defensive Consumer", "volatility_group": "LOW", "correlation_cluster": "DEFENSIVE_STAPLES"},
    "XLU": {"sector": "Utilities", "industry": "Sector ETF", "subsector": "Utilities ETF", "theme": "Defensive Yield", "volatility_group": "LOW", "correlation_cluster": "UTILITIES_DEFENSIVE"},
    "XLB": {"sector": "Materials", "industry": "Sector ETF", "subsector": "Materials ETF", "theme": "Industrial Materials", "volatility_group": "MEDIUM", "correlation_cluster": "MATERIALS"},
    "XLC": {"sector": "Communication Services", "industry": "Sector ETF", "subsector": "Communication Services ETF", "theme": "Digital Media", "volatility_group": "MEDIUM", "correlation_cluster": "DIGITAL_ADS"},
    "IBB": {"sector": "Healthcare", "industry": "Biotechnology ETF", "subsector": "Biotechnology", "theme": "Biotech", "volatility_group": "HIGH", "correlation_cluster": "BIOTECH"},
    "XBI": {"sector": "Healthcare", "industry": "Biotechnology ETF", "subsector": "Biotechnology", "theme": "Biotech", "volatility_group": "HIGH", "correlation_cluster": "BIOTECH"},
    "BOTZ": {"sector": "Technology", "industry": "Thematic ETF", "subsector": "Robotics & AI", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SOFTWARE"},
    "AIQ": {"sector": "Technology", "industry": "Thematic ETF", "subsector": "Artificial Intelligence", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SOFTWARE"},
})

ETF_SYMBOL_PATTERNS = ("ETF", "FUND", "TRUST", "ISHARES", "VANGUARD", "SPDR", "INVESCO", "DIREXION", "PROSHARES", "ARK ", "ARKK", "GLOBAL X")
SYMBOL_PATTERN_CLASSIFICATIONS = {
    "X": {"sector": "Materials", "industry": "Metals & Mining", "subsector": "Steel", "theme": "Industrial Materials", "volatility_group": "HIGH", "correlation_cluster": "MATERIALS"},
}
KEYWORD_CLASSIFICATIONS = [
    (("semiconductor", "chip", "micro devices", "silicon", "lithography"), {"sector": "Technology", "industry": "Semiconductors", "subsector": "Semiconductors", "theme": "AI Infrastructure", "volatility_group": "HIGH", "correlation_cluster": "AI_SEMICONDUCTORS"}),
    (("biotech", "biotechnology", "therapeutics", "pharma", "oncology", "genomics", "biosciences"), {"sector": "Healthcare", "industry": "Biotechnology", "subsector": "Biopharma", "theme": "Biotech", "volatility_group": "HIGH", "correlation_cluster": "BIOTECH"}),
    (("artificial intelligence", " ai ", "data cloud", "analytics", "software", "cybersecurity", "cloud"), {"sector": "Technology", "industry": "Software", "subsector": "Software", "theme": "Artificial Intelligence", "volatility_group": "HIGH", "correlation_cluster": "AI_SOFTWARE"}),
    (("health", "medical", "surgical", "diagnostic", "hospital"), {"sector": "Healthcare", "industry": "Health Care", "subsector": "Healthcare Services", "theme": "Defensive Healthcare", "volatility_group": "MEDIUM", "correlation_cluster": "DEFENSIVE_HEALTHCARE"}),
    (("oil", "gas", "energy", "petroleum", "midstream", "pipeline", "solar"), {"sector": "Energy", "industry": "Energy", "subsector": "Energy", "theme": "Energy Commodities", "volatility_group": "HIGH", "correlation_cluster": "ENERGY_OIL_GAS"}),
    (("bank", "financial", "capital", "asset management", "insurance", "payments"), {"sector": "Financials", "industry": "Financial Services", "subsector": "Financial Services", "theme": "Credit Cycle", "volatility_group": "MEDIUM", "correlation_cluster": "FINANCIALS_SECTOR"}),
    (("aerospace", "defense", "industrial", "machinery", "rail", "logistics", "transport"), {"sector": "Industrials", "industry": "Industrials", "subsector": "Industrial Cyclicals", "theme": "Industrial Cyclicals", "volatility_group": "MEDIUM", "correlation_cluster": "INDUSTRIAL_CYCLICALS"}),
    (("retail", "consumer", "restaurant", "apparel", "foods", "beverage", "automotive"), {"sector": "Consumer", "industry": "Consumer", "subsector": "Consumer Products", "theme": "Consumer Brands", "volatility_group": "MEDIUM", "correlation_cluster": "CONSUMER_RETAIL"}),
    (("utility", "utilities", "electric", "water", "regulated"), {"sector": "Utilities", "industry": "Utilities", "subsector": "Regulated Utilities", "theme": "Defensive Yield", "volatility_group": "LOW", "correlation_cluster": "UTILITIES_DEFENSIVE"}),
    (("materials", "mining", "chemical", "copper", "gold", "steel", "lithium"), {"sector": "Materials", "industry": "Materials", "subsector": "Basic Materials", "theme": "Industrial Materials", "volatility_group": "HIGH", "correlation_cluster": "MATERIALS"}),
    (("communications", "telecom", "media", "entertainment", "streaming", "advertising"), {"sector": "Communication Services", "industry": "Communication Services", "subsector": "Media & Telecom", "theme": "Digital Media", "volatility_group": "MEDIUM", "correlation_cluster": "TELECOM_MEDIA"}),
]


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
    return str(symbol or "").strip().upper().replace("/", ".")


def normalize_sector(sector: str) -> str:
    sector_text = str(sector or UNKNOWN).strip() or UNKNOWN
    return SECTOR_ALIASES.get(sector_text, sector_text)


def _source_confidence(source: str, values: dict[str, Any]) -> float:
    if values.get("classification_confidence") is not None:
        return safe_float(values.get("classification_confidence"))
    if values.get("confidence") is not None:
        return safe_float(values.get("confidence"))
    defaults = {
        "static_override": 1.0,
        "sector_override": 0.9,
        "static_mapping": 0.98,
        "etf_static_mapping": 0.98,
        "etf_theme_inference": 0.98,
        "cached_enrichment": 0.88,
        "cached_metadata": 0.82,
        "company_name_keyword_inference": 0.7,
        "etf_name_detection": 0.72,
        "symbol_pattern_inference": 0.6,
        "fallback_unknown": 0.05,
    }
    return defaults.get(source, 0.5)


def _coerce_classification(symbol: str, values: dict[str, Any], source: str) -> SymbolClassification:
    sector = normalize_sector(str(values.get("sector") or UNKNOWN))
    inferred_vs_static = "static" if source in {"static_override", "sector_override", "static_mapping", "etf_static_mapping", "etf_theme_inference"} else "inferred"
    if sector == UNKNOWN:
        inferred_vs_static = "unknown"
    return SymbolClassification(
        symbol=symbol,
        sector=sector,
        industry=str(values.get("industry") or UNKNOWN),
        subsector=str(values.get("subsector") or UNKNOWN),
        theme=str(values.get("theme") or UNKNOWN),
        volatility_group=str(values.get("volatility_group") or values.get("volatility") or "MEDIUM").upper(),
        correlation_cluster=str(values.get("correlation_cluster") or UNKNOWN),
        source=source,
        classification_confidence=_source_confidence(source, values),
        inferred_vs_static=inferred_vs_static,
        fallback_used=source in {"fallback_unknown", "symbol_pattern_inference", "company_name_keyword_inference", "etf_name_detection"},
    )


def _config_cache() -> dict[str, dict[str, Any]]:
    cache = getattr(config, "SYMBOL_INTELLIGENCE_CACHE", {}) or getattr(config, "SYMBOL_CLASSIFICATION_CACHE", {}) or {}
    return {normalize_symbol(symbol): value for symbol, value in cache.items() if isinstance(value, dict)} if isinstance(cache, dict) else {}


def _merged_cache(cached_enrichment: dict[str, dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    merged_cache = _config_cache()
    if cached_enrichment:
        merged_cache.update({normalize_symbol(k): v for k, v in cached_enrichment.items() if isinstance(v, dict)})
    return merged_cache


def _metadata_text(values: dict[str, Any]) -> str:
    fields = [
        values.get("name"),
        values.get("company_name"),
        values.get("longName"),
        values.get("shortName"),
        values.get("description"),
        values.get("industry"),
        values.get("sector"),
        values.get("asset_type"),
        values.get("quoteType"),
    ]
    return f" {' '.join(str(item) for item in fields if item)} ".lower()


def _infer_from_metadata(symbol: str, metadata: dict[str, Any]) -> SymbolClassification | None:
    text = _metadata_text(metadata)
    if not text.strip():
        return None

    if any(pattern.lower() in text for pattern in ETF_SYMBOL_PATTERNS):
        sector = normalize_sector(str(metadata.get("sector") or "ETFs"))
        return _coerce_classification(
            symbol,
            {
                "sector": sector if sector != UNKNOWN else "ETFs",
                "industry": metadata.get("industry") or "ETF",
                "subsector": metadata.get("subsector") or metadata.get("category") or "Exchange Traded Fund",
                "theme": metadata.get("theme") or "ETF Exposure",
                "volatility_group": metadata.get("volatility_group") or "MEDIUM",
                "correlation_cluster": metadata.get("correlation_cluster") or "ETF_DIVERSIFIED",
            },
            "etf_name_detection",
        )

    if metadata.get("sector") and str(metadata.get("sector")).upper() != UNKNOWN:
        metadata_source = "cached_metadata" if any(metadata.get(key) for key in ("name", "company_name", "longName", "shortName", "description")) else "cached_enrichment"
        return _coerce_classification(symbol, metadata, metadata_source)

    for keywords, classification in KEYWORD_CLASSIFICATIONS:
        if any(keyword in text for keyword in keywords):
            merged = dict(classification)
            merged["industry"] = metadata.get("industry") or merged.get("industry")
            return _coerce_classification(symbol, merged, "company_name_keyword_inference")
    return None


def _infer_from_symbol_pattern(symbol: str) -> SymbolClassification | None:
    if symbol in SYMBOL_PATTERN_CLASSIFICATIONS:
        return _coerce_classification(symbol, SYMBOL_PATTERN_CLASSIFICATIONS[symbol], "symbol_pattern_inference")
    if symbol.endswith(("U", "W")) and len(symbol) >= 4:
        return None
    if symbol.startswith(("XL", "X", "I", "V")) and len(symbol) <= 5:
        return _coerce_classification(
            symbol,
            {"sector": "ETFs", "industry": "ETF", "subsector": "Detected ETF", "theme": "ETF Exposure", "volatility_group": "MEDIUM", "correlation_cluster": "ETF_DIVERSIFIED"},
            "symbol_pattern_inference",
        )
    return None


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

    merged_cache = _merged_cache(cached_enrichment)
    if normalized in merged_cache:
        cached_result = _infer_from_metadata(normalized, merged_cache[normalized])
        if cached_result:
            return cached_result.as_dict()
        return _coerce_classification(normalized, merged_cache[normalized], "cached_enrichment").as_dict()

    pattern_result = _infer_from_symbol_pattern(normalized)
    if pattern_result:
        return pattern_result.as_dict()

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
    confidence_totals: dict[str, float] = {}
    confidence_counts: dict[str, int] = {}
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
        confidence_totals[group] = confidence_totals.get(group, 0.0) + safe_float(classification.get("confidence"))
        confidence_counts[group] = confidence_counts.get(group, 0) + 1

    return [
        {
            group_key: group,
            "market_value": round(value, 2),
            "exposure_percent": pct(value, account_equity),
            "symbols": sorted(set(symbols.get(group, []))),
            "average_confidence": round(confidence_totals.get(group, 0.0) / max(1, confidence_counts.get(group, 0)), 4),
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

    known_sector_value = 0.0
    unknown_sector_value = 0.0
    for position in open_positions:
        symbol, _, _, value = _position_value(position)
        classification = classifications.get(symbol) or classify_symbol(symbol, cached_enrichment)
        if classification.get("sector") == UNKNOWN:
            unknown_sector_value += value
        else:
            known_sector_value += value

    known_sector_percentage = pct(known_sector_value, total_market_value) if total_market_value > 0 else 100.0
    unknown_sector_percentage = pct(unknown_sector_value, total_market_value) if total_market_value > 0 else 0.0
    known_sector_exposures = [item for item in exposure_by_sector if item.get("sector") != UNKNOWN]
    largest_known_sector_percent = max([safe_float(item.get("exposure_percent")) for item in known_sector_exposures] or [0.0])
    largest_group_percent = max([safe_float(item.get("exposure_percent")) for item in known_sector_exposures + exposure_by_industry + correlation_clusters if item.get("sector") != UNKNOWN and item.get("industry") != UNKNOWN and item.get("correlation_cluster") != UNKNOWN] or [0.0])
    total_exposure_percent = pct(total_market_value, account_equity)
    known_count = sum(1 for item in classifications.values() if item.get("sector") != UNKNOWN)
    classification_coverage_percent = pct(known_count, len(classifications)) if classifications else 100.0
    group_count = len([item for item in known_sector_exposures if safe_float(item.get("market_value")) > 0])
    concentration_percent = largest_group_percent
    diversification_score = round(max(0.0, min(100.0, 100.0 - concentration_percent + min(group_count, 6) * 3 - unknown_sector_percentage * 0.35)), 2)
    if unknown_sector_percentage <= 10 and classification_coverage_percent >= 80:
        diversification_quality = "HIGH" if diversification_score >= 75 else "MODERATE"
    elif unknown_sector_percentage <= 25:
        diversification_quality = "MODERATE"
    else:
        diversification_quality = "LOW"
    top_sectors = [item for item in known_sector_exposures[:5]]
    unknown_sector_rows = [item for item in exposure_by_sector if item.get("sector") == UNKNOWN and safe_float(item.get("market_value")) > 0]
    visible_sector_exposure = top_sectors + [item for item in unknown_sector_rows if unknown_sector_percentage > 10.0]

    return {
        "checked_at": checked_at or now_iso(),
        "read_only": True,
        "no_trading_actions": True,
        "classification_sources": ["static_mappings", "etf_static_mapping", "cached_metadata", "etf_name_detection", "company_name_keyword_inference", "symbol_pattern_inference", "fallback_unknown"],
        "classifications": classifications,
        "portfolio": {
            "account_equity": round(account_equity, 2),
            "total_market_value": round(total_market_value, 2),
            "total_exposure_percent": total_exposure_percent,
            "open_positions": len(open_positions),
            "classification_coverage_percent": classification_coverage_percent,
            "known_sector_percentage": known_sector_percentage,
            "unknown_sector_percentage": unknown_sector_percentage,
            "diversification_quality": diversification_quality,
        },
        "known_sector_percentage": known_sector_percentage,
        "unknown_sector_percentage": unknown_sector_percentage,
        "diversification_quality": diversification_quality,
        "largest_known_sector_percent": round(largest_known_sector_percent, 4),
        "top_sectors": top_sectors,
        "visible_sector_exposure": visible_sector_exposure,
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

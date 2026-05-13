import os
from dotenv import load_dotenv

load_dotenv()


# ==============================
# HELPERS
# ==============================
def get_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("true", "1", "yes", "y", "on")


# ==============================
# API
# ==============================
API_PROVIDER = get_str("API_PROVIDER", "polygon").lower()
API_KEY = get_str("API_KEY")


# ==============================
# TELEGRAM
# ==============================
TELEGRAM_BOT_TOKEN = get_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_str("TELEGRAM_CHAT_ID")


# ==============================
# TELEGRAM FILTERS
# ==============================
BUY_TELEGRAM_MIN_SCORE = get_float("BUY_TELEGRAM_MIN_SCORE", 80.0)
SELL_TELEGRAM_MIN_SCORE = get_float("SELL_TELEGRAM_MIN_SCORE", 80.0)
ALERT_COOLDOWN_MINUTES = get_int("ALERT_COOLDOWN_MINUTES", 240)


# ==============================
# SCAN SETTINGS
# ==============================
SCAN_MODE = get_str("SCAN_MODE", "interval").lower()
USE_DYNAMIC_SYMBOLS = get_bool("USE_DYNAMIC_SYMBOLS", True)
MAX_SYMBOLS_PER_SCAN = get_int("MAX_SYMBOLS_PER_SCAN", 30)

MIN_PRICE = get_float("MIN_PRICE", 5.0)
MIN_AVG_VOLUME = get_float("MIN_AVG_VOLUME", 500000.0)

BATCH_SIZE = get_int("BATCH_SIZE", 5)
REQUEST_DELAY_SECONDS = get_float("REQUEST_DELAY_SECONDS", 1.0)
SCAN_SYMBOL_TIMEOUT_SECONDS = get_float("SCAN_SYMBOL_TIMEOUT_SECONDS", 45.0)

SCAN_INTERVAL_MINUTES = get_int("SCAN_INTERVAL_MINUTES", 5)
RUN_SCAN_ON_STARTUP = get_bool("RUN_SCAN_ON_STARTUP", True)


# ==============================
# PORTFOLIO
# ==============================
MAX_OPEN_POSITIONS = get_int("MAX_OPEN_POSITIONS", 10)


# ==============================
# MONEY MANAGEMENT
# ==============================
ACCOUNT_BALANCE = get_float("ACCOUNT_BALANCE", 3000.0)
VIRTUAL_TRADING_CAPITAL_USD = get_float("VIRTUAL_TRADING_CAPITAL_USD", 5000.0)
RISK_PER_TRADE_PERCENT = get_float("RISK_PER_TRADE_PERCENT", 2.0)
MAX_RISK_PER_TRADE_PERCENT = get_float("MAX_RISK_PER_TRADE_PERCENT", 1.0)
MAX_POSITION_PERCENT = get_float("MAX_POSITION_PERCENT", 20.0)
MIN_CASH_RESERVE_PERCENT = get_float("MIN_CASH_RESERVE_PERCENT", 10.0)
MIN_TRADE_USD = get_float("MIN_TRADE_USD", 50.0)
ALLOW_FRACTIONAL_SHARES = get_bool("ALLOW_FRACTIONAL_SHARES", False)


# ==============================
# TECHNICALS
# ==============================
RSI_BUY_MIN = get_float("RSI_BUY_MIN", 50.0)
RSI_BUY_MAX = get_float("RSI_BUY_MAX", 70.0)
RSI_SELL_MAX = get_float("RSI_SELL_MAX", 45.0)


# ==============================
# RISK
# ==============================
MAX_RISK_PERCENT = get_float("MAX_RISK_PERCENT", 8.0)
MIN_RR_RATIO = get_float("MIN_RR_RATIO", 2.0)
STOP_LOSS_BUFFER = get_float("STOP_LOSS_BUFFER", 0.98)
ATR_STOP_MULTIPLIER = get_float("ATR_STOP_MULTIPLIER", 1.5)

# ==============================
# PORTFOLIO RISK ENGINE
# ==============================
MAX_TOTAL_EXPOSURE_PERCENT = get_float("MAX_TOTAL_EXPOSURE_PERCENT", 80.0)
MAX_SYMBOL_EXPOSURE_PERCENT = get_float("MAX_SYMBOL_EXPOSURE_PERCENT", 25.0)
MAX_SINGLE_SYMBOL_EXPOSURE = get_float("MAX_SINGLE_SYMBOL_EXPOSURE", MAX_SYMBOL_EXPOSURE_PERCENT)
MAX_SECTOR_EXPOSURE_PERCENT = get_float("MAX_SECTOR_EXPOSURE_PERCENT", 45.0)
MAX_DAILY_DRAWDOWN_PERCENT = get_float("MAX_DAILY_DRAWDOWN_PERCENT", 5.0)
MAX_ACCOUNT_UTILIZATION_PERCENT = get_float("MAX_ACCOUNT_UTILIZATION_PERCENT", 90.0)
PORTFOLIO_RISK_REFRESH_SECONDS = get_int("PORTFOLIO_RISK_REFRESH_SECONDS", 30)
HIGH_VOLATILITY_REDUCTION = get_float("HIGH_VOLATILITY_REDUCTION", 0.5)
LOW_LIQUIDITY_REDUCTION = get_float("LOW_LIQUIDITY_REDUCTION", 0.5)
MICRO_SIZE_THRESHOLD = get_float("MICRO_SIZE_THRESHOLD", 0.2)
POSITION_SIZING_REFRESH_SECONDS = get_int("POSITION_SIZING_REFRESH_SECONDS", 30)
SYMBOL_SECTOR_OVERRIDES = {}



# ==============================
# MARKET REGIME ENGINE
# ==============================
REGIME_VIX_WARNING = get_float("REGIME_VIX_WARNING", 25.0)
REGIME_VIX_DANGER = get_float("REGIME_VIX_DANGER", 35.0)
REGIME_DRAWDOWN_WARNING = get_float("REGIME_DRAWDOWN_WARNING", 5.0)
REGIME_DRAWDOWN_BLOCK = get_float("REGIME_DRAWDOWN_BLOCK", 10.0)
REGIME_BREADTH_WARNING = get_float("REGIME_BREADTH_WARNING", 45.0)
REGIME_BREADTH_DANGER = get_float("REGIME_BREADTH_DANGER", 35.0)
REGIME_REFRESH_SECONDS = get_int("REGIME_REFRESH_SECONDS", 60)

# ==============================
# EXECUTION QUALITY
# ==============================
MAX_SPREAD_PERCENT = get_float("MAX_SPREAD_PERCENT", 3.0)
MAX_SPREAD_DOLLARS = get_float("MAX_SPREAD_DOLLARS", 0.50)
MIN_AVERAGE_VOLUME = get_float("MIN_AVERAGE_VOLUME", 500000.0)
MIN_DOLLAR_VOLUME = get_float("MIN_DOLLAR_VOLUME", 5000000.0)
MIN_RELATIVE_VOLUME = get_float("MIN_RELATIVE_VOLUME", 1.0)
MAX_SLIPPAGE_ESTIMATE = get_float("MAX_SLIPPAGE_ESTIMATE", 2.0)
MAX_INTRADAY_VOLATILITY = get_float("MAX_INTRADAY_VOLATILITY", 6.0)
MAX_CANDLE_EXPANSION_PERCENT = get_float("MAX_CANDLE_EXPANSION_PERCENT", 250.0)

# ==============================
# SYMBOLS FALLBACK
# ==============================
SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "GOOGL", "TSLA", "AVGO", "AMD", "NFLX",
]


# ==============================
# DATABASE
# ==============================
if os.path.exists("/var/data"):
    DEFAULT_DB_PATH = "/var/data/stock_alerts.db"
else:
    DEFAULT_DB_PATH = "stock_alerts.db"

DB_PATH = get_str("DB_PATH", DEFAULT_DB_PATH)

print("✅ DB PATH:", DB_PATH)


# ==============================
# IBKR
# ==============================
IBKR_HOST = get_str("IBKR_HOST", "127.0.0.1")
IBKR_PORT = get_int("IBKR_PORT", 7497)
IBKR_CLIENT_ID = get_int("IBKR_CLIENT_ID", 20)
IBKR_PAPER_TRADING = get_bool("IBKR_PAPER_TRADING", True)
IBKR_ENABLE_REAL_TRADING = get_bool("IBKR_ENABLE_REAL_TRADING", False)
IBKR_MARKET_DATA_TYPE = get_int("IBKR_MARKET_DATA_TYPE", 3)

# ==============================
# TRADING MODE
# ==============================
TRADING_MODE = get_str("TRADING_MODE", "OFF").upper()
AUTO_SEND_ORDERS = get_bool("AUTO_SEND_ORDERS", False)
REQUIRE_MANUAL_CONFIRMATION = get_bool("REQUIRE_MANUAL_CONFIRMATION", True)

# ==============================
# MARKET HOURS GUARD
# ==============================
ENABLE_MARKET_HOURS_GUARD = get_bool("ENABLE_MARKET_HOURS_GUARD", True)
MARKET_TIMEZONE = get_str("MARKET_TIMEZONE", "America/New_York")
MARKET_OPEN_TIME = get_str("MARKET_OPEN_TIME", "09:30")
MARKET_CLOSE_TIME = get_str("MARKET_CLOSE_TIME", "16:00")

MAX_DAILY_LOSS_PERCENT = get_float(
    "MAX_DAILY_LOSS_PERCENT",
    5.0,
)

ENABLE_GLOBAL_RISK_PROTECTION = get_bool(
    "ENABLE_GLOBAL_RISK_PROTECTION",
    True,
)

# ==============================
# RECOVERY MANAGER
# ==============================
RECOVERY_CHECK_INTERVAL_SECONDS = get_int("RECOVERY_CHECK_INTERVAL_SECONDS", 30)
RECOVERY_HEARTBEAT_DEGRADED_SECONDS = get_int("RECOVERY_HEARTBEAT_DEGRADED_SECONDS", 60)
RECOVERY_HEARTBEAT_BLOCK_BUY_SECONDS = get_int("RECOVERY_HEARTBEAT_BLOCK_BUY_SECONDS", 120)
RECOVERY_POSITION_STALE_SECONDS = get_int("RECOVERY_POSITION_STALE_SECONDS", 180)

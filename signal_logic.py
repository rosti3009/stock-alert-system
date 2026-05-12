from __future__ import annotations

from config import RSI_BUY_MIN, RSI_BUY_MAX, RSI_SELL_MAX
from risk_manager import calculate_risk


def evaluate_signal(ind: dict) -> tuple[str, dict | None, list[str]]:
    price = ind.get("price")
    rsi = ind.get("rsi")
    ma20 = ind.get("ma20")
    ma50 = ind.get("ma50")
    ma200 = ind.get("ma200")
    volume = ind.get("volume")
    avg_volume = ind.get("avg_volume")
    atr = ind.get("atr")
    trend = ind.get("trend")

    if price is None or ma20 is None or ma50 is None or rsi is None:
        return "NEUTRAL", None, ["Missing required indicators"]

    # ==============================
    # VOLUME FILTER
    # ==============================
    volume_ok = (
        volume is not None
        and avg_volume is not None
        and avg_volume > 0
        and volume >= avg_volume * 1.30
    )

    volume_strong = (
        volume is not None
        and avg_volume is not None
        and avg_volume > 0
        and volume >= avg_volume * 1.80
    )

    # ==============================
    # BULLISH STRUCTURE
    # ==============================
    trend_ok = trend in ("Strong Bullish", "Bullish")

    bullish_ma = (
        price > ma50
        and (ma200 is None or ma50 > ma200)
    )

    bullish_short = (
        ma20 is not None
        and price > ma20 > ma50
    )

    # RSI_BUY_MIN / RSI_BUY_MAX נשארים מהקונפיג,
    # אבל מוסיפים טווח איכות כדי לא לקבל BUY חלש מדי.
    rsi_config_ok = RSI_BUY_MIN <= rsi <= RSI_BUY_MAX
    momentum_ok = 52 <= rsi <= 68

    # ==============================
    # BUY LOGIC — STRICTER
    # ==============================
    if bullish_ma and bullish_short and trend_ok and rsi_config_ok and momentum_ok and volume_ok:
        risk = calculate_risk(price, ma50, atr)

        reasons = [
            "Strong bullish trend confirmed",
            "Price above MA20 and MA50",
            "MA20 above MA50",
            "Healthy RSI momentum",
            "Volume confirmed above average",
        ]

        if volume_strong:
            reasons.append("Strong volume participation")

        if risk.get("risk_ok"):
            reasons.append("Risk rules approved")
            return "BUY", risk, reasons

        return "NEUTRAL", risk, reasons + ["Setup found but risk is too wide"]

    # ==============================
    # SELL LOGIC — LESS NOISE
    # ==============================
    bearish_ma = (
        price < ma50
        or (ma200 is not None and ma50 < ma200)
    )

    bearish_short = (
        ma20 is not None
        and price < ma20 < ma50
    )

    rsi_sell = rsi < RSI_SELL_MAX

    if bearish_ma and bearish_short and rsi_sell:
        reasons = [
            "Bearish MA structure",
            "Short-term bearish alignment",
            "RSI below sell threshold",
        ]

        if volume_ok:
            reasons.append("Volume confirms bearish move")

        return "SELL", None, reasons

    # ==============================
    # NEUTRAL REASONS
    # ==============================
    neutral_reasons = []

    if not bullish_ma:
        neutral_reasons.append("No bullish MA confirmation")

    if not bullish_short:
        neutral_reasons.append("No short-term bullish alignment")

    if not trend_ok:
        neutral_reasons.append("Trend is not bullish enough")

    if not momentum_ok:
        neutral_reasons.append("RSI momentum not in quality range")

    if not volume_ok:
        neutral_reasons.append("Volume below confirmation threshold")

    return "NEUTRAL", None, neutral_reasons or ["No clear signal"]
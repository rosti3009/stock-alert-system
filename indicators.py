from __future__ import annotations


def sma(values: list[float], period: int) -> float | None:
    if not values or len(values) < period:
        return None
    return sum(values[-period:]) / period


def rsi(closes: list[float], period: int = 14) -> float | None:
    """Wilder-style RSI."""
    if not closes or len(closes) < period + 1:
        return None

    gains: list[float] = []
    losses: list[float] = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def average_volume(volumes: list[float], period: int = 20) -> float | None:
    if not volumes or len(volumes) < period:
        return None
    return sum(volumes[-period:]) / period


def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return None

    true_ranges: list[float] = []

    for i in range(1, len(closes)):
        true_ranges.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )

    return sum(true_ranges[-period:]) / period


def percent_change(values: list[float], period: int) -> float | None:
    if not values or len(values) < period + 1:
        return None

    old_value = values[-period - 1]
    new_value = values[-1]

    if old_value == 0:
        return None

    return ((new_value - old_value) / old_value) * 100


def distance_percent(price: float | None, level: float | None) -> float | None:
    if price is None or level is None or level == 0:
        return None
    return ((price - level) / level) * 100


def detect_trend(
    price: float | None,
    ma20: float | None,
    ma50: float | None,
    ma200: float | None,
) -> str:
    if price is None or ma20 is None or ma50 is None:
        return "Neutral"

    if ma200 is not None and price > ma20 > ma50 > ma200:
        return "Strong Bullish"

    if price > ma20 > ma50:
        return "Bullish"

    if ma200 is not None and price < ma20 < ma50 < ma200:
        return "Strong Bearish"

    if price < ma20 < ma50:
        return "Bearish"

    return "Sideways"


def _round(v: float | None, digits: int = 4) -> float | None:
    return round(v, digits) if v is not None else None


def compute_indicators(data: dict) -> dict:
    closes = data.get("closes", []) or []
    highs = data.get("highs", []) or []
    lows = data.get("lows", []) or []
    volumes = data.get("volumes", []) or []

    price = data.get("current_price")

    if price is None and closes:
        price = closes[-1]

    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50)
    ma200 = sma(closes, 200)

    rsi_value = rsi(closes, 14)

    avg_vol = average_volume(volumes, 20)
    current_volume = volumes[-1] if volumes else None

    atr_value = atr(highs, lows, closes, 14)

    trend = detect_trend(price, ma20, ma50, ma200)

    momentum_5d = percent_change(closes, 5)
    momentum_20d = percent_change(closes, 20)
    momentum_60d = percent_change(closes, 60)

    volume_ratio = None
    if current_volume is not None and avg_vol is not None and avg_vol > 0:
        volume_ratio = current_volume / avg_vol

    atr_percent = None
    if atr_value is not None and price is not None and price > 0:
        atr_percent = (atr_value / price) * 100

    return {
        "symbol": data.get("symbol"),
        "price": _round(price),
        "rsi": rsi_value,
        "ma20": _round(ma20),
        "ma50": _round(ma50),
        "ma200": _round(ma200),
        "volume": current_volume,
        "avg_volume": round(avg_vol, 0) if avg_vol is not None else None,
        "volume_ratio": _round(volume_ratio, 2),
        "atr": _round(atr_value),
        "atr_percent": _round(atr_percent, 2),
        "trend": trend,
        "momentum_5d": _round(momentum_5d, 2),
        "momentum_20d": _round(momentum_20d, 2),
        "momentum_60d": _round(momentum_60d, 2),
        "distance_ma20": _round(distance_percent(price, ma20), 2),
        "distance_ma50": _round(distance_percent(price, ma50), 2),
        "distance_ma200": _round(distance_percent(price, ma200), 2),
    }


calculate_indicators = compute_indicators
from __future__ import annotations

import config


def calculate_weekly_score(row: dict) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    price = row.get("price")
    rsi = row.get("rsi")
    ma20 = row.get("ma20")
    ma50 = row.get("ma50")
    ma200 = row.get("ma200")
    volume = row.get("volume")
    avg_volume = row.get("avg_volume")
    trend = row.get("trend")
    risk_percent = row.get("risk_percent")
    rr_ratio = row.get("rr_ratio")
    signal = row.get("signal") or row.get("signal_type")

    if row.get("error"):
        return 0, ["Data error"]

    if signal == "SKIPPED":
        return 0, [row.get("skip_reason", "Skipped by filters")]

    if price is None or price < config.MIN_PRICE:
        return 0, ["Price below minimum"]

    if avg_volume is None or avg_volume < config.MIN_AVG_VOLUME:
        return 0, ["Average volume below minimum"]

    # ==============================
    # HARD FILTERS — REMOVE WEAK SETUPS
    # ==============================
    if signal != "BUY":
        return 0, ["Only BUY signals are eligible for weekly TOP ranking"]

    if rsi is None or not (52 <= rsi <= 68):
        return 0, ["RSI not in quality momentum range"]

    if volume is None or avg_volume is None or avg_volume <= 0:
        return 0, ["Missing volume data"]

    volume_ratio = volume / avg_volume

    if volume_ratio < 1.30:
        return 0, ["Volume confirmation too weak"]

    if ma20 is None or ma50 is None:
        return 0, ["Missing moving averages"]

    if not (price > ma20 > ma50):
        return 0, ["No short-term bullish MA alignment"]

    # ==============================
    # TREND SCORE — MAX 25
    # ==============================
    if trend == "Strong Bullish":
        score += 25
        reasons.append("Strong bullish trend structure")
    elif trend == "Bullish":
        score += 18
        reasons.append("Bullish trend structure")
    else:
        return 0, ["Trend is not bullish enough"]

    # ==============================
    # RSI / MOMENTUM — MAX 20
    # ==============================
    if 55 <= rsi <= 63:
        score += 20
        reasons.append("RSI in optimal momentum zone")
    elif 52 <= rsi < 55:
        score += 14
        reasons.append("RSI early momentum confirmation")
    elif 63 < rsi <= 68:
        score += 12
        reasons.append("RSI strong but elevated")

    # ==============================
    # VOLUME — MAX 20
    # ==============================
    if volume_ratio >= 2.0:
        score += 20
        reasons.append("Volume more than 2x average")
    elif volume_ratio >= 1.6:
        score += 16
        reasons.append("Strong volume above average")
    elif volume_ratio >= 1.3:
        score += 12
        reasons.append("Volume confirms setup")

    # ==============================
    # MA STRUCTURE — MAX 20
    # ==============================
    if ma200 is not None and price > ma20 > ma50 > ma200:
        score += 20
        reasons.append("Full bullish MA alignment")
    elif price > ma20 > ma50:
        score += 14
        reasons.append("Short and medium-term MA alignment")

    # ==============================
    # RISK QUALITY — MAX 15
    # ==============================
    if risk_percent is not None:
        if 1 <= risk_percent <= 4:
            score += 15
            reasons.append("Good stop-loss distance")
        elif 4 < risk_percent <= config.MAX_RISK_PERCENT:
            score += 8
            reasons.append("Risk acceptable but wider")
        elif risk_percent > config.MAX_RISK_PERCENT:
            return 0, ["Risk too wide"]

    if rr_ratio is not None:
        if rr_ratio >= 2:
            score += 8
            reasons.append("Strong reward/risk ratio")
        elif rr_ratio >= config.MIN_RR_RATIO:
            score += 5
            reasons.append("Reward/risk approved")
        else:
            return 0, ["Reward/risk too weak"]

    # BUY bonus
    score += 12
    reasons.append("System generated quality BUY signal")

    score = max(0, min(int(score), 100))

    return score, reasons


def rank_top_weekly_setups(rows: list[dict], limit: int = 10) -> list[dict]:
    candidates: list[dict] = []

    for row in rows:
        score, reasons = calculate_weekly_score(row)

        # Only strong setups enter TOP list
        if score < 60:
            continue

        enriched = dict(row)
        enriched["score"] = row.get("score", score)
        enriched["weekly_score"] = score
        enriched["weekly_reasons"] = reasons
        candidates.append(enriched)

    candidates.sort(
        key=lambda x: (
            x.get("weekly_score", 0),
            x.get("rr_ratio", 0) or 0,
            x.get("volume", 0) or 0,
        ),
        reverse=True,
    )

    top = candidates[:limit]

    for index, row in enumerate(top, start=1):
        row["weekly_rank"] = index

    return top
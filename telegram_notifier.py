from __future__ import annotations

import html
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


def _safe(value) -> str:
    return html.escape(str(value)) if value is not None else "-"


def _telegram_configured() -> bool:
    token = (TELEGRAM_BOT_TOKEN or "").strip()
    chat_id = (TELEGRAM_CHAT_ID or "").strip()

    if not token or not chat_id:
        return False

    fake_values = {
        "PUT_YOUR_BOT_TOKEN_HERE",
        "YOUR_BOT_TOKEN",
        "BOT_TOKEN",
        "PUT_YOUR_CHAT_ID_HERE",
        "YOUR_CHAT_ID",
        "CHAT_ID",
    }

    return token not in fake_values and chat_id not in fake_values


def _send_message(text: str) -> bool:
    if not _telegram_configured():
        print("[Telegram] Not configured")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN.strip()}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID.strip(),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True

    except Exception as exc:
        print(f"[Telegram] Error: {exc}")
        return False


# ==================================================
# BUY ALERT
# ==================================================

def send_buy_alert(
    ind: dict,
    risk: dict,
    reasons: list[str],
    score: int | None = None,
) -> bool:

    symbol = _safe(ind.get("symbol"))

    text = (
        f"🟢 <b>HIGH QUALITY BUY</b> ({symbol})\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 Price: <b>{_safe(ind.get('price'))}</b>\n"
        f"📊 RSI: <b>{_safe(ind.get('rsi'))}</b>\n"
        f"📈 Trend: <b>{_safe(ind.get('trend'))}</b>\n"
    )

    if score is not None:
        text += f"⭐ Score: <b>{_safe(score)}/100</b>\n"

    text += (
        f"\n🎯 <b>Trade Setup</b>\n"
        f"Entry: <b>{_safe(risk.get('entry_price'))}</b>\n"
        f"Stop Loss: <b>{_safe(risk.get('stop_loss'))}</b>\n"
        f"TP1: <b>{_safe(risk.get('take_profit_1'))}</b>\n"
        f"TP2: <b>{_safe(risk.get('take_profit_2'))}</b>\n"
        f"Risk: <b>{_safe(risk.get('risk_percent'))}%</b>\n"
        f"RR: <b>{_safe(risk.get('rr_ratio'))}</b>\n"
        f"\n⚙ <b>IBKR Setup</b>\n"
        f"BUY LMT\n"
        f"SELL STP\n"
        f"Partial TP Only\n"
        f"\n🧠 <b>Reasons</b>\n"
        + "\n".join(f"• {html.escape(str(r))}" for r in reasons[:7])
    )

    return _send_message(text)


# ==================================================
# SELL SIGNAL
# ==================================================

def send_sell_alert(
    ind: dict,
    reasons: list[str],
    score: int | None = None,
) -> bool:

    symbol = _safe(ind.get("symbol"))

    text = (
        f"🔴 <b>SELL SIGNAL</b> ({symbol})\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 Price: <b>{_safe(ind.get('price'))}</b>\n"
        f"📊 RSI: <b>{_safe(ind.get('rsi'))}</b>\n"
        f"📉 Trend: <b>{_safe(ind.get('trend'))}</b>\n"
    )

    if score is not None:
        text += f"⭐ Score: <b>{_safe(score)}/100</b>\n"

    text += (
        f"\n🧠 <b>Reasons</b>\n"
        + "\n".join(f"• {html.escape(str(r))}" for r in reasons[:7])
    )

    return _send_message(text)


# ==================================================
# POSITION MANAGEMENT ALERTS
# ==================================================

def send_position_alert(position: dict) -> bool:

    action = str(position.get("action", "HOLD")).strip().upper()

    allowed_actions = {
        "STOP_LOSS_HIT",
        "SELL_SIGNAL",
        "TAKE_PROFIT_1",
        "TAKE_PROFIT_2",
        "MOVE_STOP_TO_BREAKEVEN",
        "TRAILING_STOP_UPDATED",
        "EXIT_WARNING",
    }

    if action not in allowed_actions:
        return False

    symbol = _safe(position.get("symbol"))

    if action == "STOP_LOSS_HIT":
        emoji = "🔴"
        title = "STOP LOSS HIT"

    elif action == "SELL_SIGNAL":
        emoji = "🔴"
        title = "SELL SIGNAL"

    elif action == "TAKE_PROFIT_1":
        emoji = "🟢"
        title = "TAKE PROFIT 1 HIT"

    elif action == "TAKE_PROFIT_2":
        emoji = "🟢"
        title = "TAKE PROFIT 2 HIT"

    elif action == "MOVE_STOP_TO_BREAKEVEN":
        emoji = "🟡"
        title = "MOVE STOP TO BREAKEVEN"

    elif action == "TRAILING_STOP_UPDATED":
        emoji = "🟡"
        title = "TRAILING STOP UPDATED"

    elif action == "EXIT_WARNING":
        emoji = "🟠"
        title = "EXIT WARNING"

    else:
        return False

    text = (
        f"{emoji} <b>{title}</b> ({symbol})\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Action: <b>{_safe(action)}</b>\n"
        f"Buy Price: <b>{_safe(position.get('buy_price'))}</b>\n"
        f"Current Price: <b>{_safe(position.get('current_price'))}</b>\n"
        f"P/L %: <b>{_safe(position.get('profit_percent'))}%</b>\n"
        f"P/L Amount: <b>{_safe(position.get('profit_amount'))}</b>\n\n"
        f"Stop Loss: <b>{_safe(position.get('stop_loss'))}</b>\n"
        f"TP1: <b>{_safe(position.get('take_profit_1'))}</b>\n"
        f"TP2: <b>{_safe(position.get('take_profit_2'))}</b>\n\n"
        f"Reason:\n"
        f"• {_safe(position.get('reason'))}"
    )

    return _send_message(text)

# ==================================================
# WATCHDOG ALERT
# ==================================================

def send_watchdog_alert(message: str) -> bool:
    text = (
        "🛡️ <b>TWS Watchdog</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"{_safe(message)}"
    )
    return _send_message(text)

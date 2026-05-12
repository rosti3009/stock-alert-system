from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

EASTERN_TZ = ZoneInfo("America/New_York")

PREMARKET_START = time(4, 0)
MARKET_OPEN_TIME = time(9, 30)
POWER_HOUR_START = time(15, 0)
MARKET_CLOSE_TIME = time(16, 0)
MARKET_CLOSE_END = time(16, 15)
AFTER_HOURS_END = time(20, 0)


class SessionState(str, Enum):
    PREMARKET = "PREMARKET"
    MARKET_OPEN = "MARKET_OPEN"
    POWER_HOUR = "POWER_HOUR"
    MARKET_CLOSE = "MARKET_CLOSE"
    AFTER_HOURS = "AFTER_HOURS"
    CLOSED = "CLOSED"
    WEEKEND = "WEEKEND"
    HOLIDAY = "HOLIDAY"


@dataclass(frozen=True)
class SessionWindow:
    state: SessionState
    starts_at: datetime
    ends_at: datetime


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_eastern(value: datetime | None = None) -> datetime:
    current = value or now_utc()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(EASTERN_TZ)


def _dt(day: date, clock: time) -> datetime:
    return datetime.combine(day, clock, tzinfo=EASTERN_TZ)


def _observed_date(month: int, day: int, year: int) -> date:
    actual = date(year, month, day)
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    days_until = (weekday - current.weekday()) % 7
    return current + timedelta(days=days_until + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        current = date(year, 12, 31)
    else:
        current = date(year, month + 1, 1) - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _easter_date(year: int) -> date:
    """Return Gregorian Easter Sunday for the supplied year."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def market_holidays(year: int) -> set[date]:
    """NYSE full-day holidays for regular modern US equity sessions."""
    return {
        _observed_date(1, 1, year),
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
        _easter_date(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),  # Memorial Day
        _observed_date(6, 19, year),  # Juneteenth
        _observed_date(7, 4, year),  # Independence Day
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving Day
        _observed_date(12, 25, year),
    }


def is_market_holiday(day: date) -> bool:
    return day in market_holidays(day.year)


def is_market_day(day: date) -> bool:
    return day.weekday() < 5 and not is_market_holiday(day)


def _weekend_or_holiday_state(day: date) -> SessionState:
    if day.weekday() >= 5:
        return SessionState.WEEKEND
    return SessionState.HOLIDAY


def get_session_state(value: datetime | None = None) -> SessionState:
    eastern_now = to_eastern(value)
    day = eastern_now.date()
    clock = eastern_now.time()

    if not is_market_day(day):
        return _weekend_or_holiday_state(day)
    if PREMARKET_START <= clock < MARKET_OPEN_TIME:
        return SessionState.PREMARKET
    if MARKET_OPEN_TIME <= clock < POWER_HOUR_START:
        return SessionState.MARKET_OPEN
    if POWER_HOUR_START <= clock < MARKET_CLOSE_TIME:
        return SessionState.POWER_HOUR
    if MARKET_CLOSE_TIME <= clock < MARKET_CLOSE_END:
        return SessionState.MARKET_CLOSE
    if MARKET_CLOSE_END <= clock < AFTER_HOURS_END:
        return SessionState.AFTER_HOURS
    return SessionState.CLOSED


def _next_market_day(start_day: date) -> date:
    day = start_day
    while not is_market_day(day):
        day += timedelta(days=1)
    return day


def _session_window_for(value: datetime | None = None) -> SessionWindow:
    eastern_now = to_eastern(value)
    day = eastern_now.date()
    state = get_session_state(eastern_now)

    if state in (SessionState.WEEKEND, SessionState.HOLIDAY):
        next_day = _next_market_day(day + timedelta(days=1))
        return SessionWindow(state, _dt(day, time.min), _dt(next_day, PREMARKET_START))

    if state == SessionState.PREMARKET:
        return SessionWindow(state, _dt(day, PREMARKET_START), _dt(day, MARKET_OPEN_TIME))
    if state == SessionState.MARKET_OPEN:
        return SessionWindow(state, _dt(day, MARKET_OPEN_TIME), _dt(day, POWER_HOUR_START))
    if state == SessionState.POWER_HOUR:
        return SessionWindow(state, _dt(day, POWER_HOUR_START), _dt(day, MARKET_CLOSE_TIME))
    if state == SessionState.MARKET_CLOSE:
        return SessionWindow(state, _dt(day, MARKET_CLOSE_TIME), _dt(day, MARKET_CLOSE_END))
    if state == SessionState.AFTER_HOURS:
        return SessionWindow(state, _dt(day, MARKET_CLOSE_END), _dt(day, AFTER_HOURS_END))

    next_day = _next_market_day(day + timedelta(days=1))
    if eastern_now.time() < PREMARKET_START:
        return SessionWindow(state, _dt(day, time.min), _dt(day, PREMARKET_START))
    return SessionWindow(state, _dt(day, AFTER_HOURS_END), _dt(next_day, PREMARKET_START))


def permissions_for_state(state: SessionState | str) -> dict[str, bool]:
    normalized = SessionState(state)
    return {
        "trading_allowed": normalized in {
            SessionState.PREMARKET,
            SessionState.MARKET_OPEN,
            SessionState.POWER_HOUR,
            SessionState.MARKET_CLOSE,
            SessionState.AFTER_HOURS,
        },
        "scan_allowed": normalized in {
            SessionState.PREMARKET,
            SessionState.MARKET_OPEN,
            SessionState.POWER_HOUR,
        },
        "buy_allowed": normalized in {
            SessionState.MARKET_OPEN,
            SessionState.POWER_HOUR,
        },
        "sell_allowed": normalized not in {
            SessionState.CLOSED,
            SessionState.WEEKEND,
            SessionState.HOLIDAY,
        },
    }


def get_session_status(value: datetime | None = None) -> dict:
    eastern_now = to_eastern(value)
    window = _session_window_for(eastern_now)
    state = window.state
    market_day = _next_market_day(eastern_now.date()) if is_market_day(eastern_now.date()) else _next_market_day(eastern_now.date() + timedelta(days=1))
    market_open = _dt(market_day, MARKET_OPEN_TIME)
    market_close = _dt(market_day, MARKET_CLOSE_TIME)

    return {
        "current_session": state.value,
        "state": state.value,
        "timezone": "America/New_York",
        "market_clock": eastern_now.isoformat(),
        "server_clock_utc": eastern_now.astimezone(timezone.utc).isoformat(),
        "next_transition": {
            "at": window.ends_at.isoformat(),
            "to": get_session_state(window.ends_at).value,
        },
        "market_open": market_open.isoformat(),
        "market_close": market_close.isoformat(),
        **permissions_for_state(state),
    }


_latest_status: dict | None = None


def refresh_session_status(value: datetime | None = None) -> dict:
    global _latest_status
    _latest_status = get_session_status(value)
    return _latest_status


def get_cached_session_status() -> dict:
    return _latest_status or refresh_session_status()

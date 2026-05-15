from __future__ import annotations

import unittest
from unittest.mock import patch
from datetime import datetime
from zoneinfo import ZoneInfo

import config
from trading_safety import get_market_hours_status, require_paper_auto_trading_allowed


class TradingSafetyGateTests(unittest.TestCase):
    def setUp(self):
        self.original = {
            "TRADING_MODE": config.TRADING_MODE,
            "AUTO_SEND_ORDERS": config.AUTO_SEND_ORDERS,
            "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
            "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
            "IBKR_PORT": config.IBKR_PORT,
            "ENABLE_MARKET_HOURS_GUARD": config.ENABLE_MARKET_HOURS_GUARD,
            "MARKET_TIMEZONE": config.MARKET_TIMEZONE,
            "MARKET_OPEN_TIME": config.MARKET_OPEN_TIME,
            "MARKET_CLOSE_TIME": config.MARKET_CLOSE_TIME,
        }
        config.TRADING_MODE = "PAPER"
        config.AUTO_SEND_ORDERS = True
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.IBKR_PORT = 7497

    def tearDown(self):
        for key, value in self.original.items():
            setattr(config, key, value)

    def assert_blocked(self, expected_message: str) -> None:
        with self.assertRaises(RuntimeError) as raised:
            require_paper_auto_trading_allowed("TEST")
        self.assertEqual(str(raised.exception), expected_message)

    def test_safe_paper_auto_configuration_is_allowed(self):
        with patch("watchdog.get_watchdog_status_sync", return_value={"trading_blocked": False}):
            require_paper_auto_trading_allowed("TEST")

    def test_live_trading_is_blocked(self):
        config.IBKR_ENABLE_REAL_TRADING = True
        self.assert_blocked("TEST blocked: LIVE trading is enabled")

    def test_wrong_port_is_blocked(self):
        config.IBKR_PORT = 7496
        self.assert_blocked("TEST blocked: IBKR port is not Paper port 7497")

    def test_paper_disabled_is_blocked(self):
        config.IBKR_PAPER_TRADING = False
        self.assert_blocked("TEST blocked: IBKR_PAPER_TRADING is false")

    def test_auto_orders_disabled_is_blocked(self):
        config.AUTO_SEND_ORDERS = False
        self.assert_blocked("TEST blocked: AUTO_SEND_ORDERS is false")

    def test_trading_mode_off_is_blocked(self):
        config.TRADING_MODE = "OFF"
        self.assert_blocked("TEST blocked: TRADING_MODE is OFF")

    def test_market_hours_guard_allows_inside_regular_session(self):
        config.ENABLE_MARKET_HOURS_GUARD = True
        config.MARKET_TIMEZONE = "America/New_York"
        config.MARKET_OPEN_TIME = "09:30"
        config.MARKET_CLOSE_TIME = "16:00"

        status = get_market_hours_status(
            datetime(2026, 5, 13, 10, 0, tzinfo=ZoneInfo("America/New_York"))
        )

        self.assertTrue(status["allowed"])
        self.assertEqual(status["reason"].split(" (")[0], "US regular market is open")

    def test_market_hours_guard_blocks_before_open(self):
        config.ENABLE_MARKET_HOURS_GUARD = True
        config.MARKET_TIMEZONE = "America/New_York"
        config.MARKET_OPEN_TIME = "09:30"
        config.MARKET_CLOSE_TIME = "16:00"

        status = get_market_hours_status(
            datetime(2026, 5, 13, 9, 29, tzinfo=ZoneInfo("America/New_York"))
        )

        self.assertFalse(status["allowed"])
        self.assertIn("not open yet", status["reason"])

    def test_market_hours_guard_blocks_after_close(self):
        config.ENABLE_MARKET_HOURS_GUARD = True
        config.MARKET_TIMEZONE = "America/New_York"
        config.MARKET_OPEN_TIME = "09:30"
        config.MARKET_CLOSE_TIME = "16:00"

        status = get_market_hours_status(
            datetime(2026, 5, 13, 16, 1, tzinfo=ZoneInfo("America/New_York"))
        )

        self.assertFalse(status["allowed"])
        self.assertIn("closed after regular session", status["reason"])

    def test_market_hours_guard_blocks_weekend(self):
        config.ENABLE_MARKET_HOURS_GUARD = True
        config.MARKET_TIMEZONE = "America/New_York"
        config.MARKET_OPEN_TIME = "09:30"
        config.MARKET_CLOSE_TIME = "16:00"

        status = get_market_hours_status(
            datetime(2026, 5, 16, 10, 0, tzinfo=ZoneInfo("America/New_York"))
        )

        self.assertFalse(status["allowed"])
        self.assertTrue(status["is_weekend"])
        self.assertIn("weekends", status["reason"])


if __name__ == "__main__":
    unittest.main()

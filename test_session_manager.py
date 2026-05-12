from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

import session_manager
from session_manager import SessionState

NY = ZoneInfo("America/New_York")


class SessionManagerTests(unittest.TestCase):
    def status_at(self, value: datetime) -> dict:
        return session_manager.get_session_status(value)

    def test_premarket_allows_scans_and_sells_but_not_buys(self):
        status = self.status_at(datetime(2026, 5, 12, 8, 0, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.PREMARKET.value)
        self.assertTrue(status["trading_allowed"])
        self.assertTrue(status["scan_allowed"])
        self.assertFalse(status["buy_allowed"])
        self.assertTrue(status["sell_allowed"])
        self.assertEqual(status["next_transition"]["to"], SessionState.MARKET_OPEN.value)

    def test_market_open_allows_buys_scans_and_sells(self):
        status = self.status_at(datetime(2026, 5, 12, 10, 0, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.MARKET_OPEN.value)
        self.assertTrue(status["scan_allowed"])
        self.assertTrue(status["buy_allowed"])
        self.assertTrue(status["sell_allowed"])

    def test_power_hour_allows_buys_scans_and_sells(self):
        status = self.status_at(datetime(2026, 5, 12, 15, 30, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.POWER_HOUR.value)
        self.assertTrue(status["scan_allowed"])
        self.assertTrue(status["buy_allowed"])
        self.assertTrue(status["sell_allowed"])
        self.assertEqual(status["next_transition"]["to"], SessionState.MARKET_CLOSE.value)

    def test_market_close_allows_sells_only(self):
        status = self.status_at(datetime(2026, 5, 12, 16, 5, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.MARKET_CLOSE.value)
        self.assertTrue(status["trading_allowed"])
        self.assertFalse(status["scan_allowed"])
        self.assertFalse(status["buy_allowed"])
        self.assertTrue(status["sell_allowed"])

    def test_after_hours_allows_sells_only(self):
        status = self.status_at(datetime(2026, 5, 12, 17, 0, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.AFTER_HOURS.value)
        self.assertTrue(status["trading_allowed"])
        self.assertFalse(status["scan_allowed"])
        self.assertFalse(status["buy_allowed"])
        self.assertTrue(status["sell_allowed"])

    def test_closed_blocks_all_actions_and_points_to_next_premarket(self):
        status = self.status_at(datetime(2026, 5, 12, 21, 0, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.CLOSED.value)
        self.assertFalse(status["trading_allowed"])
        self.assertFalse(status["scan_allowed"])
        self.assertFalse(status["buy_allowed"])
        self.assertFalse(status["sell_allowed"])
        self.assertEqual(status["next_transition"]["to"], SessionState.PREMARKET.value)

    def test_weekend_blocks_all_actions(self):
        status = self.status_at(datetime(2026, 5, 16, 10, 0, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.WEEKEND.value)
        self.assertFalse(status["trading_allowed"])
        self.assertFalse(status["scan_allowed"])
        self.assertFalse(status["buy_allowed"])
        self.assertFalse(status["sell_allowed"])

    def test_holiday_blocks_all_actions(self):
        status = self.status_at(datetime(2026, 7, 3, 10, 0, tzinfo=NY))

        self.assertEqual(status["current_session"], SessionState.HOLIDAY.value)
        self.assertFalse(status["trading_allowed"])
        self.assertFalse(status["scan_allowed"])
        self.assertFalse(status["buy_allowed"])
        self.assertFalse(status["sell_allowed"])


if __name__ == "__main__":
    unittest.main()

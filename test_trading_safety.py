from __future__ import annotations

import unittest

import config
from trading_safety import require_paper_auto_trading_allowed


class TradingSafetyGateTests(unittest.TestCase):
    def setUp(self):
        self.original = {
            "TRADING_MODE": config.TRADING_MODE,
            "AUTO_SEND_ORDERS": config.AUTO_SEND_ORDERS,
            "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
            "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
            "IBKR_PORT": config.IBKR_PORT,
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


if __name__ == "__main__":
    unittest.main()

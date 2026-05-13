from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import config
import database
import paper_liquidation


class FakeIB:
    def __init__(self):
        self.orders = []

    def qualifyContracts(self, contract):
        return [contract]

    def placeOrder(self, contract, order):
        self.orders.append((contract, order))
        return SimpleNamespace(
            order=SimpleNamespace(orderId=len(self.orders)),
            orderStatus=SimpleNamespace(status="Submitted"),
        )

    def sleep(self, seconds):
        return None


class FakeClient:
    def __init__(self, positions=None):
        self.ib = FakeIB()
        self.positions = positions or []
        self.connected = False

    def connect(self):
        self.connected = True
        return True

    def is_connected(self):
        return self.connected

    def get_positions(self):
        return self.positions

    def disconnect(self):
        self.connected = False


class PaperLiquidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.original = {
            "database_DB_PATH": database.DB_PATH,
            "TRADING_MODE": config.TRADING_MODE,
            "AUTO_SEND_ORDERS": config.AUTO_SEND_ORDERS,
            "IBKR_PAPER_TRADING": config.IBKR_PAPER_TRADING,
            "IBKR_ENABLE_REAL_TRADING": config.IBKR_ENABLE_REAL_TRADING,
            "IBKR_PORT": config.IBKR_PORT,
        }
        database.DB_PATH = self.tmp.name
        config.TRADING_MODE = "PAPER"
        config.AUTO_SEND_ORDERS = True
        config.IBKR_PAPER_TRADING = True
        config.IBKR_ENABLE_REAL_TRADING = False
        config.IBKR_PORT = 7497

    def tearDown(self):
        database.DB_PATH = self.original["database_DB_PATH"]
        config.TRADING_MODE = self.original["TRADING_MODE"]
        config.AUTO_SEND_ORDERS = self.original["AUTO_SEND_ORDERS"]
        config.IBKR_PAPER_TRADING = self.original["IBKR_PAPER_TRADING"]
        config.IBKR_ENABLE_REAL_TRADING = self.original["IBKR_ENABLE_REAL_TRADING"]
        config.IBKR_PORT = self.original["IBKR_PORT"]
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def make_position(self, symbol="AAPL", quantity=2):
        return SimpleNamespace(
            contract=SimpleNamespace(symbol=symbol),
            position=quantity,
        )

    def test_liquidation_is_blocked_if_live_trading_is_enabled(self):
        config.IBKR_ENABLE_REAL_TRADING = True
        client = FakeClient([self.make_position()])

        with self.assertRaisesRegex(RuntimeError, "LIVE trading is enabled"):
            paper_liquidation.liquidate_all_paper_positions(ibkr_client=client)

        self.assertEqual(client.ib.orders, [])

    def test_liquidation_is_blocked_if_port_is_not_7497(self):
        config.IBKR_PORT = 7496
        client = FakeClient([self.make_position()])

        with self.assertRaisesRegex(RuntimeError, "not Paper port 7497"):
            paper_liquidation.liquidate_all_paper_positions(ibkr_client=client)

        self.assertEqual(client.ib.orders, [])

    def test_liquidation_is_blocked_if_paper_trading_is_false(self):
        config.IBKR_PAPER_TRADING = False
        client = FakeClient([self.make_position()])

        with self.assertRaisesRegex(RuntimeError, "IBKR_PAPER_TRADING is false"):
            paper_liquidation.liquidate_all_paper_positions(ibkr_client=client)

        self.assertEqual(client.ib.orders, [])

    def test_liquidation_calls_safety_gate_before_any_order_is_sent(self):
        client = FakeClient([self.make_position("MSFT", 3)])
        call_order = []

        def safety_gate(action):
            call_order.append(("safety", action))

        def place_order(contract, order):
            call_order.append(("order", contract.symbol))
            return SimpleNamespace(
                order=SimpleNamespace(orderId=10),
                orderStatus=SimpleNamespace(status="Submitted"),
            )

        client.ib.placeOrder = place_order

        with patch.object(paper_liquidation, "require_paper_auto_trading_allowed", side_effect=safety_gate):
            result = paper_liquidation.liquidate_all_paper_positions(ibkr_client=client)

        self.assertEqual(call_order[0], ("safety", "Paper liquidation"))
        self.assertEqual(call_order[1], ("order", "MSFT"))
        self.assertEqual(result["long_positions_liquidated"], 1)


if __name__ == "__main__":
    unittest.main()

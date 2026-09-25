import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

os.environ["CHATGPT_BRIDGE_TOKEN"] = "T" * 40
os.environ["CHATGPT_BRIDGE_ALLOW_SUBMIT"] = "false"
os.environ["CHATGPT_BRIDGE_IBKR_HOST"] = "127.0.0.1"
os.environ["CHATGPT_BRIDGE_IBKR_PORT"] = "7497"
os.environ["IBKR_ENABLE_REAL_TRADING"] = "false"
os.environ["IBKR_PAPER_TRADING"] = "true"

import chatgpt_bridge as bridge


class FakeSubmitIB:
    def __init__(self):
        self.disconnected = False

    def qualifyContracts(self, contract):
        return [contract]

    def placeOrder(self, contract, order):
        return SimpleNamespace(
            order=SimpleNamespace(orderId=123, permId=456),
            orderStatus=SimpleNamespace(
                status="Submitted",
                filled=0,
                remaining=float(order.totalQuantity),
                avgFillPrice=0,
            ),
        )

    def sleep(self, seconds):
        return None

    def disconnect(self):
        self.disconnected = True


class ChatGPTBridgeTests(unittest.TestCase):
    def setUp(self):
        bridge._proposals.clear()
        bridge._approvals.clear()
        bridge._rate.clear()
        bridge._ALLOW_SUBMIT = False
        bridge._MAX_QTY = 10
        bridge._MAX_NOTIONAL = 5000
        self.tmp = tempfile.TemporaryDirectory()
        bridge._AUDIT_PATH = Path(self.tmp.name) / "audit.jsonl"

    def tearDown(self):
        self.tmp.cleanup()

    def _prepare(self):
        req = bridge.PrepareOrder(
            symbol="AMD",
            side="BUY",
            quantity=1,
            order_type="LIMIT",
            limit_price=100,
        )
        with patch.object(bridge, "_notional", return_value=100):
            return bridge.prepare(req, "_")

    def test_auth_rejects_wrong_token(self):
        with self.assertRaises(HTTPException) as ctx:
            bridge._auth("Bearer wrong-token")
        self.assertEqual(ctx.exception.status_code, 401)

    def test_prepare_never_submits(self):
        proposal = self._prepare()
        self.assertEqual(proposal["status"], "AWAITING_CONFIRMATION")
        self.assertFalse(proposal["submitted"])
        self.assertNotIn("broker_result", proposal)

    def test_prepare_enforces_quantity_limit(self):
        bridge._MAX_QTY = 1
        req = bridge.PrepareOrder(
            symbol="AMD", side="BUY", quantity=2, order_type="LIMIT", limit_price=100
        )
        with self.assertRaises(HTTPException) as ctx:
            bridge.prepare(req, "_")
        self.assertEqual(ctx.exception.status_code, 422)

    def test_submit_is_disabled_by_default(self):
        proposal = self._prepare()
        confirmation = bridge.confirm(bridge.ProposalRef(proposal_id=proposal["proposal_id"]), "_")
        req = bridge.SubmitOrder(
            proposal_id=proposal["proposal_id"],
            approval_token=confirmation["approval_token"],
        )
        with self.assertRaises(HTTPException) as ctx:
            bridge.submit(req, "_")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_expired_approval_is_rejected(self):
        proposal = self._prepare()
        confirmation = bridge.confirm(bridge.ProposalRef(proposal_id=proposal["proposal_id"]), "_")
        bridge._approvals[proposal["proposal_id"]]["expires_at"] = bridge._now() - 1
        bridge._ALLOW_SUBMIT = True
        req = bridge.SubmitOrder(
            proposal_id=proposal["proposal_id"],
            approval_token=confirmation["approval_token"],
        )
        with self.assertRaises(HTTPException) as ctx:
            bridge.submit(req, "_")
        self.assertEqual(ctx.exception.status_code, 410)

    def test_tampered_order_is_rejected_after_confirmation(self):
        proposal = self._prepare()
        confirmation = bridge.confirm(bridge.ProposalRef(proposal_id=proposal["proposal_id"]), "_")
        bridge._proposals[proposal["proposal_id"]]["order"]["quantity"] = 2
        bridge._ALLOW_SUBMIT = True
        req = bridge.SubmitOrder(
            proposal_id=proposal["proposal_id"],
            approval_token=confirmation["approval_token"],
        )
        with self.assertRaises(HTTPException) as ctx:
            bridge.submit(req, "_")
        self.assertEqual(ctx.exception.status_code, 409)

    def test_successful_paper_submit_and_duplicate_rejection(self):
        proposal = self._prepare()
        confirmation = bridge.confirm(bridge.ProposalRef(proposal_id=proposal["proposal_id"]), "_")
        bridge._ALLOW_SUBMIT = True
        req = bridge.SubmitOrder(
            proposal_id=proposal["proposal_id"],
            approval_token=confirmation["approval_token"],
        )
        fake = FakeSubmitIB()
        with patch.object(bridge, "_connect", return_value=fake), patch.object(bridge, "_notional", return_value=100):
            result = bridge.submit(req, "_")

        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "PAPER_ONLY")
        self.assertEqual(result["broker"]["order_id"], 123)
        self.assertTrue(bridge._proposals[proposal["proposal_id"]]["submitted"])
        self.assertTrue(bridge._approvals[proposal["proposal_id"]]["used"])
        self.assertTrue(fake.disconnected)

        with self.assertRaises(HTTPException) as ctx:
            bridge.submit(req, "_")
        self.assertEqual(ctx.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()

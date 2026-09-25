from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from ib_insync import IB, LimitOrder, MarketOrder, Stock

router = APIRouter(prefix="/api/chatgpt", tags=["chatgpt-bridge"])

_TOKEN = os.getenv("CHATGPT_BRIDGE_TOKEN", "").strip()
_ALLOW_SUBMIT = os.getenv("CHATGPT_BRIDGE_ALLOW_SUBMIT", "false").strip().lower() in {"1","true","yes","on"}
_HOST = os.getenv("CHATGPT_BRIDGE_IBKR_HOST", "127.0.0.1").strip()
_PORT = int(os.getenv("CHATGPT_BRIDGE_IBKR_PORT", "7497"))
_READ_CLIENT_ID = int(os.getenv("CHATGPT_BRIDGE_READ_CLIENT_ID", "71"))
_TRADE_CLIENT_ID = int(os.getenv("CHATGPT_BRIDGE_TRADE_CLIENT_ID", "72"))
_APPROVAL_TTL = int(os.getenv("CHATGPT_BRIDGE_APPROVAL_TTL_SECONDS", "120"))
_MAX_QTY = float(os.getenv("CHATGPT_BRIDGE_MAX_QUANTITY", "10"))
_MAX_NOTIONAL = float(os.getenv("CHATGPT_BRIDGE_MAX_NOTIONAL_USD", "5000"))
_RATE_LIMIT = int(os.getenv("CHATGPT_BRIDGE_RATE_LIMIT_PER_MINUTE", "60"))
_AUDIT_PATH = Path(os.getenv("CHATGPT_BRIDGE_AUDIT_PATH", "chatgpt_bridge_audit.jsonl"))

if not _TOKEN or len(_TOKEN) < 32 or _TOKEN.lower() in {"change-me","changeme","default","test"}:
    raise RuntimeError("CHATGPT_BRIDGE_TOKEN must be a strong unique token of at least 32 characters")
if _HOST not in {"127.0.0.1", "localhost"}:
    raise RuntimeError("ChatGPT Bridge IBKR host must remain local-only")
if _PORT != 7497:
    raise RuntimeError("ChatGPT Bridge is hard-gated to IBKR Paper TWS port 7497")
if os.getenv("IBKR_ENABLE_REAL_TRADING", "false").strip().lower() not in {"0","false","no","off"}:
    raise RuntimeError("ChatGPT Bridge refuses startup while real trading is enabled")
if os.getenv("IBKR_PAPER_TRADING", "true").strip().lower() not in {"1","true","yes","on"}:
    raise RuntimeError("ChatGPT Bridge requires IBKR_PAPER_TRADING=true")

_lock = threading.RLock()
_proposals: dict[str, dict] = {}
_approvals: dict[str, dict] = {}
_rate: dict[str, deque] = defaultdict(deque)


class PrepareOrder(BaseModel):
    symbol: str
    side: Literal["BUY", "SELL"]
    quantity: float = Field(gt=0)
    order_type: Literal["MARKET", "LIMIT"] = "LIMIT"
    limit_price: float | None = Field(default=None, gt=0)

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        value = value.strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", value):
            raise ValueError("invalid symbol")
        return value


class ProposalRef(BaseModel):
    proposal_id: str


class SubmitOrder(BaseModel):
    proposal_id: str
    approval_token: str


def _now() -> float:
    return time.time()


def _audit(event: str, payload: dict) -> None:
    row = {"ts": _now(), "event": event, **payload}
    _AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _AUDIT_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _auth(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Bearer token required")
    supplied = authorization[7:].strip()
    if not hmac.compare_digest(supplied, _TOKEN):
        raise HTTPException(401, "Invalid token")
    now = _now()
    q = _rate[supplied]
    while q and now - q[0] > 60:
        q.popleft()
    if len(q) >= _RATE_LIMIT:
        raise HTTPException(429, "Rate limit exceeded")
    q.append(now)
    return supplied


def _canonical(order: dict) -> str:
    return json.dumps(order, sort_keys=True, separators=(",", ":"))


def _order_hash(order: dict) -> str:
    return hashlib.sha256(_canonical(order).encode()).hexdigest()


def _connect(readonly: bool, client_id: int) -> IB:
    ib = IB()
    ib.connect(_HOST, _PORT, clientId=client_id, readonly=readonly, timeout=10)
    accounts = ib.managedAccounts() or []
    if not accounts or not str(accounts[0]).upper().startswith("DU"):
        ib.disconnect()
        raise RuntimeError("ChatGPT Bridge requires an IBKR Paper account (DU...)")
    return ib


def _quote(symbol: str) -> dict:
    ib = _connect(True, _READ_CLIENT_ID)
    try:
        c = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(c)
        t = ib.reqMktData(c, "", False, False)
        ib.sleep(2)
        out = {"symbol": symbol, "bid": t.bid, "ask": t.ask, "last": t.last, "close": t.close, "market_price": t.marketPrice()}
        ib.cancelMktData(c)
        return out
    finally:
        ib.disconnect()


def _notional(order: dict) -> float:
    price = order.get("limit_price")
    if price is None:
        q = _quote(order["symbol"])
        price = q.get("market_price") or q.get("last") or q.get("ask") or q.get("close")
    if not price or float(price) <= 0:
        raise HTTPException(409, "Cannot establish a valid price for risk validation")
    return float(order["quantity"]) * float(price)


@router.get("/health")
def health(_: str = Depends(_auth)):
    return {"ok": True, "mode": "PAPER_ONLY", "submit_enabled": _ALLOW_SUBMIT, "ibkr_host": _HOST, "ibkr_port": _PORT}


@router.get("/account")
def account(_: str = Depends(_auth)):
    ib = _connect(True, _READ_CLIENT_ID)
    try:
        rows = [{"tag": r.tag, "value": r.value, "currency": r.currency, "account": r.account} for r in ib.accountSummary()]
        accounts = ib.managedAccounts() or []
        return {"ok": True, "mode": "PAPER_ONLY", "account": accounts[0] if accounts else None, "summary": rows}
    finally:
        ib.disconnect()


@router.get("/positions")
def positions(_: str = Depends(_auth)):
    ib = _connect(True, _READ_CLIENT_ID)
    try:
        rows = [{"symbol": p.contract.symbol, "quantity": float(p.position), "avg_cost": float(p.avgCost), "account": p.account} for p in ib.positions()]
        return {"ok": True, "positions": rows}
    finally:
        ib.disconnect()


@router.get("/orders")
def orders(_: str = Depends(_auth)):
    ib = _connect(True, _READ_CLIENT_ID)
    try:
        ib.reqAllOpenOrders(); ib.sleep(0.5)
        rows=[]
        for t in ib.openTrades():
            rows.append({"order_id": t.order.orderId, "symbol": t.contract.symbol, "side": t.order.action, "quantity": float(t.order.totalQuantity), "order_type": t.order.orderType, "limit_price": t.order.lmtPrice, "status": t.orderStatus.status, "filled": float(t.orderStatus.filled), "remaining": float(t.orderStatus.remaining)})
        return {"ok": True, "orders": rows}
    finally:
        ib.disconnect()


@router.get("/executions")
def executions(_: str = Depends(_auth)):
    ib = _connect(True, _READ_CLIENT_ID)
    try:
        rows=[]
        for f in ib.fills():
            e=f.execution
            rows.append({"execution_id": e.execId, "order_id": e.orderId, "symbol": f.contract.symbol, "side": e.side, "shares": float(e.shares), "price": float(e.price), "time": str(e.time), "account": e.acctNumber})
        return {"ok": True, "executions": rows}
    finally:
        ib.disconnect()


@router.get("/quote/{symbol}")
def quote(symbol: str, _: str = Depends(_auth)):
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", symbol):
        raise HTTPException(422, "invalid symbol")
    return {"ok": True, "quote": _quote(symbol)}


@router.post("/order/prepare")
def prepare(req: PrepareOrder, _: str = Depends(_auth)):
    if req.order_type == "LIMIT" and req.limit_price is None:
        raise HTTPException(422, "limit_price is required for LIMIT")
    if req.order_type == "MARKET" and req.limit_price is not None:
        raise HTTPException(422, "limit_price is not allowed for MARKET")
    if req.quantity > _MAX_QTY:
        raise HTTPException(422, "quantity exceeds bridge limit")
    order=req.model_dump()
    notional=_notional(order)
    if notional > _MAX_NOTIONAL:
        raise HTTPException(422, "notional exceeds bridge limit")
    proposal_id="prop_"+secrets.token_hex(12)
    proposal={"proposal_id":proposal_id,"order":order,"order_hash":_order_hash(order),"notional_usd":round(notional,2),"status":"AWAITING_CONFIRMATION","created_at":_now(),"submitted":False}
    with _lock:
        _proposals[proposal_id]=proposal
    _audit("PREPARE", {"proposal_id":proposal_id,"order":order,"notional_usd":notional})
    return proposal


@router.post("/order/confirm")
def confirm(req: ProposalRef, _: str = Depends(_auth)):
    with _lock:
        p=_proposals.get(req.proposal_id)
        if not p:
            raise HTTPException(404, "proposal not found")
        if p.get("submitted"):
            raise HTTPException(409, "proposal already submitted")
        raw=secrets.token_urlsafe(32)
        token_hash=hashlib.sha256(raw.encode()).hexdigest()
        _approvals[req.proposal_id]={"token_hash":token_hash,"order_hash":p["order_hash"],"expires_at":_now()+_APPROVAL_TTL,"used":False}
        p["status"]="CONFIRMED"
    _audit("CONFIRM", {"proposal_id":req.proposal_id,"order_hash":p["order_hash"]})
    return {"ok":True,"proposal_id":req.proposal_id,"approval_token":raw,"expires_in_seconds":_APPROVAL_TTL}


@router.post("/order/submit")
def submit(req: SubmitOrder, _: str = Depends(_auth)):
    if not _ALLOW_SUBMIT:
        raise HTTPException(403, "submission disabled by CHATGPT_BRIDGE_ALLOW_SUBMIT")
    with _lock:
        p=_proposals.get(req.proposal_id); a=_approvals.get(req.proposal_id)
        if not p or not a:
            raise HTTPException(404, "proposal/approval not found")
        if p.get("submitted") or a.get("used"):
            raise HTTPException(409, "approval already used")
        if _now() > float(a["expires_at"]):
            raise HTTPException(410, "approval expired")
        if a["order_hash"] != _order_hash(p["order"]):
            _audit("REJECT", {"proposal_id":req.proposal_id,"reason":"order hash mismatch"})
            raise HTTPException(409, "proposal was modified after confirmation")
        if not hmac.compare_digest(hashlib.sha256(req.approval_token.encode()).hexdigest(), a["token_hash"]):
            raise HTTPException(401, "invalid approval token")
        if _notional(p["order"]) > _MAX_NOTIONAL or float(p["order"]["quantity"]) > _MAX_QTY:
            raise HTTPException(422, "risk limits exceeded")
        a["used"]=True
    ib=_connect(False, _TRADE_CLIENT_ID)
    try:
        o=p["order"]; c=Stock(o["symbol"],"SMART","USD"); ib.qualifyContracts(c)
        order=MarketOrder(o["side"],o["quantity"],tif="DAY") if o["order_type"]=="MARKET" else LimitOrder(o["side"],o["quantity"],o["limit_price"],tif="DAY")
        trade=ib.placeOrder(c,order); ib.sleep(1)
        result={"order_id":trade.order.orderId,"perm_id":trade.order.permId,"status":trade.orderStatus.status,"filled":float(trade.orderStatus.filled),"remaining":float(trade.orderStatus.remaining),"avg_fill_price":float(trade.orderStatus.avgFillPrice or 0)}
        with _lock:
            p["submitted"]=True; p["status"]="SUBMITTED"; p["broker_result"]=result
        _audit("SUBMIT", {"proposal_id":req.proposal_id,"order":o,"broker_result":result})
        return {"ok":True,"mode":"PAPER_ONLY","proposal_id":req.proposal_id,"order":o,"broker":result}
    except Exception as exc:
        _audit("REJECT", {"proposal_id":req.proposal_id,"reason":str(exc)})
        raise
    finally:
        ib.disconnect()


@router.get("/order/{proposal_id}")
def verify(proposal_id: str, _: str = Depends(_auth)):
    with _lock:
        p=_proposals.get(proposal_id)
        if not p:
            raise HTTPException(404, "proposal not found")
        out=dict(p)
    if p.get("submitted") and (p.get("broker_result") or {}).get("order_id"):
        ib=_connect(True, _READ_CLIENT_ID)
        try:
            ib.reqAllOpenOrders(); ib.sleep(0.5)
            oid=int(p["broker_result"]["order_id"])
            matches=[]
            for t in ib.trades():
                if int(getattr(t.order,"orderId",-1))==oid:
                    matches.append({"order_id":oid,"status":t.orderStatus.status,"filled":float(t.orderStatus.filled),"remaining":float(t.orderStatus.remaining),"avg_fill_price":float(t.orderStatus.avgFillPrice or 0)})
            out["read_back"]=matches
        finally:
            ib.disconnect()
    _audit("VERIFY", {"proposal_id":proposal_id,"status":out.get("status")})
    return {"ok":True,"proposal":out}

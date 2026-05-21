from __future__ import annotations
import asyncio, json
from datetime import datetime, timezone
import config
from tws_connection_manager import with_shared_ib_sync

BROKER_SYNC_CLIENT_ID_OFFSET = 700

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _f(v, d=0.0):
    try: return float(v) if v is not None else d
    except: return d

def _i(v, d=0):
    try: return int(v) if v is not None else d
    except: return d

def _s(v):
    return "" if v is None else str(v)

def fetch_broker_snapshot_sync() -> dict:
    errors=[]
    snapshot={"ok":False,"connected":False,"account":None,"synced_at":now_iso(),"equity":{"net_liquidation":0.0,"total_cash":0.0,"available_funds":0.0,"buying_power":0.0,"currency":"USD"},"positions":[],"open_orders":[],"executions":[],"errors":errors}
    def _fetch(ib):
        snapshot["connected"]=ib.isConnected()
        if not snapshot["connected"]:
            errors.append("IBKR connect returned disconnected state"); return snapshot
        accts=ib.managedAccounts(); snapshot["account"]=accts[0] if accts else None
        summary=ib.accountSummary() or []
        tags={r.tag:r for r in summary if getattr(r,'tag',None)}
        for key,tag in (("net_liquidation","NetLiquidation"),("total_cash","TotalCashValue"),("available_funds","AvailableFunds"),("buying_power","BuyingPower")):
            row=tags.get(tag); snapshot["equity"][key]=_f(getattr(row,'value',None),0.0)
            if row and getattr(row,'currency',None): snapshot["equity"]["currency"]=_s(row.currency)
        port={}
        for p in ib.portfolio() or []:
            sym=_s(getattr(p.contract,'symbol',None)).upper()
            if sym: port[sym]=p
        for pos in ib.positions() or []:
            sym=_s(getattr(pos.contract,'symbol',None)).upper()
            if not sym: continue
            p=port.get(sym)
            snapshot["positions"].append({"symbol":sym,"quantity":_f(pos.position),"avg_cost":_f(pos.avgCost),"market_price":_f(getattr(p,'marketPrice',None)),"market_value":_f(getattr(p,'marketValue',None)),"unrealized_pnl":_f(getattr(p,'unrealizedPNL',None)),"realized_pnl":_f(getattr(p,'realizedPNL',None)),"account":_s(getattr(pos,'account',None) or snapshot['account'])})
        ib.reqAllOpenOrders(); ib.sleep(0.5)
        for t in ib.openTrades() or []:
            c,o,s=t.contract,t.order,t.orderStatus
            snapshot["open_orders"].append({"order_id":_i(o.orderId),"perm_id":_i(o.permId),"symbol":_s(c.symbol).upper(),"action":_s(o.action).upper(),"order_type":_s(o.orderType),"quantity":_f(o.totalQuantity),"filled_quantity":_f(s.filled),"remaining_quantity":_f(s.remaining),"limit_price":_f(o.lmtPrice),"stop_price":_f(o.auxPrice),"status":_s(s.status),"account":_s(o.account or snapshot['account'])})
        for fill in ib.fills() or []:
            ex=fill.execution; cm=getattr(fill,'commissionReport',None)
            snapshot["executions"].append({"execution_id":_s(ex.execId),"order_id":_i(ex.orderId),"perm_id":_i(ex.permId),"symbol":_s(ex.symbol).upper(),"side":_s(ex.side).upper(),"shares":_f(ex.shares),"price":_f(ex.price),"time":_s(ex.time),"account":_s(ex.acctNumber or snapshot['account']),"commission":_f(getattr(cm,'commission',None))})
        snapshot["ok"]=True
        return snapshot
    try:
        return with_shared_ib_sync(_fetch, readonly=True)
    except Exception as exc:
        errors.append(str(exc)); return snapshot
    finally:
        snapshot["synced_at"]=now_iso()

async def run_broker_sync_once() -> dict:
    return await asyncio.to_thread(fetch_broker_snapshot_sync)

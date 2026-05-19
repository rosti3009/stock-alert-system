from __future__ import annotations
import json
from datetime import datetime, timezone
import database

HIGH='HIGH'

def now_iso(): return datetime.now(timezone.utc).isoformat()

def _j(v): return json.dumps(v, ensure_ascii=False, default=str)

async def run_reconciliation(snapshot: dict) -> dict:
    await database.init_db()
    broker_pos={p['symbol']:p for p in snapshot.get('positions',[])}
    db_positions=await database.get_open_positions()
    db_pos={str(p.get('symbol','')).upper():p for p in db_positions}
    issues=[]

    for sym,bp in broker_pos.items():
        if sym not in db_pos:
            await database.upsert_position({'symbol':sym,'buy_price':bp.get('avg_cost') or 0.0,'quantity':bp.get('quantity') or 0.0,'current_price':bp.get('market_price') or 0.0,'profit_amount':bp.get('unrealized_pnl') or 0.0,'status':'OPEN','action':'HOLD','source':'IBKR_ADOPTED','reason':'IBKR_ADOPTED'})
            issues.append({'event_type':'IBKR_POSITION_ADOPTED','severity':'INFO','symbol':sym})
        else:
            q1=float(db_pos[sym].get('quantity') or 0); q2=float(bp.get('quantity') or 0)
            if abs(q1-q2)>1e-9:
                await database.upsert_position({'symbol':sym,'buy_price':bp.get('avg_cost') or db_pos[sym].get('buy_price') or 0.0,'quantity':q2,'current_price':bp.get('market_price') or 0.0,'profit_amount':bp.get('unrealized_pnl') or 0.0,'status':'OPEN','action':'HOLD','reason':'BROKER_QUANTITY_RECONCILIATION','source':'IBKR'})
                issues.append({'event_type':'BROKER_QUANTITY_RECONCILIATION','severity':'MEDIUM','symbol':sym})
    for sym,dp in db_pos.items():
        if sym not in broker_pos:
            await database.close_position(sym, reason='BROKER_FLAT_RECONCILIATION')
            issues.append({'event_type':'BROKER_FLAT_RECONCILIATION','severity':'HIGH','symbol':sym})

    order_state = await database.reconcile_orders_and_executions(snapshot)
    for i in issues:
        await database.insert_reconciliation_event(i['event_type'], i['severity'], i.get('symbol'), i)
    for e in order_state.get('events',[]):
        await database.insert_reconciliation_event(e['event_type'], e['severity'], e.get('symbol'), e)
        issues.append(e)
    high=[i for i in issues if i.get('severity')==HIGH]
    return {'ok': len(high)==0, 'issues':issues, 'open_issues_count':len(issues), 'high_severity_issues_count':len(high), 'last_checked_at': now_iso()}

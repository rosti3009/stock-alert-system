import asyncio
import broker_sync
import reconciliation_engine
import database


def test_broker_sync_connected_empty_ok(monkeypatch):
    class FakeIB:
        def connect(self,*a,**k): pass
        def isConnected(self): return True
        def managedAccounts(self): return ['DU123']
        def accountSummary(self): return []
        def portfolio(self): return []
        def positions(self): return []
        def reqAllOpenOrders(self): pass
        def sleep(self,_): pass
        def openTrades(self): return []
        def fills(self): return []
        def disconnect(self): pass
    monkeypatch.setattr(broker_sync, 'IB', FakeIB)
    snap=asyncio.run(broker_sync.run_broker_sync_once())
    assert snap['ok'] is True and snap['connected'] is True


def test_reconcile_adopts_broker_position(tmp_path, monkeypatch):
    monkeypatch.setattr(database, 'DB_PATH', str(tmp_path/'t.db'))
    async def run():
        await database.init_db()
        snap={'positions':[{'symbol':'AAPL','quantity':2,'avg_cost':100,'market_price':101,'market_value':202,'unrealized_pnl':2,'realized_pnl':0,'account':'DU'}],'open_orders':[],'executions':[]}
        res=await reconciliation_engine.run_reconciliation(snap)
        p=await database.get_position('AAPL')
        return res,p
    res,p=asyncio.run(run())
    assert p and float(p['quantity'])==2 and res['ok']


def test_reconcile_closes_db_when_broker_flat(tmp_path, monkeypatch):
    monkeypatch.setattr(database, 'DB_PATH', str(tmp_path/'t2.db'))
    async def run():
        await database.init_db()
        await database.add_position({'symbol':'MSFT','buy_price':10,'quantity':1}, max_open_positions=10)
        res=await reconciliation_engine.run_reconciliation({'positions':[],'open_orders':[],'executions':[]})
        p=await database.get_position('MSFT')
        return res,p
    res,p=asyncio.run(run())
    assert p['status']=='CLOSED' and res['high_severity_issues_count']>=1

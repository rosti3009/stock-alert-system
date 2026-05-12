from ib_insync import *
import time

ib = IB()
ib.connect("127.0.0.1", 7497, clientId=3)

# 1 = live, 3 = delayed
ib.reqMarketDataType(3)

contract = Stock("AAPL", "SMART", "USD")
ib.qualifyContracts(contract)

ticker = ib.reqMktData(contract, "", False, False)

time.sleep(10)
ib.sleep(1)

print("Symbol:", contract.symbol)
print("Bid:", ticker.bid)
print("Ask:", ticker.ask)
print("Last:", ticker.last)
print("Close:", ticker.close)
print("Market Price:", ticker.marketPrice())

ib.cancelMktData(contract)
ib.disconnect()
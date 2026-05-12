from ib_insync import *

ib = IB()
ib.connect("127.0.0.1", 7497, clientId=2)

print("Connected:", ib.isConnected())
print("Accounts:", ib.managedAccounts())

print("\nAccount Summary:")
for row in ib.accountSummary():
    print(row.tag, row.value, row.currency)

print("\nPositions:")
for p in ib.positions():
    print(p.account, p.contract.symbol, p.position, p.avgCost)

ib.disconnect()
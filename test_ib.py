from ib_insync import *

ib = IB()

ib.connect("127.0.0.1", 7497, clientId=1)

print(ib.isConnected())
print(ib.managedAccounts())

ib.disconnect()
from ib_insync import *
import time

ib = IB()
ib.connect("127.0.0.1", 7497, clientId=4)

contract = Stock("AAPL", "SMART", "USD")
ib.qualifyContracts(contract)

order = LimitOrder("BUY", 1, 1.00)
trade = ib.placeOrder(contract, order)

time.sleep(3)
ib.sleep(1)

print("Order ID:", trade.order.orderId)
print("Status:", trade.orderStatus.status)
print("Filled:", trade.orderStatus.filled)
print("Remaining:", trade.orderStatus.remaining)

ib.cancelOrder(order)

time.sleep(2)
ib.sleep(1)

print("After Cancel:", trade.orderStatus.status)

ib.disconnect()
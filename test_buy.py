from ibkr_client import IBKRClient

client = IBKRClient(client_id=12)
client.connect()

price = client.get_stock_price("AAPL")
print("AAPL price:", price)

limit_price = 1.00

result = client.place_limit_buy_order(
    "AAPL",
    1,
    limit_price,
)

print("Order result:", result)

client.disconnect()
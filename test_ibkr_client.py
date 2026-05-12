from ibkr_client import IBKRClient

client = IBKRClient(client_id=3)

connected = client.connect()

print("Connected:", connected)
print("Accounts:", client.get_accounts())

summary = client.get_account_summary()

print("Summary rows:", len(summary))

price = client.get_stock_price("AAPL")

print("AAPL price:", price)

client.disconnect()
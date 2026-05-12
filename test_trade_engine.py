from trade_engine import TradeEngine


engine = TradeEngine()

connected = engine.connect()

print("Connected:", connected)

if not connected:
    raise SystemExit("IBKR connection failed")


symbol = "AAPL"

price_data = engine.client.get_stock_price(symbol)

print("Market data:", price_data)

market_price = float(
    price_data.get("market_price")
    or price_data.get("last")
    or price_data.get("ask")
    or price_data.get("close")
    or 0
)

if market_price <= 0:
    raise SystemExit("No valid market price")


limit_price = round(market_price * 0.995, 2)

print()
print("🚀 BUY SIGNAL:", symbol)
print("Quantity:", 1)
print("Market Price:", market_price)
print("Limit Price:", limit_price)
print("Order Value:", round(limit_price * 1, 2))


result = engine.execute_buy_signal(
    symbol=symbol,
    quantity=1,
    limit_price=limit_price,
)

print()
print("✅ Order Result:")
print(result)

print()
print("Test result:")
print(result)

engine.disconnect()
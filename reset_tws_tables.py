import sqlite3

db = sqlite3.connect("stock_alerts.db")

db.execute("DROP TABLE IF EXISTS tws_account")
db.execute("DROP TABLE IF EXISTS tws_positions")
db.execute("DROP TABLE IF EXISTS tws_orders")
db.execute("DROP TABLE IF EXISTS tws_heartbeat")

db.commit()
db.close()

print("DONE")
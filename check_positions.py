import sqlite3

db = sqlite3.connect("stock_alerts.db")

rows = db.execute(
    """
    SELECT
        symbol,
        quantity,
        buy_price,
        status
    FROM positions
    WHERE symbol IN (
        'MIRM',
        'PTCT',
        'ROAD'
    )
    """
).fetchall()

print(rows)

db.close()
import sqlite3
from datetime import datetime, timezone

db = sqlite3.connect("stock_alerts.db")

now = datetime.now(timezone.utc).isoformat()

rows = db.execute(
    """
    SELECT
        symbol,
        quantity,
        avg_cost,
        market_price
    FROM tws_positions
    WHERE symbol IN (
        'MIRM',
        'PTCT',
        'ROAD'
    )
    """
).fetchall()

for symbol, quantity, avg_cost, market_price in rows:

    existing = db.execute(
        """
        SELECT symbol
        FROM positions
        WHERE symbol = ?
        """,
        (symbol,),
    ).fetchone()

    if existing:

        db.execute(
            """
            UPDATE positions
            SET
                quantity = ?,
                buy_price = ?,
                current_price = ?,
                status = 'OPEN',
                action = 'HOLD',
                updated_at = ?
            WHERE symbol = ?
            """,
            (
                quantity,
                avg_cost,
                market_price,
                now,
                symbol,
            ),
        )

    else:

        db.execute(
            """
            INSERT INTO positions (
                symbol,
                buy_price,
                quantity,
                buy_date,
                current_price,
                status,
                action,
                reason,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                avg_cost,
                quantity,
                now,
                market_price,
                "OPEN",
                "HOLD",
                "Repaired from TWS reconciliation",
                now,
                now,
            ),
        )

db.commit()
db.close()

print("POSITIONS REPAIRED")
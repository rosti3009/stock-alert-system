import sqlite3

db = sqlite3.connect("stock_alerts.db")

db.execute("""
UPDATE positions
SET
    status='CLOSED',
    action='CLOSED',
    reason='Manual sync',
    updated_at=datetime('now')
WHERE status='OPEN'
""")

db.commit()

print("DONE")
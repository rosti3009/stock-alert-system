import sqlite3

db = sqlite3.connect("stock_alerts.db")
cursor = db.cursor()

# ==========================================
# RESET POSITIONS
# ==========================================

cursor.execute("""
DELETE FROM positions
""")

# ==========================================
# RESET SIGNALS
# ==========================================

cursor.execute("""
DELETE FROM signals
""")

cursor.execute("""
DELETE FROM last_signals
""")

# ==========================================
# RESET SCAN HISTORY
# ==========================================

cursor.execute("""
DELETE FROM scan_runs
""")

# ==========================================
# RESET DAILY CANDIDATES
# ==========================================

cursor.execute("""
DELETE FROM daily_candidates
""")

# ==========================================
# RESET SQLITE SEQUENCES
# ==========================================

cursor.execute("""
DELETE FROM sqlite_sequence
""")

db.commit()
db.close()

print("✅ ACCOUNT RESET COMPLETE")
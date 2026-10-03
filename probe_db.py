import sqlite3

conn = sqlite3.connect('data/mtg_tracker.db')
rows = conn.execute(
    "SELECT name, set_code, collector_number, foil, price_usd FROM cards WHERE lower(name) = 'lotus petal' ORDER BY set_code, collector_number, foil"
).fetchall()
print('lotus_count', len(rows))
for row in rows:
    print(row)
conn.close()

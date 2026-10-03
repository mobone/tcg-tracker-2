import sqlite3
import app

app.refresh_default_cards_catalog()
conn = sqlite3.connect('data/mtg_tracker.db')
row = conn.execute("SELECT name, set_code, collector_number, image_url FROM cards WHERE lower(name)='lotus petal' ORDER BY set_code, collector_number LIMIT 1").fetchone()
print(row)
conn.close()

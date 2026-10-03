import sqlite3

conn = sqlite3.connect('data/mtg_tracker.db')
print('image_count', conn.execute("SELECT COUNT(*) FROM cards WHERE image_url IS NOT NULL").fetchone()[0])
print('lotus_samples', conn.execute("SELECT name, image_url FROM cards WHERE lower(name)='lotus petal' LIMIT 5").fetchall())
conn.close()

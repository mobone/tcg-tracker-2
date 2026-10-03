from app import app
import sqlite3

client = app.test_client()
print('HOME_STATUS', client.get('/').status_code)
print('SYNC_STATUS', client.post('/sync-scryfall').status_code)
conn = sqlite3.connect('data/mtg_tracker.db')
print('DB_CARD_COUNT', conn.execute('SELECT COUNT(*) FROM cards').fetchone()[0])
conn.close()

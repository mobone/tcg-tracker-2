import sqlite3

conn = sqlite3.connect('data/mtg_tracker.db')
queries = [
    'The Emperor, Hell Tyrant - Yawgmoth, Thran Physician',
    '1 The Emperor, Hell Tyrant - Yawgmoth, Thran Physician (Showcase) [FCA] 11',
    'The Emperor',
]
for q in queries:
    rows = conn.execute("SELECT name, set_code, collector_number, foil FROM cards WHERE lower(name) LIKE lower(?) LIMIT 10", ('%' + q + '%',)).fetchall()
    print('QUERY:', q)
    print(rows)
    print('---')
conn.close()

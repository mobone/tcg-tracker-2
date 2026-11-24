import gzip
import json
import logging
import os
import re
import sqlite3
import threading
import time
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, flash, make_response, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("MTG_TRACKER_DB") or BASE_DIR / "data" / "mtg_tracker.db")
SCRYFALL_FILE_TIMESTAMP_RE = re.compile(r"default-cards-(\d{14})\.jsonl\.gz")

_env_file = BASE_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        if "=" in _line and not _line.lstrip().startswith("#"):
            _key, _value = _line.split("=", 1)
            os.environ.setdefault(_key.strip(), _value.strip().strip("'\""))

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-me"
app.config["DATABASE"] = str(DB_PATH)
app.config["CATALOG_SYNCED"] = False
SYNC_LOCK = threading.Lock()
# Serializes the big write jobs (catalog refresh, pruning/VACUUM) so they never fight for the DB lock.
DB_MAINTENANCE_LOCK = threading.Lock()
RETENTION_DAYS = 30
SYNC_THREAD = None
SCHEDULER = None

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

#test again

def utc_now():
    """Current UTC time as a naive datetime (same values the old utc_now() returned,
    which is what the stored timestamps and date comparisons expect)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def log(*args):
    print(datetime.now().strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True)


WAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024


def get_db_connection(timeout=5.0):
    # timeout = how long to wait for another connection's lock before "database is locked".
    conn = sqlite3.connect(app.config["DATABASE"], timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # SQLite never shrinks the -wal file by itself; after each checkpoint, trim it back to this size
    # so one big write (the daily sync, VACUUM) doesn't leave a huge -wal file behind for good.
    conn.execute(f"PRAGMA journal_size_limit={WAL_SIZE_LIMIT_BYTES}")
    return conn


def shrink_wal(conn, context):
    """Copy everything in the -wal file into the main database and truncate the -wal to 0 bytes.

    Skipped (and logged) if a reader is mid-query; the journal_size_limit trim and the next run
    take care of it then. Committed data is never at risk either way.
    """
    busy, wal_pages, checkpointed = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if busy:
        log(f"{context}: WAL checkpoint postponed, database in use ({checkpointed}/{wal_pages} pages copied)")
    else:
        log(f"{context}: WAL checkpointed and truncated")


def init_db():
    log("Initializing database...")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_db_connection()
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scryfall_id TEXT,
            name TEXT NOT NULL,
            flavor_name TEXT,
            set_code TEXT NOT NULL,
            set_name TEXT,
            collector_number TEXT NOT NULL,
            foil INTEGER NOT NULL DEFAULT 0,
            price_usd REAL,
            image_url TEXT,
            last_updated TEXT,
            UNIQUE(name, set_code, collector_number, foil)
        )
        """
    )

    existing_columns = [
        row[1] for row in conn.execute("PRAGMA table_info(cards)").fetchall()
    ]
    if "flavor_name" not in existing_columns:
        conn.execute("ALTER TABLE cards ADD COLUMN flavor_name TEXT")
    if "image_url" not in existing_columns:
        conn.execute("ALTER TABLE cards ADD COLUMN image_url TEXT")
    for column_name, column_sql in {
        "matched_product_id": "INTEGER",
        "matched_product_name": "TEXT",
        "matched_set_code": "TEXT",
        "matched_set_name": "TEXT",
        "matched_foil": "INTEGER",
        "scryfall_price_usd": "REAL",
        "finish": "TEXT NOT NULL DEFAULT 'nonfoil'",
        "match_score": "INTEGER",
        "match_method": "TEXT",
        "is_matched": "INTEGER DEFAULT 0",
        "is_sealed": "INTEGER DEFAULT 0",
        "tcgplayer_product_id": "INTEGER",
    }.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE cards ADD COLUMN {column_name} {column_sql}")
    conn.execute(
        """
        UPDATE cards
        SET finish = CASE WHEN foil = 1 THEN 'foil' ELSE 'nonfoil' END
        WHERE finish IS NULL
           OR (finish = 'nonfoil' AND foil = 1)
           OR (finish = 'foil' AND foil = 0)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS collection (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            acquired_at TEXT DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            name TEXT,
            set_code TEXT,
            set_name TEXT,
            collector_number TEXT,
            foil INTEGER,
            image_url TEXT,
            price_usd REAL,
            last_updated TEXT,
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE,
            UNIQUE(card_id)
        )
        """
    )
    collection_columns = [
        row[1] for row in conn.execute("PRAGMA table_info(collection)").fetchall()
    ]
    for column_name, column_sql in {
        "name": "TEXT",
        "set_code": "TEXT",
        "set_name": "TEXT",
        "collector_number": "TEXT",
        "foil": "INTEGER",
        "image_url": "TEXT",
        "price_usd": "REAL",
        "last_updated": "TEXT",
    }.items():
        if column_name not in collection_columns:
            conn.execute(f"ALTER TABLE collection ADD COLUMN {column_name} {column_sql}")
    conn.execute(
        """
        UPDATE collection
        SET name = (SELECT cards.name FROM cards WHERE cards.id = collection.card_id),
            set_code = (SELECT cards.set_code FROM cards WHERE cards.id = collection.card_id),
            set_name = (SELECT cards.set_name FROM cards WHERE cards.id = collection.card_id),
            collector_number = (SELECT cards.collector_number FROM cards WHERE cards.id = collection.card_id),
            foil = (SELECT cards.foil FROM cards WHERE cards.id = collection.card_id),
            image_url = (SELECT cards.image_url FROM cards WHERE cards.id = collection.card_id),
            price_usd = (SELECT cards.price_usd FROM cards WHERE cards.id = collection.card_id),
            last_updated = (SELECT cards.last_updated FROM cards WHERE cards.id = collection.card_id)
        WHERE card_id IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER NOT NULL,
            recorded_at TEXT NOT NULL,
            foil INTEGER NOT NULL DEFAULT 0,
            price_usd REAL,
            UNIQUE(card_id, recorded_at, foil),
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            synced_at TEXT NOT NULL,
            cards_updated INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    sync_log_columns = [row[1] for row in conn.execute("PRAGMA table_info(sync_log)").fetchall()]
    for column_name in ("source_url", "data_generated_at"):
        if column_name not in sync_log_columns:
            conn.execute(f"ALTER TABLE sync_log ADD COLUMN {column_name} TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_name ON cards(name COLLATE NOCASE)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_set_number ON cards(set_code, collector_number)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_card_date ON price_history(card_id, recorded_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS collection_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            start_price REAL,
            acquired_at TEXT NOT NULL,
            FOREIGN KEY(collection_id) REFERENCES collection(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lots_collection ON collection_lots(collection_id)"
    )
    ensure_collection_lots(conn)
    if conn.execute("PRAGMA user_version").fetchone()[0] < 1:
        remove_duplicate_price_days(conn)
        conn.execute("PRAGMA user_version = 1")
    conn.commit()
    conn.close()


DUPLICATE_DAY_MIN_MATCHES = 20


def remove_duplicate_price_days(conn):
    """One-time cleanup: drop price_history days that are a re-import of the previous day's data.

    Before price rows were keyed by the Scryfall file's own date, pulling the same file on two
    days stored it twice. A day counts as a duplicate only if every price it shares with the
    previous kept day is identical (and it shares at least DUPLICATE_DAY_MIN_MATCHES of them;
    real days have thousands of changes). Only the matching rows are deleted, so cards that
    exist solely on that day keep their row. Charts and percent changes are unaffected because
    they carry the last known price forward.
    """
    days = [row[0] for row in conn.execute("SELECT DISTINCT recorded_at FROM price_history ORDER BY recorded_at")]
    removed_days, removed_rows = [], 0
    previous = days[0] if days else None
    for day in days[1:]:
        shared, differing = conn.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(d.price_usd IS NOT p.price_usd), 0)
            FROM price_history d
            JOIN price_history p
              ON p.card_id = d.card_id AND p.foil = d.foil AND p.recorded_at = ?
            WHERE d.recorded_at = ?
            """,
            (previous, day),
        ).fetchone()
        if shared >= DUPLICATE_DAY_MIN_MATCHES and differing == 0:
            removed_rows += conn.execute(
                """
                DELETE FROM price_history
                WHERE recorded_at = ?
                  AND EXISTS (
                      SELECT 1 FROM price_history p
                      WHERE p.card_id = price_history.card_id AND p.foil = price_history.foil
                        AND p.recorded_at = ? AND p.price_usd IS price_history.price_usd
                  )
                """,
                (day, previous),
            ).rowcount
            removed_days.append(day)
            continue  # same data as `previous`, so keep comparing against it
        previous = day
    if removed_days:
        log(f"Removed {removed_rows} duplicate price rows from re-imported days: {', '.join(removed_days)}")
    return removed_days, removed_rows


def ensure_collection_lots(conn):
    """Give every collection row without purchase lots a single lot.

    The lot's start price is the card's recorded price on the day it was acquired (or the
    closest price after that), so existing rows get a fixed baseline instead of whatever the
    oldest catalog price happens to be.
    """
    conn.execute(
        "DELETE FROM collection_lots WHERE collection_id NOT IN (SELECT id FROM collection)"
    )
    conn.execute(
        """
        INSERT INTO collection_lots (collection_id, quantity, start_price, acquired_at)
        SELECT
            c.id,
            COALESCE(c.quantity, 1),
            COALESCE(
                (SELECT h.price_usd FROM price_history h
                 WHERE h.card_id = c.card_id AND h.price_usd IS NOT NULL
                   AND h.recorded_at <= substr(COALESCE(c.acquired_at, CURRENT_TIMESTAMP), 1, 10)
                 ORDER BY h.recorded_at DESC LIMIT 1),
                (SELECT h.price_usd FROM price_history h
                 WHERE h.card_id = c.card_id AND h.price_usd IS NOT NULL
                 ORDER BY h.recorded_at ASC LIMIT 1),
                (SELECT cards.price_usd FROM cards WHERE cards.id = c.card_id)
            ),
            COALESCE(c.acquired_at, CURRENT_TIMESTAMP)
        FROM collection c
        WHERE NOT EXISTS (SELECT 1 FROM collection_lots l WHERE l.collection_id = c.id)
        """
    )


def normalize_history_date(value):
    parsed = parse_history_date(value)
    return parsed.isoformat() if parsed else None


def load_owned_price_data(conn):
    """Return (lots, history_by_card) for cards in the collection.

    history_by_card maps card_id -> (sorted dates, prices) for price_as_of lookups.
    """
    ensure_collection_lots(conn)
    lots = []
    for row in conn.execute(
        """
        SELECT l.collection_id, c.card_id, l.quantity, l.start_price, l.acquired_at
        FROM collection_lots l
        JOIN collection c ON c.id = l.collection_id
        ORDER BY l.acquired_at ASC, l.id ASC
        """
    ).fetchall():
        lots.append(
            {
                "collection_id": row["collection_id"],
                "card_id": row["card_id"],
                "quantity": row["quantity"] or 0,
                "start_price": row["start_price"],
                "acquired_date": normalize_history_date(row["acquired_at"]),
            }
        )

    history_by_card = {}
    for row in conn.execute(
        """
        SELECT card_id, recorded_at, price_usd
        FROM price_history
        WHERE price_usd IS NOT NULL
          AND card_id IN (SELECT card_id FROM collection)
        ORDER BY card_id, recorded_at
        """
    ).fetchall():
        date = normalize_history_date(row["recorded_at"])
        if date is None:
            continue
        dates, prices = history_by_card.setdefault(row["card_id"], ([], []))
        dates.append(date)
        prices.append(float(row["price_usd"]))
    return lots, history_by_card


def price_as_of(history_by_card, card_id, date):
    """Most recent recorded price on or before date, or None."""
    dates, prices = history_by_card.get(card_id, ([], []))
    index = bisect_right(dates, date)
    return prices[index - 1] if index else None


def first_price_on_or_after(history_by_card, card_id, date):
    dates, prices = history_by_card.get(card_id, ([], []))
    index = bisect_right(dates, date)
    if index and dates[index - 1] == date:
        return prices[index - 1]
    return prices[index] if index < len(prices) else None


def lot_start_price(lot, history_by_card):
    if lot["start_price"] is not None:
        return float(lot["start_price"])
    if lot["acquired_date"]:
        return first_price_on_or_after(history_by_card, lot["card_id"], lot["acquired_date"])
    return None


def migrate_product_matches_to_cards():
    product_db_path = BASE_DIR / "tcg_collection.db"
    if not product_db_path.exists():
        return 0

    product_conn = sqlite3.connect(str(product_db_path))
    product_conn.row_factory = sqlite3.Row
    app_conn = get_db_connection()

    matched_rows = product_conn.execute(
        """
        SELECT id, name, matched_card_name, matched_set_code, matched_set_name, matched_foil,
               scryfall_price_usd, match_score, match_method, is_matched
        FROM products
        WHERE is_matched = 1
        """
    ).fetchall()

    updated = 0
    for row in matched_rows:
        if not row["matched_card_name"]:
            continue
        updated_rows = app_conn.execute(
            """
            UPDATE cards
            SET matched_product_id = ?,
                matched_product_name = ?,
                matched_set_code = ?,
                matched_set_name = ?,
                matched_foil = ?,
                scryfall_price_usd = ?,
                match_score = ?,
                match_method = ?,
                is_matched = 1
            WHERE name = ? AND set_code = ? AND foil = ?
            """,
            (
                row["id"],
                row["name"],
                row["matched_set_code"],
                row["matched_set_name"],
                1 if row["matched_foil"] else 0,
                row["scryfall_price_usd"],
                row["match_score"],
                row["match_method"],
                row["matched_card_name"],
                row["matched_set_code"],
                1 if row["matched_foil"] else 0,
            ),
        )
        updated += updated_rows.rowcount

    app_conn.commit()
    app_conn.close()
    product_conn.close()
    return updated


def normalize_collection_quantities():
    conn = get_db_connection()
    conn.execute(
        """
        UPDATE collection
        SET quantity = CASE
            WHEN quantity % 3 = 0 THEN quantity / 3
            ELSE quantity
        END
        WHERE quantity > 0
        """
    )
    conn.commit()
    conn.close()


def import_external_products_into_collection():
    product_db_path = BASE_DIR / "tcg_collection.db"
    if not product_db_path.exists():
        return 0

    product_conn = sqlite3.connect(str(product_db_path))
    product_conn.row_factory = sqlite3.Row
    app_conn = get_db_connection()

    rows = product_conn.execute(
        """
        SELECT matched_card_name, matched_set_code, matched_foil, SUM(CAST(quantity AS INTEGER)) AS total_quantity
        FROM products
        WHERE is_matched = 1
          AND matched_card_name IS NOT NULL
          AND matched_set_code IS NOT NULL
        GROUP BY matched_card_name, matched_set_code, matched_foil
        ORDER BY matched_card_name, matched_set_code
        """
    ).fetchall()

    imported_count = 0
    for row in rows:
        matched_name = row["matched_card_name"]
        matched_set_code = row["matched_set_code"]
        matched_foil = 1 if row["matched_foil"] else 0
        qty = int(row["total_quantity"] or 1)
        if qty <= 0:
            qty = 1
        if qty % 3 == 0:
            qty = qty // 3

        card_row = app_conn.execute(
            """
            SELECT id
            FROM cards
            WHERE name = ? AND set_code = ? AND foil = ?
            ORDER BY last_updated DESC, id DESC
            LIMIT 1
            """,
            (matched_name, matched_set_code, matched_foil),
        ).fetchone()
        if card_row is None:
            continue

        existing = app_conn.execute(
            "SELECT id, quantity FROM collection WHERE card_id = ?",
            (card_row["id"],),
        ).fetchone()
        if existing:
            app_conn.execute(
                "UPDATE collection SET quantity = ? WHERE card_id = ?",
                (qty, card_row["id"]),
            )
        else:
            app_conn.execute(
                "INSERT INTO collection (card_id, quantity) VALUES (?, ?)",
                (card_row["id"], qty),
            )
        imported_count += 1

    app_conn.commit()
    app_conn.close()
    product_conn.close()
    return imported_count


def fetch_latest_default_cards_url():
    try:
        page = requests.get("https://scryfall.com/docs/api/bulk-data", timeout=30)
        page.raise_for_status()
        match = re.search(
            r'https://data\.scryfall\.io/default-cards/default-cards-\d+\.jsonl\.gz',
            page.text,
        )
        if match:
            log(f"Using Scryfall default cards URL from docs page: {match.group(0)}")
            return match.group(0)
        log("Default Cards link not found on docs page; trying bulk-data API")
    except requests.RequestException as exc:
        log(f"Docs page lookup failed: {exc!r}")

    try:
        bulk_response = requests.get("https://api.scryfall.com/bulk-data", timeout=30)
        bulk_response.raise_for_status()
        bulk_data = bulk_response.json()
        for item in bulk_data.get("data", []):
            if item.get("type") == "default_cards":
                download_url = item.get("jsonl_download_uri") or item.get("download_uri")
                if download_url:
                    log(f"Using Scryfall default cards URL: {download_url}")
                    return download_url
    except (requests.RequestException, ValueError) as exc:
        log(f"Bulk-data lookup failed: {exc!r}")
    # No stale fallback: importing an old file would record old prices as new data.
    raise RuntimeError("Could not determine the latest Scryfall default-cards file")


def scryfall_file_timestamp(url):
    """UTC datetime Scryfall generated this bulk file, from its name
    (default-cards-YYYYMMDDHHMMSS.jsonl.gz), or None if the name has no timestamp."""
    match = SCRYFALL_FILE_TIMESTAMP_RE.search(url or "")
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")



def normalize_search_query(raw_query):
    if raw_query is None:
        return ""

    cleaned = raw_query.strip()
    cleaned = re.sub(r"^\s*\d+\s+", "", cleaned)

    parts = [part.strip() for part in cleaned.split(" - ")]
    if len(parts) > 2:
        cleaned = " - ".join(parts[:2])

    cleaned = re.sub(r"\s+\[[A-Z0-9/]+\]\s*(?:\d+)?\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def save_search_cookie(response, query):
    if query:
        response.set_cookie("last_search", query, max_age=60 * 60 * 24 * 30, samesite="Lax")
    else:
        response.delete_cookie("last_search")
    return response


def save_analytics_period_cookie(response, period):
    if period:
        response.set_cookie("last_analytics_period", period, max_age=60 * 60 * 24 * 30, samesite="Lax")
    else:
        response.delete_cookie("last_analytics_period")
    return response


def extract_image_url(record):
    image_uris = record.get("image_uris") or {}
    image_url = image_uris.get("png") or image_uris.get("normal") or image_uris.get("small")
    if image_url:
        return image_url

    for face in record.get("card_faces") or []:
        face_uris = face.get("image_uris") or {}
        image_url = face_uris.get("png") or face_uris.get("normal") or face_uris.get("small")
        if image_url:
            return image_url
    return None


def upsert_card_record(record, conn=None, recorded_date=None):
    """Insert/update a card and its price for recorded_date (the Scryfall file's date,
    defaulting to today), so re-importing the same data never adds a second price row."""
    name = (record.get("name") or "").strip()
    flavor_name = (record.get("flavor_name") or "").strip()
    set_code = (record.get("set") or "").strip()
    set_name = (record.get("set_name") or "").strip()
    collector_number = (record.get("collector_number") or "").strip()
    if not name or not set_code or not collector_number:
        return

    prices = record.get("prices") or {}
    finish_details = {
        "nonfoil": (0, "usd"),
        "foil": (1, "usd_foil"),
        "etched": (2, "usd_etched"),
    }
    available_finishes = record.get("finishes")
    if not isinstance(available_finishes, list):
        available_finishes = [
            finish
            for finish, (_, price_key) in finish_details.items()
            if prices.get(price_key) is not None
        ]
        if not available_finishes:
            available_finishes = ["nonfoil", "foil"]
    variants = []
    for finish in available_finishes:
        if finish not in finish_details:
            continue
        foil_flag, price_key = finish_details[finish]
        variants.append((finish, foil_flag, prices.get(price_key)))

    image_url = extract_image_url(record)
    close_conn = conn is None
    if conn is None:
        conn = get_db_connection()
    now = utc_now().strftime("%Y-%m-%d %H:%M:%S")
    for finish, foil_flag, raw_price in variants:
        price_value = float(raw_price) if raw_price is not None else None
        conn.execute(
            """
            INSERT INTO cards (scryfall_id, name, flavor_name, set_code, set_name, collector_number, foil, finish, price_usd, image_url, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name, set_code, collector_number, foil)
            DO UPDATE SET
                scryfall_id = excluded.scryfall_id,
                flavor_name = excluded.flavor_name,
                set_name = excluded.set_name,
                finish = excluded.finish,
                price_usd = excluded.price_usd,
                image_url = excluded.image_url,
                last_updated = excluded.last_updated
            """,
            (
                record.get("id"),
                name,
                flavor_name,
                set_code,
                set_name,
                collector_number,
                foil_flag,
                finish,
                price_value,
                image_url,
                now,
            ),
        )

        card_row = conn.execute(
            "SELECT id FROM cards WHERE name = ? AND set_code = ? AND collector_number = ? AND foil = ?",
            (name, set_code, collector_number, foil_flag),
        ).fetchone()
        if card_row is None or price_value is None:
            continue

        conn.execute(
            """
            INSERT INTO price_history (card_id, recorded_at, foil, price_usd)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(card_id, recorded_at, foil)
            DO UPDATE SET price_usd = excluded.price_usd
            """,
            (
                card_row["id"],
                recorded_date or utc_now().strftime("%Y-%m-%d"),
                foil_flag,
                price_value,
            ),
        )
    if close_conn:
        conn.commit()
        conn.close()


def refresh_default_cards_catalog():
    log("Starting Scryfall default-cards refresh...")
    url = fetch_latest_default_cards_url()
    generated_at = scryfall_file_timestamp(url)

    conn = get_db_connection()
    try:
        latest = conn.execute(
            "SELECT source_url, data_generated_at FROM sync_log "
            "WHERE data_generated_at IS NOT NULL ORDER BY data_generated_at DESC LIMIT 1"
        ).fetchone()
        already_imported = conn.execute(
            "SELECT 1 FROM sync_log WHERE source_url = ? LIMIT 1", (url,)
        ).fetchone()
    finally:
        conn.close()
    if already_imported:
        log(f"Scryfall file already imported, skipping: {url}")
        return 0
    if generated_at and latest and latest["data_generated_at"] >= generated_at.strftime("%Y-%m-%d %H:%M:%S"):
        log(f"Scryfall file is not newer than the last import ({latest['source_url']}), skipping: {url}")
        return 0

    recorded_date = (generated_at or utc_now()).strftime("%Y-%m-%d")
    log(f"Downloading catalog from: {url} (prices recorded for {recorded_date})")
    response = requests.get(url, timeout=90)
    log(f"Download status: {response.status_code}, size={len(response.content)} bytes")
    response.raise_for_status()
    decompressed = gzip.decompress(response.content)
    log(f"Decompressed payload size: {len(decompressed)} bytes")
    lines = decompressed.decode("utf-8").splitlines()
    log(f"Catalog line count: {len(lines)}")

    valid_records = []
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("name") and record.get("set") and record.get("collector_number"):
            valid_records.append(record)

    with DB_MAINTENANCE_LOCK:
        conn = get_db_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for record in valid_records:
                upsert_card_record(record, conn=conn, recorded_date=recorded_date)
            conn.execute(
                "INSERT INTO sync_log (synced_at, cards_updated, source_url, data_generated_at) VALUES (?, ?, ?, ?)",
                (
                    utc_now().strftime("%Y-%m-%d %H:%M:%S"),
                    len(valid_records),
                    url,
                    generated_at.strftime("%Y-%m-%d %H:%M:%S") if generated_at else None,
                ),
            )
            conn.commit()
            shrink_wal(conn, "Scryfall refresh")
        finally:
            conn.close()

    processed = len(valid_records)
    log(f"Scryfall refresh complete: {processed} records processed.")
    return processed


PRUNE_LOCK_TIMEOUT_SECONDS = 60
PRUNE_RETRY_SECONDS = 30
PRUNE_MAX_WAIT_SECONDS = 30 * 60


def prune_untracked_data(retention_days=RETENTION_DAYS, max_wait_seconds=PRUNE_MAX_WAIT_SECONDS,
                         retry_seconds=PRUNE_RETRY_SECONDS):
    """Run _prune_untracked_data_once, waiting for the database if another process or connection
    holds a lock (e.g. a long catalog write), for up to max_wait_seconds in total."""
    deadline = time.monotonic() + max_wait_seconds
    while True:
        try:
            return _prune_untracked_data_once(retention_days)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            if time.monotonic() + retry_seconds > deadline:
                raise
            log(f"Database is busy ({exc}); retrying prune in {retry_seconds}s")
            time.sleep(retry_seconds)


def _prune_untracked_data_once(retention_days):
    """Delete data older than retention_days for cards/products that are not in the collection,
    then compact the database file.

    - price_history rows older than the cutoff are removed for untracked cards.
    - cards rows the catalog sync has not refreshed in retention_days (no longer in Scryfall's
      data, or sealed products whose prices stopped updating) are removed if untracked.
    Cards in the collection keep their full history.
    """
    now = utc_now()
    history_cutoff = (now - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    with DB_MAINTENANCE_LOCK:
        conn = get_db_connection(timeout=PRUNE_LOCK_TIMEOUT_SECONDS)
        try:
            # Measure card staleness against the last successful catalog sync rather than the
            # clock, so a sync outage can never make the whole catalog look stale and wipe it.
            last_sync = conn.execute("SELECT MAX(synced_at) FROM sync_log").fetchone()[0]
            reference = datetime.strptime(last_sync, "%Y-%m-%d %H:%M:%S") if last_sync else None
            stale_cards_sql = (
                "SELECT id FROM cards WHERE last_updated < ? "
                "AND id NOT IN (SELECT card_id FROM collection)"
            )

            conn.execute("BEGIN IMMEDIATE")
            history_deleted = conn.execute(
                "DELETE FROM price_history WHERE recorded_at < ? "
                "AND card_id NOT IN (SELECT card_id FROM collection)",
                (history_cutoff,),
            ).rowcount
            cards_deleted = 0
            if reference is not None:
                card_cutoff = (reference - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
                history_deleted += conn.execute(
                    f"DELETE FROM price_history WHERE card_id IN ({stale_cards_sql})",
                    (card_cutoff,),
                ).rowcount
                cards_deleted = conn.execute(
                    f"DELETE FROM cards WHERE id IN ({stale_cards_sql})",
                    (card_cutoff,),
                ).rowcount
            conn.commit()
            log(f"Pruned {history_deleted} price history rows and {cards_deleted} untracked cards older than {retention_days} days")

            log("Compacting database (VACUUM)...")
            conn.execute("VACUUM")
            log("Database compaction complete")
            shrink_wal(conn, "Prune")
        finally:
            conn.close()
    return history_deleted, cards_deleted


def scheduled_prune():
    try:
        prune_untracked_data()
    except Exception as exc:
        log(f"Scheduled prune FAILED: {exc!r}")


def import_sealed_products_into_collection():
    """Add sealed products from tcg_collection.db whose TCGplayer id matches a previously synced sealed product.

    Sealed prices are no longer refreshed (TCGplayer API access was revoked), so this only
    links against sealed rows already present in the cards table.
    """
    product_db_path = BASE_DIR / "tcg_collection.db"
    if not product_db_path.exists():
        return 0

    product_conn = sqlite3.connect(str(product_db_path))
    product_conn.row_factory = sqlite3.Row
    app_conn = get_db_connection()
    rows = product_conn.execute(
        """
        SELECT tcg_id, SUM(CAST(quantity AS INTEGER)) AS total_quantity
        FROM products
        WHERE tcg_id IS NOT NULL
        GROUP BY tcg_id
        """
    ).fetchall()

    imported = 0
    for row in rows:
        card = app_conn.execute(
            "SELECT id FROM cards WHERE is_sealed = 1 AND tcgplayer_product_id = ?",
            (row["tcg_id"],),
        ).fetchone()
        if card is None:
            continue
        qty = max(int(row["total_quantity"] or 1), 1)
        if app_conn.execute(
            "SELECT 1 FROM collection WHERE card_id = ?", (card["id"],)
        ).fetchone() is None:
            app_conn.execute(
                "INSERT INTO collection (card_id, quantity) VALUES (?, ?)",
                (card["id"], qty),
            )
            imported += 1

    app_conn.commit()
    app_conn.close()
    product_conn.close()
    return imported


def needs_daily_sync(last_sync):
    if last_sync is None:
        return True

    if isinstance(last_sync, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                last_sync = datetime.strptime(last_sync, fmt)
                break
            except ValueError:
                continue
        else:
            return True

    return (utc_now() - last_sync).total_seconds() >= 86400


def sync_if_needed():
    log("Scheduled sync check triggered")
    conn = get_db_connection()
    last_sync = conn.execute(
        "SELECT synced_at FROM sync_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    last_sync_value = last_sync["synced_at"] if last_sync is not None else None
    log(f"Last sync (UTC): {last_sync_value}")
    #if last_sync is not None and not needs_daily_sync(last_sync["synced_at"]):
    #    log("Last sync is less than 24 hours old; skipping refresh")
    #    return False

    log("Sync needed; starting refresh")
    try:
        processed = refresh_default_cards_catalog()
    except Exception as exc:
        log(f"Scheduled sync FAILED: {exc!r}")
        raise
    log(f"Scheduled sync succeeded: {processed} records")
    return True



def schedule_daily_scryfall_sync():
    global SCHEDULER
    if SCHEDULER is not None and SCHEDULER.running:
        return SCHEDULER

    scheduler = BackgroundScheduler(timezone="America/Chicago")
    scheduler.add_job(
        sync_if_needed,
        "cron",
        hour=5,
        minute=0,
        id="daily_scryfall_sync",
        replace_existing=True,
    )
    # Runs after the 05:00 catalog sync; DB_MAINTENANCE_LOCK makes it wait if the sync overruns.
    scheduler.add_job(
        scheduled_prune,
        "cron",
        hour=6,
        minute=0,
        id="daily_prune",
        replace_existing=True,
    )
    scheduler.start()
    job = scheduler.get_job("daily_scryfall_sync")
    log(f"Daily Scryfall sync scheduler started; next run at {job.next_run_time}")
    log(f"Daily prune of untracked data older than {RETENTION_DAYS} days; next run at {scheduler.get_job('daily_prune').next_run_time}")
    SCHEDULER = scheduler
    return scheduler


def start_background_sync(prune=False):
    """Refresh the catalog in a background thread.

    With prune=True (used at startup) it then removes untracked data older than RETENTION_DAYS
    and compacts the database. The prune runs even if the refresh fails; it is safe either way
    because stale-card detection is measured from the last successful sync.
    """
    global SYNC_THREAD

    with SYNC_LOCK:
        if SYNC_THREAD is not None and SYNC_THREAD.is_alive():
            return False

        def worker():
            global SYNC_THREAD
            log("Background sync started")
            try:
                try:
                    refresh_default_cards_catalog()
                except Exception as exc:
                    log(f"Background catalog refresh FAILED: {exc!r}")
                    if not prune:
                        raise
                if prune:
                    try:
                        prune_untracked_data()
                    except Exception as exc:
                        log(f"Startup prune FAILED: {exc!r}")
            finally:
                with SYNC_LOCK:
                    SYNC_THREAD = None
                log("Background sync finished")

        SYNC_THREAD = threading.Thread(target=worker, daemon=True)
        SYNC_THREAD.start()
        return True


def get_collection_rows():
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT
            c.id AS collection_id,
            c.quantity,
            cards.id AS card_id,
            cards.name,
            cards.set_code,
            cards.set_name,
            cards.collector_number,
            cards.foil,
            cards.finish,
            cards.price_usd,
            cards.image_url,
            cards.last_updated
        FROM collection c
        JOIN cards ON cards.id = c.card_id
        ORDER BY cards.name ASC, cards.set_code ASC, cards.collector_number ASC
        """
    ).fetchall()
    lots, history_by_card = load_owned_price_data(conn)
    conn.commit()
    conn.close()

    # Quantity-weighted start price across every time this card was added.
    cost_by_collection = {}
    for lot in lots:
        start_price = lot_start_price(lot, history_by_card)
        if start_price is None:
            continue
        totals = cost_by_collection.setdefault(lot["collection_id"], [0, 0.0, lot["acquired_date"]])
        totals[0] += lot["quantity"]
        totals[1] += lot["quantity"] * start_price

    enriched_rows = []
    for row in rows:
        row_dict = dict(row)
        quantity, cost, acquired_date = cost_by_collection.get(row["collection_id"], (0, 0.0, None))
        start_price = (cost / quantity) if quantity else None
        current_price = row["price_usd"]
        if start_price and current_price is not None:
            percent_change = (current_price - start_price) / start_price * 100
        else:
            percent_change = 0.0
        row_dict["start_price"] = round(start_price, 2) if start_price is not None else None
        row_dict["acquired_date"] = acquired_date
        row_dict["percent_change"] = round(percent_change, 2)
        enriched_rows.append(row_dict)

    return enriched_rows


def get_card_history(card_id):
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT recorded_at, price_usd, foil
        FROM price_history
        WHERE card_id = ?
        ORDER BY recorded_at ASC
        """,
        (card_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_card_by_id(card_id):
    conn = get_db_connection()
    card = conn.execute("SELECT * FROM cards WHERE id = ?", (card_id,)).fetchone()
    conn.close()
    return dict(card) if card else None


def get_search_results(query):
    if not query:
        return []

    tokens = query.split()
    haystack = (
        "lower(name || ' ' || COALESCE(flavor_name, '') || ' ' || "
        "COALESCE(set_name, '') || ' ' || set_code)"
    )
    where_clause = " AND ".join(f"{haystack} LIKE lower(?) ESCAPE '\\'" for _ in tokens)
    params = [
        "%" + token.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        for token in tokens
    ]

    conn = get_db_connection()
    search_results = conn.execute(
        f"""
        SELECT *
        FROM cards
        WHERE {where_clause}
        ORDER BY COALESCE(price_usd, 0) DESC, name ASC, set_code ASC, collector_number ASC
        """,
        params,
    ).fetchall()
    conn.close()

    grouped = {}
    for row in search_results:
        key = (row["name"], row["set_code"], row["collector_number"], row["set_name"])
        entry = grouped.setdefault(
            key,
            {
                "name": row["name"],
                "set_code": row["set_code"],
                "set_name": row["set_name"],
                "collector_number": row["collector_number"],
                "image_url": row["image_url"],
                "is_sealed": bool(row["is_sealed"]),
                "variants": {},
            },
        )
        finish = row["finish"]
        entry["variants"][finish] = {
            "id": row["id"],
            "finish": finish,
            "price_usd": row["price_usd"],
            "image_url": row["image_url"],
            "last_updated": row["last_updated"],
        }
        if not entry["image_url"]:
            entry["image_url"] = row["image_url"]

    priced_entries = []
    for entry in grouped.values():
        priced = {k: v for k, v in entry["variants"].items() if v["price_usd"] is not None}
        if not priced:
            continue
        entry["variants"] = priced
        entry["first_variant_id"] = next(iter(priced.values()))["id"]
        priced_entries.append(entry)

    results = priced_entries
    sealed_count = sum(1 for entry in results if entry["is_sealed"])
    if results and sealed_count * 2 > len(results):
        results.sort(key=lambda entry: (entry["name"].lower(), entry["set_code"]))
    return results


def parse_history_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    value_str = str(value).strip()
    if not value_str:
        return None
    try:
        return datetime.fromisoformat(value_str).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value_str, fmt).date()
        except ValueError:
            continue
    return None


def add_months_to_date(base_date, months):
    month_index = (base_date.year * 12) + (base_date.month - 1) + months
    year = month_index // 12
    month = (month_index % 12) + 1
    day = min(base_date.day, 28)
    return base_date.replace(year=year, month=month, day=day)


def get_analytics_data(period="all"):
    valid_periods = {"1m": 1, "3m": 3, "6m": 6, "1y": 12, "all": None}
    period_key = (period or "all").lower()
    lookup_months = valid_periods.get(period_key)

    conn = get_db_connection()
    collection_rows = conn.execute(
        """
        SELECT
            c.id AS collection_id,
            c.quantity,
            cards.id AS card_id,
            cards.name,
            cards.set_code,
            cards.set_name,
            cards.collector_number,
            cards.foil,
            cards.finish,
            cards.price_usd,
            cards.image_url
        FROM collection c
        JOIN cards ON cards.id = c.card_id
        ORDER BY cards.name ASC, cards.set_code ASC, cards.collector_number ASC
        """
    ).fetchall()

    lots, history_by_card = load_owned_price_data(conn)
    conn.commit()
    conn.close()

    lot_dates = {lot["acquired_date"] for lot in lots if lot["acquired_date"]}
    history_dates = {date for dates, _ in history_by_card.values() for date in dates}
    all_known_dates = history_dates | lot_dates
    latest_date = max(all_known_dates) if all_known_dates else utc_now().date().isoformat()
    if lookup_months is None:
        boundary = None
    else:
        boundary = add_months_to_date(datetime.fromisoformat(latest_date).date(), -lookup_months).isoformat()

    current_collection_value = sum((row["quantity"] or 1) * (row["price_usd"] or 0) for row in collection_rows)

    set_totals = {}
    finish_totals = {}
    for row in collection_rows:
        set_label = row["set_name"] or row["set_code"]
        set_totals.setdefault(set_label, {"quantity": 0, "total_value": 0.0})
        set_totals[set_label]["quantity"] += row["quantity"] or 1
        set_totals[set_label]["total_value"] += (row["quantity"] or 1) * (row["price_usd"] or 0)

        finish_label = row["finish"].replace("_", " ").title()
        finish_totals.setdefault(finish_label, {"quantity": 0, "total_value": 0.0})
        finish_totals[finish_label]["quantity"] += row["quantity"] or 1
        finish_totals[finish_label]["total_value"] += (row["quantity"] or 1) * (row["price_usd"] or 0)

    # Each lot only counts from the day it was acquired, measured against a fixed baseline:
    # its acquisition price, or (for lots held before the selected window) its price at the
    # start of the window. Adding a card therefore raises value and baseline equally and
    # never shows up as gain or loss.
    first_acquired = min(lot_dates) if lot_dates else None
    trend_dates = sorted(
        date
        for date in all_known_dates
        if first_acquired and date >= first_acquired and (boundary is None or date >= boundary)
    )
    window_start = trend_dates[0] if trend_dates else None

    for lot in lots:
        start_price = lot_start_price(lot, history_by_card)
        if window_start and lot["acquired_date"] and lot["acquired_date"] < window_start:
            baseline = price_as_of(history_by_card, lot["card_id"], window_start)
            lot["baseline"] = baseline if baseline is not None else start_price
        else:
            lot["baseline"] = start_price
    active_lots = [lot for lot in lots if lot["baseline"] is not None and lot["acquired_date"]]

    collection_trend = []
    for date in trend_dates:
        total_value = 0.0
        basis = 0.0
        for lot in active_lots:
            if lot["acquired_date"] > date:
                continue
            price = price_as_of(history_by_card, lot["card_id"], date)
            if price is None:
                price = lot["baseline"]
            total_value += lot["quantity"] * price
            basis += lot["quantity"] * lot["baseline"]
        dollar_change = total_value - basis
        percent_change = (dollar_change / basis * 100) if basis else 0.0
        collection_trend.append(
            {
                "date": date,
                "total_value": round(total_value, 2),
                "cost_basis": round(basis, 2),
                "dollar_change": round(dollar_change, 2),
                "percent_change": round(percent_change, 2),
                "dollar_change_display": f"${dollar_change:+.2f}",
                "percent_change_display": f"{percent_change:+.2f}%",
            }
        )

    overall_change = collection_trend[-1]["dollar_change"] if collection_trend else 0.0
    overall_percent_change = collection_trend[-1]["percent_change"] if collection_trend else 0.0

    lots_by_card = {}
    for lot in active_lots:
        lots_by_card.setdefault(lot["card_id"], []).append(lot)

    card_change_rows = []
    for row in collection_rows:
        card_lots = lots_by_card.get(row["card_id"])
        if not card_lots or not trend_dates:
            continue
        quantity = sum(lot["quantity"] for lot in card_lots)
        if not quantity:
            continue
        start_price = sum(lot["quantity"] * lot["baseline"] for lot in card_lots) / quantity
        latest_price = price_as_of(history_by_card, row["card_id"], trend_dates[-1])
        if latest_price is None:
            latest_price = row["price_usd"] if row["price_usd"] is not None else start_price
        dollar_change = latest_price - start_price
        percent_change = ((dollar_change / start_price) * 100) if start_price else 0.0
        card_change_rows.append(
            {
                "card_id": row["card_id"],
                "name": row["name"],
                "set_code": row["set_code"],
                "set_name": row["set_name"],
                "start_price": round(start_price, 2),
                "latest_price": round(latest_price, 2),
                "dollar_change": round(dollar_change, 2),
                "percent_change": round(percent_change, 2),
                "dollar_change_display": f"${dollar_change:+.2f}",
                "percent_change_display": f"{percent_change:+.2f}%",
            }
        )

    winners = sorted(
        (item for item in card_change_rows if item["percent_change"] > 0),
        key=lambda item: item["percent_change"],
        reverse=True,
    )[:5]
    losers = sorted(
        (item for item in card_change_rows if item["percent_change"] < 0),
        key=lambda item: item["percent_change"],
    )[:5]

    return {
        "period": period_key,
        "summary": {
            "total_rows": len(collection_rows),
            "total_cards": sum(row["quantity"] or 1 for row in collection_rows),
            "foil_cards": sum((row["quantity"] or 1) for row in collection_rows if row["finish"] == "foil"),
            "nonfoil_cards": sum((row["quantity"] or 1) for row in collection_rows if row["finish"] == "nonfoil"),
            "etched_cards": sum((row["quantity"] or 1) for row in collection_rows if row["finish"] == "etched"),
            "total_value": round(current_collection_value, 2),
            "overall_change": round(overall_change, 2),
            "overall_percent_change": round(overall_percent_change, 2),
            "overall_change_display": f"${overall_change:+.2f}",
            "overall_percent_change_display": f"{overall_percent_change:+.2f}%",
        },
        "collection_trend": collection_trend,
        "top_winners": winners,
        "top_losers": losers,
        "top_cards": [
            {
                "name": row["name"],
                "set_code": row["set_code"],
                "set_name": row["set_name"],
                "quantity": row["quantity"] or 1,
                "total_value": round((row["quantity"] or 1) * (row["price_usd"] or 0), 2),
            }
            for row in sorted(collection_rows, key=lambda item: ((item["quantity"] or 1) * (item["price_usd"] or 0)), reverse=True)[:8]
        ],
        "by_set": [
            {
                "set_label": set_label,
                "quantity": values["quantity"],
                "total_value": round(values["total_value"], 2),
            }
            for set_label, values in sorted(set_totals.items(), key=lambda item: item[1]["total_value"], reverse=True)[:8]
        ],
        "by_finish": [
            {
                "finish": finish_label,
                "quantity": values["quantity"],
                "total_value": round(values["total_value"], 2),
            }
            for finish_label, values in finish_totals.items()
        ],
    }


@app.route("/")
def index():
    collection_rows = get_collection_rows()
    total_value = sum((row["quantity"] or 1) * (row["price_usd"] or 0) for row in collection_rows)
    conn = get_db_connection()
    try:
        last_sync = conn.execute(
            "SELECT synced_at FROM sync_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        last_sync = None
    finally:
        conn.close()

    return render_template(
        "index.html",
        collection_rows=collection_rows,
        total_value=total_value,
        last_sync=last_sync[0] if last_sync else "Not synced yet",
    )


@app.route("/search")
def search_page():
    raw_query = (request.args.get("q") or request.cookies.get("last_search") or "").strip()
    query = normalize_search_query(raw_query)
    search_results = get_search_results(query)
    response = make_response(render_template("search.html", query=query, search_results=search_results))
    return save_search_cookie(response, query)


@app.route("/analytics")
def analytics():
    period = (request.args.get("period") or request.cookies.get("last_analytics_period") or "all").lower()
    analytics_data = get_analytics_data(period=period)
    response = make_response(render_template("analytics.html", analytics=analytics_data, period=period))
    return save_analytics_period_cookie(response, period)


@app.route("/add-to-collection", methods=["POST"])
def add_to_collection():
    card_id = request.form.get("card_id", type=int)
    quantity = max(1, min(request.form.get("quantity", type=int) or 1, 999))
    search_q = request.form.get("search_q") or request.cookies.get("last_search") or ""
    if card_id is None:
        flash("Choose a card before adding it to your collection.")
        return redirect(url_for("search_page", q=search_q)) if search_q else redirect(url_for("index"))

    card = get_card_by_id(card_id)
    if not card:
        flash("That card could not be found in the catalog.")
        return redirect(url_for("search_page", q=search_q)) if search_q else redirect(url_for("index"))

    now = utc_now()
    conn = get_db_connection()
    # Make sure any older rows already have their lot before this purchase is appended.
    ensure_collection_lots(conn)
    existing = conn.execute(
        "SELECT id, quantity FROM collection WHERE card_id = ?",
        (card_id,),
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE collection SET quantity = quantity + ? WHERE card_id = ?",
            (quantity, card_id),
        )
        collection_id = existing["id"]
    else:
        collection_id = conn.execute(
            "INSERT INTO collection (card_id, quantity, acquired_at) VALUES (?, ?, ?)",
            (card_id, quantity, now.strftime("%Y-%m-%d %H:%M:%S")),
        ).lastrowid
    # Lock in the price paid for this purchase; it is never updated afterwards.
    conn.execute(
        "INSERT INTO collection_lots (collection_id, quantity, start_price, acquired_at) VALUES (?, ?, ?, ?)",
        (collection_id, quantity, card["price_usd"], now.strftime("%Y-%m-%d %H:%M:%S")),
    )
    if card["price_usd"] is not None:
        # Anchor the price history on the acquisition day so the graph starts at the price paid.
        conn.execute(
            "INSERT OR IGNORE INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)",
            (card_id, now.strftime("%Y-%m-%d"), card["foil"], card["price_usd"]),
        )
    conn.commit()
    conn.close()

    flash(f"Added {quantity} × {card['name']} ({card['set_code']}) to your collection.")
    target = url_for("search_page", q=search_q) if search_q else url_for("index")
    response = redirect(target)
    return save_search_cookie(response, search_q)


@app.route("/remove-from-collection", methods=["POST"])
def remove_from_collection():
    collection_id = request.form.get("collection_id", type=int)
    if collection_id is None:
        flash("Choose a collection entry to delete.")
        return redirect(url_for("index"))

    conn = get_db_connection()
    ensure_collection_lots(conn)
    row = conn.execute(
        """
        SELECT collection.quantity, cards.name, cards.set_code
        FROM collection
        JOIN cards ON cards.id = collection.card_id
        WHERE collection.id = ?
        """,
        (collection_id,),
    ).fetchone()
    if row is None:
        conn.close()
        flash("That collection entry could not be found.")
        return redirect(url_for("index"))

    # Remove one copy per click, taken from the most recent purchase so it undoes the latest add
    # and earlier copies keep their locked-in start prices.
    remaining = (row["quantity"] or 1) - 1
    if remaining <= 0:
        conn.execute("DELETE FROM collection WHERE id = ?", (collection_id,))
        conn.execute("DELETE FROM collection_lots WHERE collection_id = ?", (collection_id,))
    else:
        conn.execute("UPDATE collection SET quantity = ? WHERE id = ?", (remaining, collection_id))
        lot = conn.execute(
            "SELECT id, quantity FROM collection_lots WHERE collection_id = ? ORDER BY acquired_at DESC, id DESC LIMIT 1",
            (collection_id,),
        ).fetchone()
        if lot is not None:
            if lot["quantity"] > 1:
                conn.execute("UPDATE collection_lots SET quantity = quantity - 1 WHERE id = ?", (lot["id"],))
            else:
                conn.execute("DELETE FROM collection_lots WHERE id = ?", (lot["id"],))
    conn.commit()
    conn.close()

    label = f"{row['name']} ({row['set_code']})"
    if remaining <= 0:
        log(f"Removed collection entry {collection_id}: {label}")
        flash(f"Removed {label} from your collection.")
    else:
        log(f"Removed 1 copy of collection entry {collection_id}: {label}; {remaining} left")
        flash(f"Removed 1 × {label}. {remaining} left in your collection.")
    return redirect(url_for("index"))


@app.route("/sync-scryfall", methods=["POST"])
def sync_scryfall():
    if not start_background_sync():
        flash("A Scryfall sync is already running in the background.")
        return redirect(url_for("index"))

    flash("Scryfall sync started in the background. This can take a while, then the catalog will be ready for search.")
    return redirect(url_for("index"))


@app.route("/card/<int:card_id>")
def card_detail(card_id):
    card = get_card_by_id(card_id)
    if not card:
        flash("Card not found.")
        return redirect(url_for("index"))

    history = get_card_history(card_id)
    return render_template("card_detail.html", card=card, history=history)


log("Starting app initialization...")
init_db()
#normalize_collection_quantities()
'''
log("Migrating product match metadata into cards...")
log(f"Updated {migrate_product_matches_to_cards()} card rows with product match metadata.")
log("Importing matched product rows into collection...")
log(f"Imported {import_external_products_into_collection()} matched product rows into the collection.")
log(f"Imported {import_sealed_products_into_collection()} sealed products into the collection.")
conn = get_db_connection()
conn.execute(
    """
    UPDATE collection
    SET name = (SELECT cards.name FROM cards WHERE cards.id = collection.card_id),
        set_code = (SELECT cards.set_code FROM cards WHERE cards.id = collection.card_id),
        set_name = (SELECT cards.set_name FROM cards WHERE cards.id = collection.card_id),
        collector_number = (SELECT cards.collector_number FROM cards WHERE cards.id = collection.card_id),
        foil = (SELECT cards.foil FROM cards WHERE cards.id = collection.card_id),
        image_url = (SELECT cards.image_url FROM cards WHERE cards.id = collection.card_id),
        price_usd = (SELECT cards.price_usd FROM cards WHERE cards.id = collection.card_id),
        last_updated = (SELECT cards.last_updated FROM cards WHERE cards.id = collection.card_id)
    WHERE card_id IS NOT NULL
    """
)
conn.commit()
conn.close()

'''
#normalize_collection_quantities()
if os.getenv("MTG_TRACKER_DISABLE_BACKGROUND") != "1":
    start_background_sync(prune=True)
    log("Database initialized. Startup refresh + prune/compaction running in the background; starting the daily scheduler.")
    schedule_daily_scryfall_sync()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log(f"Launching Flask dev server on port {port}...")
    # Debug mode (interactive debugger) only when FLASK_DEBUG=1. The reloader stays off: it re-runs
    # this module in a second process, which would start a second copy of the sync, the prune and
    # the scheduler that then fight over the database lock.
    debug = os.getenv("FLASK_DEBUG") == "1"
    app.run(debug=debug, use_reloader=False, host="0.0.0.0", port=port)

import gzip
import json
import logging
import os
import re
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, flash, make_response, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "mtg_tracker.db"
SCRYFALL_DEFAULT_CARDS_URL = (
    "https://data.scryfall.io/default-cards/default-cards-20261002210553.jsonl.gz"
)

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-me"
app.config["DATABASE"] = str(DB_PATH)
app.config["CATALOG_SYNCED"] = False
SYNC_LOCK = threading.Lock()
SYNC_THREAD = None
SCHEDULER = None

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def log(*args):
    print(datetime.now().strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True)


def get_db_connection():
    conn = sqlite3.connect(app.config["DATABASE"])
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


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
        "match_score": "INTEGER",
        "match_method": "TEXT",
        "is_matched": "INTEGER DEFAULT 0",
    }.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE cards ADD COLUMN {column_name} {column_sql}")
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_name ON cards(name COLLATE NOCASE)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_set_number ON cards(set_code, collector_number)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_card_date ON price_history(card_id, recorded_at)"
    )
    conn.commit()
    conn.close()


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
        bulk_response = requests.get("https://api.scryfall.io/bulk-data", timeout=30)
        bulk_response.raise_for_status()
        bulk_data = bulk_response.json()
        for item in bulk_data.get("data", []):
            if item.get("type") == "default_cards":
                log("Using Scryfall default cards URL:", item.get("download_uri"))
                return item.get("download_uri") or SCRYFALL_DEFAULT_CARDS_URL
    except requests.RequestException:
        pass
    return SCRYFALL_DEFAULT_CARDS_URL


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


def upsert_card_record(record, conn=None):
    name = (record.get("name") or "").strip()
    flavor_name = (record.get("flavor_name") or "").strip()
    set_code = (record.get("set") or "").strip()
    set_name = (record.get("set_name") or "").strip()
    collector_number = (record.get("collector_number") or "").strip()
    if not name or not set_code or not collector_number:
        return

    prices = record.get("prices") or {}
    variants = []
    for foil_flag, price_key in [(0, "usd"), (1, "usd_foil")]:
        price_value = prices.get(price_key)
        if price_value is None:
            variants.append((foil_flag, None))
        else:
            variants.append((foil_flag, float(price_value)))

    image_url = extract_image_url(record)
    close_conn = conn is None
    if conn is None:
        conn = get_db_connection()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for foil_flag, price_value in variants:
        conn.execute(
            """
            INSERT INTO cards (scryfall_id, name, flavor_name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name, set_code, collector_number, foil)
            DO UPDATE SET
                scryfall_id = excluded.scryfall_id,
                flavor_name = excluded.flavor_name,
                set_name = excluded.set_name,
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
                datetime.utcnow().strftime("%Y-%m-%d"),
                foil_flag,
                price_value,
            ),
        )
    if close_conn:
        conn.close()


def refresh_default_cards_catalog():
    log("Starting Scryfall default-cards refresh...")
    url = fetch_latest_default_cards_url()
    log(f"Downloading catalog from: {url}")
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

    conn = get_db_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for record in valid_records:
            upsert_card_record(record, conn=conn)
        conn.execute(
            "INSERT INTO sync_log (synced_at, cards_updated) VALUES (?, ?)",
            (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), len(valid_records)),
        )
        conn.commit()
    finally:
        conn.close()

    processed = len(valid_records)
    log(f"Scryfall refresh complete: {processed} records processed.")
    return processed


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

    return (datetime.utcnow() - last_sync).total_seconds() >= 86400


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
    scheduler.start()
    job = scheduler.get_job("daily_scryfall_sync")
    log(f"Daily Scryfall sync scheduler started; next run at {job.next_run_time}")
    SCHEDULER = scheduler
    return scheduler


def start_background_sync():
    global SYNC_THREAD

    with SYNC_LOCK:
        if SYNC_THREAD is not None and SYNC_THREAD.is_alive():
            return False

        def worker():
            global SYNC_THREAD
            log("Background sync started")
            try:
                refresh_default_cards_catalog()
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
            cards.price_usd,
            cards.image_url,
            cards.last_updated
        FROM collection c
        JOIN cards ON cards.id = c.card_id
        ORDER BY cards.name ASC, cards.set_code ASC, cards.collector_number ASC
        """
    ).fetchall()
    history_rows = conn.execute(
        "SELECT card_id, recorded_at, price_usd FROM price_history ORDER BY recorded_at ASC, card_id ASC"
    ).fetchall()
    conn.close()

    history_by_card = {}
    for row in history_rows:
        history_by_card.setdefault(row["card_id"], []).append(float(row["price_usd"] or 0))

    enriched_rows = []
    for row in rows:
        prices = history_by_card.get(row["card_id"], [])
        if len(prices) >= 2:
            start_price = prices[0]
            latest_price = prices[-1]
            percent_change = ((latest_price - start_price) / start_price * 100) if start_price else 0.0
        else:
            percent_change = 0.0
        row_dict = dict(row)
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

    conn = get_db_connection()
    search_results = conn.execute(
        """
        SELECT *
        FROM cards
        WHERE lower(name) LIKE lower(?)
           OR lower(COALESCE(flavor_name, '')) LIKE lower(?)
           OR lower(COALESCE(flavor_name, '') || ' - ' || name) LIKE lower(?)
           OR lower(name || ' - ' || COALESCE(flavor_name, '')) LIKE lower(?)
        ORDER BY COALESCE(price_usd, 0) DESC, name ASC, set_code ASC, collector_number ASC
        """,
        (f"%{query}%",) * 4,
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
                "variants": {},
            },
        )
        finish = "foil" if row["foil"] else "nonfoil"
        entry["variants"][finish] = {
            "id": row["id"],
            "foil": bool(row["foil"]),
            "price_usd": row["price_usd"],
            "image_url": row["image_url"],
            "last_updated": row["last_updated"],
        }
        if not entry["image_url"]:
            entry["image_url"] = row["image_url"]

    # Drop finishes with no price (e.g. no non-foil exists) unless nothing else is available
    for entry in grouped.values():
        priced = {k: v for k, v in entry["variants"].items() if v["price_usd"] is not None}
        if priced:
            entry["variants"] = priced

    return list(grouped.values())


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
            cards.price_usd,
            cards.image_url
        FROM collection c
        JOIN cards ON cards.id = c.card_id
        ORDER BY cards.name ASC, cards.set_code ASC, cards.collector_number ASC
        """
    ).fetchall()

    history_rows = conn.execute(
        "SELECT card_id, recorded_at, price_usd FROM price_history ORDER BY recorded_at ASC, card_id ASC"
    ).fetchall()
    conn.close()

    history_dates = [parse_history_date(row["recorded_at"]) for row in history_rows]
    history_dates = [d for d in history_dates if d is not None]
    latest_history_date = max(history_dates) if history_dates else datetime.utcnow().date()
    start_boundary = latest_history_date if lookup_months is None else add_months_to_date(latest_history_date, -lookup_months)

    filtered_history_rows = []
    for row in history_rows:
        recorded_date = parse_history_date(row["recorded_at"])
        if recorded_date is None:
            continue
        if lookup_months is None or recorded_date >= start_boundary:
            filtered_history_rows.append(row)

    collection_qty = {row["card_id"]: row["quantity"] or 1 for row in collection_rows}
    current_collection_value = sum((row["quantity"] or 1) * (row["price_usd"] or 0) for row in collection_rows)

    set_totals = {}
    finish_totals = {}
    for row in collection_rows:
        set_label = row["set_name"] or row["set_code"]
        set_totals.setdefault(set_label, {"quantity": 0, "total_value": 0.0})
        set_totals[set_label]["quantity"] += row["quantity"] or 1
        set_totals[set_label]["total_value"] += (row["quantity"] or 1) * (row["price_usd"] or 0)

        finish_label = "Foil" if row["foil"] == 1 else "Non-foil"
        finish_totals.setdefault(finish_label, {"quantity": 0, "total_value": 0.0})
        finish_totals[finish_label]["quantity"] += row["quantity"] or 1
        finish_totals[finish_label]["total_value"] += (row["quantity"] or 1) * (row["price_usd"] or 0)

    trend_totals = {}
    for row in filtered_history_rows:
        card_id = row["card_id"]
        if card_id not in collection_qty:
            continue
        date_key = row["recorded_at"]
        trend_totals.setdefault(date_key, 0.0)
        trend_totals[date_key] += float(row["price_usd"] or 0) * collection_qty[card_id]

    ordered_trend = [
        {"date": date, "total_value": round(float(value), 2)}
        for date, value in sorted(trend_totals.items())
    ]

    start_value = ordered_trend[0]["total_value"] if ordered_trend else 0.0
    collection_trend = []
    for index, entry in enumerate(ordered_trend):
        if index == 0:
            dollar_change = 0.0
            percent_change = 0.0
        else:
            dollar_change = float(entry["total_value"]) - float(start_value)
            percent_change = ((dollar_change / start_value) * 100) if start_value else 0.0

        collection_trend.append(
            {
                "date": entry["date"],
                "total_value": float(entry["total_value"]),
                "dollar_change": round(dollar_change, 2),
                "percent_change": round(percent_change, 2),
                "dollar_change_display": f"${dollar_change:+.2f}",
                "percent_change_display": f"{percent_change:+.2f}%",
            }
        )

    overall_change = 0.0
    overall_percent_change = 0.0
    if collection_trend:
        overall_change = float(collection_trend[-1]["total_value"]) - float(start_value)
        overall_percent_change = ((overall_change / start_value) * 100) if start_value else 0.0

    history_by_card = {}
    for row in filtered_history_rows:
        history_by_card.setdefault(row["card_id"], []).append({
            "date": row["recorded_at"],
            "price_usd": float(row["price_usd"] or 0),
        })

    card_change_rows = []
    for row in collection_rows:
        card_prices = sorted(history_by_card.get(row["card_id"], []), key=lambda item: item["date"])
        if len(card_prices) < 2:
            continue
        start_price = card_prices[0]["price_usd"]
        latest_price = card_prices[-1]["price_usd"]
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

    winners = sorted(card_change_rows, key=lambda item: item["percent_change"], reverse=True)[:5]
    losers = sorted(card_change_rows, key=lambda item: item["percent_change"])[:5]

    return {
        "period": period_key,
        "summary": {
            "total_rows": len(collection_rows),
            "total_cards": sum(row["quantity"] or 1 for row in collection_rows),
            "foil_cards": sum((row["quantity"] or 1) for row in collection_rows if row["foil"] == 1),
            "nonfoil_cards": sum((row["quantity"] or 1) for row in collection_rows if row["foil"] == 0),
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
    quantity = request.form.get("quantity", type=int) or 1
    search_q = request.form.get("search_q") or request.cookies.get("last_search") or ""
    if card_id is None:
        flash("Choose a card before adding it to your collection.")
        return redirect(url_for("search_page", q=search_q)) if search_q else redirect(url_for("index"))

    card = get_card_by_id(card_id)
    if not card:
        flash("That card could not be found in the catalog.")
        return redirect(url_for("search_page", q=search_q)) if search_q else redirect(url_for("index"))

    conn = get_db_connection()
    existing = conn.execute(
        "SELECT id, quantity FROM collection WHERE card_id = ?",
        (card_id,),
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE collection SET quantity = quantity + ? WHERE card_id = ?",
            (quantity, card_id),
        )
    else:
        conn.execute(
            "INSERT INTO collection (card_id, quantity) VALUES (?, ?)",
            (card_id, quantity),
        )
    conn.commit()
    conn.close()

    flash(f"Added {card['name']} ({card['set_code']}) to your collection.")
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
    row = conn.execute(
        """
        SELECT cards.name, cards.set_code
        FROM collection
        JOIN cards ON cards.id = collection.card_id
        WHERE collection.id = ?
        """,
        (collection_id,),
    ).fetchone()
    deleted = conn.execute("DELETE FROM collection WHERE id = ?", (collection_id,)).rowcount
    conn.commit()
    conn.close()

    if deleted and row:
        log(f"Removed collection entry {collection_id}: {row['name']} ({row['set_code']})")
        flash(f"Removed {row['name']} ({row['set_code']}) from your collection.")
    else:
        flash("That collection entry could not be found.")
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
normalize_collection_quantities()
log("Migrating product match metadata into cards...")
log(f"Updated {migrate_product_matches_to_cards()} card rows with product match metadata.")
log("Importing matched product rows into collection...")
log(f"Imported {import_external_products_into_collection()} matched product rows into the collection.")
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
normalize_collection_quantities()
start_background_sync()
log("Database initialized. Startup refresh running in the background; starting the daily Scryfall sync scheduler.")
schedule_daily_scryfall_sync()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log(f"Launching Flask dev server on port {port}...")
    app.run(debug=True, host="0.0.0.0", port=port)

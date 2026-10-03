import re
import sqlite3
from difflib import SequenceMatcher
from pathlib import Path

ROOT = Path(__file__).resolve().parent
APP_DB = ROOT / "data" / "mtg_tracker.db"
PRODUCT_DB = ROOT / "tcg_collection.db"


def normalize_name(value):
    if value is None:
        return ""
    text = str(value).strip().lower().replace("’", "'")
    text = re.sub(r"\s*\([^)]*\)", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def variants_for_name(value):
    base = str(value or "").strip()
    variants = {normalize_name(base)}
    cleaned = re.sub(r"\s*\([^)]*\)$", "", base).strip()
    if cleaned:
        variants.add(normalize_name(cleaned))
    for suffix in ["foil", "etched", "anime", "borderless", "extended art", "showcase", "promo", "alt art"]:
        v = re.sub(rf"\s*\({suffix}\)\s*$", "", base, flags=re.IGNORECASE).strip()
        if v != base:
            variants.add(normalize_name(v))
    return {v for v in variants if v}


def set_variants(value):
    if value is None:
        return {""}
    text = str(value).strip().lower().replace("’", "'")
    cleaned = re.sub(r"[^a-z0-9]+", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return {cleaned}


def ensure_columns(conn, table_name, columns):
    existing = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_sql in columns.items():
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def load_card_index():
    card_db = sqlite3.connect(APP_DB)
    card_db.row_factory = sqlite3.Row
    rows = card_db.execute(
        "SELECT id, name, set_name, set_code, foil, price_usd FROM cards"
    ).fetchall()
    card_db.close()

    index = {}
    for row in rows:
        name = row["name"]
        for variant in variants_for_name(name):
            index.setdefault(variant, []).append(row)
    return index


def choose_best_match(product_name, product_set_name, card_candidates):
    if not card_candidates:
        return None, None

    best = None
    for row in card_candidates:
        name_score = 0
        set_score = 0
        normalized_product = normalize_name(product_name)
        normalized_card = normalize_name(row["name"])
        if normalized_product == normalized_card:
            name_score = 100
        elif normalized_product == normalize_name(re.sub(r"\s*\([^)]*\)$", "", row["name"])):
            name_score = 92
        else:
            name_score = round(SequenceMatcher(None, normalized_product, normalized_card).ratio() * 100)

        product_set = set_variants(product_set_name)
        card_set_values = set_variants(row["set_name"]) | set_variants(row["set_code"])
        if product_set & card_set_values:
            set_score = 30
        elif product_set_name and row["set_name"] and normalize_name(product_set_name) == normalize_name(row["set_name"]):
            set_score = 25

        score = name_score + set_score
        if best is None or score > best[0]:
            best = (score, row)

    if best is None:
        return None, None
    return best[0], best[1]


def sync_product_matches():
    product_conn = sqlite3.connect(PRODUCT_DB)
    product_conn.row_factory = sqlite3.Row
    ensure_columns(
        product_conn,
        "products",
        {
            "matched_card_id": "INTEGER",
            "matched_card_name": "TEXT",
            "matched_set_code": "TEXT",
            "matched_set_name": "TEXT",
            "matched_foil": "INTEGER",
            "scryfall_price_usd": "REAL",
            "match_score": "INTEGER",
            "match_method": "TEXT",
            "is_matched": "INTEGER DEFAULT 0",
        },
    )

    card_index = load_card_index()
    products = product_conn.execute(
        "SELECT id, name, set_name, current_price, matched_card_id, matched_card_name, matched_set_code FROM products"
    ).fetchall()

    matched_count = 0
    for row in products:
        candidates = []
        for variant in variants_for_name(row["name"]):
            candidates.extend(card_index.get(variant, []))

        if not candidates:
            product_conn.execute(
                "UPDATE products SET matched_card_id = NULL, matched_card_name = NULL, matched_set_code = NULL, matched_set_name = NULL, matched_foil = NULL, scryfall_price_usd = NULL, match_score = NULL, match_method = NULL, is_matched = 0 WHERE id = ?",
                (row["id"],),
            )
            continue

        score, best_row = choose_best_match(row["name"], row["set_name"], candidates)
        if not best_row:
            product_conn.execute(
                "UPDATE products SET matched_card_id = NULL, matched_card_name = NULL, matched_set_code = NULL, matched_set_name = NULL, matched_foil = NULL, scryfall_price_usd = NULL, match_score = NULL, match_method = NULL, is_matched = 0 WHERE id = ?",
                (row["id"],),
            )
            continue

        match_method = "exact_name"
        if score < 100:
            match_method = "normalized_name"
        if row["set_name"] and best_row["set_name"] and normalize_name(row["set_name"]) == normalize_name(best_row["set_name"]):
            match_method = "set_and_name"

        product_conn.execute(
            """
            UPDATE products
            SET matched_card_id = ?,
                matched_card_name = ?,
                matched_set_code = ?,
                matched_set_name = ?,
                matched_foil = ?,
                scryfall_price_usd = ?,
                match_score = ?,
                match_method = ?,
                is_matched = 1,
                current_price = COALESCE(?, current_price)
            WHERE id = ?
            """,
            (
                best_row["id"],
                best_row["name"],
                best_row["set_code"],
                best_row["set_name"],
                1 if best_row["foil"] else 0,
                best_row["price_usd"],
                score,
                match_method,
                best_row["price_usd"],
                row["id"],
            ),
        )
        matched_count += 1

    product_conn.commit()
    product_conn.close()
    return matched_count


if __name__ == "__main__":
    count = sync_product_matches()
    print(f"Matched {count} product rows to Scryfall cards.")

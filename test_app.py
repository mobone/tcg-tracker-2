import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta

import app


class DailySyncTests(unittest.TestCase):
    def test_needs_daily_sync_when_last_sync_is_old(self):
        last_sync = datetime.utcnow() - timedelta(days=2)
        self.assertTrue(app.needs_daily_sync(last_sync))

    def test_needs_daily_sync_when_last_sync_is_recent(self):
        last_sync = datetime.utcnow() - timedelta(hours=2)
        self.assertFalse(app.needs_daily_sync(last_sync))


class AnalyticsTrendTests(unittest.TestCase):
    def setUp(self):
        self.original_db = app.app.config["DATABASE"]
        fd, self.temp_db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        app.app.config["DATABASE"] = self.temp_db
        app.init_db()

    def tearDown(self):
        app.app.config["DATABASE"] = self.original_db
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_analytics_tracks_price_change_and_rankings(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.execute(
            "INSERT INTO cards (id, name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "Winner Card", "M20", "Core Set 2020", "1", 0, 10.0, "https://example.com/winner.jpg", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.execute(
            "INSERT INTO cards (id, name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (2, "Loser Card", "M20", "Core Set 2020", "2", 0, 100.0, "https://example.com/loser.jpg", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.execute("INSERT INTO collection (card_id, quantity) VALUES (?, ?)", (1, 1))
        conn.execute("INSERT INTO collection (card_id, quantity) VALUES (?, ?)", (2, 1))
        conn.execute("INSERT INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)", (1, "2024-01-01", 0, 5.0))
        conn.execute("INSERT INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)", (1, "2024-01-10", 0, 15.0))
        conn.execute("INSERT INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)", (2, "2024-01-01", 0, 200.0))
        conn.execute("INSERT INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)", (2, "2024-01-10", 0, 50.0))
        conn.commit()
        conn.close()

        analytics = app.get_analytics_data()

        self.assertIn("collection_trend", analytics)
        self.assertIn("top_winners", analytics)
        self.assertIn("top_losers", analytics)
        self.assertGreater(analytics["top_winners"][0]["percent_change"], 0)
        self.assertLess(analytics["top_losers"][0]["percent_change"], 0)
        self.assertEqual(analytics["top_winners"][0]["name"], "Winner Card")
        self.assertEqual(analytics["top_losers"][0]["name"], "Loser Card")

    def test_analytics_range_filter_limits_history_window(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.execute(
            "INSERT INTO cards (id, name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "Range Card", "M20", "Core Set 2020", "10", 0, 10.0, "https://example.com/range.jpg", "2026-09-20 00:00:00"),
        )
        conn.execute("INSERT INTO collection (card_id, quantity) VALUES (?, ?)", (1, 1))
        conn.execute("INSERT INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)", (1, "2026-09-01", 0, 5.0))
        conn.execute("INSERT INTO price_history (card_id, recorded_at, foil, price_usd) VALUES (?, ?, ?, ?)", (1, "2026-09-20", 0, 25.0))
        conn.commit()
        conn.close()

        analytics = app.get_analytics_data(period="1m")

        self.assertEqual(len(analytics["top_winners"]), 1)
        self.assertEqual(analytics["top_winners"][0]["name"], "Range Card")
        self.assertGreater(analytics["top_winners"][0]["percent_change"], 0)


class SearchResultModalTests(unittest.TestCase):
    def setUp(self):
        self.original_db = app.app.config["DATABASE"]
        fd, self.temp_db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        app.app.config["DATABASE"] = self.temp_db
        app.init_db()

    def tearDown(self):
        app.app.config["DATABASE"] = self.original_db
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_search_matches_flavor_name_plus_base_name(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.execute(
            "INSERT INTO cards (name, flavor_name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("Yawgmoth, Thran Physician", "The Emperor, Hell Tyrant", "FCA", "FINAL FANTASY: Through the Ages", "11", 0, 25.0, "https://example.com/empire.jpg", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        conn.close()

        client = app.app.test_client()
        response = client.get("/search?q=The+Emperor%2C+Hell+Tyrant+-+Yawgmoth%2C+Thran+Physician")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Yawgmoth, Thran Physician', response.data)
        self.assertIn(b'The Emperor, Hell Tyrant', response.data)

    def test_search_results_render_clickable_names_with_modal(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.execute(
            "INSERT INTO cards (name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("Lotus Petal", "LEA", "Limited Edition Alpha", "1", 0, 999.0, "https://example.com/lotus-petal.jpg", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        conn.close()

        client = app.app.test_client()
        response = client.get("/search?q=Lotus+Petal")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'card-hover-name', response.data)
        self.assertIn(b'data-bs-toggle="modal"', response.data)
        self.assertIn(b'Lotus Petal', response.data)
        self.assertIn(b'Limited Edition Alpha', response.data)
        self.assertIn(b'https://example.com/lotus-petal.jpg', response.data)

    def test_index_page_omits_inline_search_results(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.execute(
            "INSERT INTO cards (name, set_code, set_name, collector_number, foil, price_usd, image_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("Lotus Petal", "LEA", "Limited Edition Alpha", "1", 0, 999.0, "https://example.com/lotus-petal.jpg", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.execute(
            "INSERT INTO collection (card_id, quantity, name, set_code, set_name, collector_number, foil, image_url, price_usd, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 1, "Lotus Petal", "LEA", "Limited Edition Alpha", "1", 0, "https://example.com/lotus-petal.jpg", 999.0, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        conn.close()

        client = app.app.test_client()
        response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b'Search results', response.data)
        self.assertIn(b'card-hover-image', response.data)
        self.assertIn(b'https://example.com/lotus-petal.jpg', response.data)


class EtchedFinishTests(unittest.TestCase):
    def setUp(self):
        self.original_db = app.app.config["DATABASE"]
        fd, self.temp_db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        app.app.config["DATABASE"] = self.temp_db
        app.init_db()

    def tearDown(self):
        app.app.config["DATABASE"] = self.original_db
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_etched_only_card_is_imported_and_shown_as_etched(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.executemany(
            """
            INSERT INTO cards (name, set_code, set_name, collector_number, foil, price_usd)
            VALUES (?, 'cmm', 'Commander Masters', '611', ?, NULL)
            """,
            [("Jeweled Lotus", 0), ("Jeweled Lotus", 1)],
        )
        conn.commit()
        conn.close()

        app.upsert_card_record(
            {
                "id": "etched-card-id",
                "name": "Jeweled Lotus",
                "set": "cmm",
                "set_name": "Commander Masters",
                "collector_number": "611",
                "finishes": ["etched"],
                "prices": {
                    "usd": None,
                    "usd_foil": None,
                    "usd_etched": "140.48",
                },
            }
        )

        results = app.get_search_results("Jeweled Lotus")

        self.assertEqual(len(results), 1)
        self.assertEqual(list(results[0]["variants"]), ["etched"])
        etched = results[0]["variants"]["etched"]
        self.assertEqual(etched["price_usd"], 140.48)
        self.assertEqual(etched["finish"], "etched")

        response = app.app.test_client().get("/search?q=Jeweled+Lotus")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Add Etched Foil $140.48", response.data)
        self.assertIn(b"Etched price: $140.48", response.data)
        self.assertNotIn(b"Add foil", response.data)
        self.assertNotIn(b"Add nonfoil", response.data)

    def test_card_without_any_market_price_is_hidden_from_search(self):
        app.upsert_card_record(
            {
                "id": "unpriced-card-id",
                "name": "Unpriced Card",
                "set": "cmm",
                "set_name": "Commander Masters",
                "collector_number": "999",
                "finishes": ["nonfoil", "foil", "etched"],
                "prices": {
                    "usd": None,
                    "usd_foil": None,
                    "usd_etched": None,
                },
            }
        )

        self.assertEqual(app.get_search_results("Unpriced Card"), [])

        response = app.app.test_client().get("/search?q=Unpriced+Card")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"No cards matched that search.", response.data)
        self.assertNotIn(b"cardModal-0", response.data)
        self.assertNotIn(b"search-card-tile", response.data)
        self.assertNotIn(b"$0.00", response.data)

    def test_add_to_collection_honors_quantity_and_accumulates(self):
        conn = sqlite3.connect(app.app.config["DATABASE"])
        conn.execute(
            "INSERT INTO cards (id, name, set_code, set_name, collector_number, foil, price_usd) VALUES (1, 'Sol Ring', 'cmm', 'Commander Masters', '1', 0, 2.0)"
        )
        conn.commit()
        conn.close()

        client = app.app.test_client()
        response = client.get("/search?q=Sol+Ring")
        self.assertIn(b'id="add-quantity"', response.data)

        client.post("/add-to-collection", data={"card_id": 1, "quantity": 8})
        client.post("/add-to-collection", data={"card_id": 1, "quantity": 0})
        client.post("/add-to-collection", data={"card_id": 1, "quantity": -5})

        conn = sqlite3.connect(app.app.config["DATABASE"])
        quantity = conn.execute("SELECT quantity FROM collection WHERE card_id = 1").fetchone()[0]
        conn.close()
        self.assertEqual(quantity, 10)


if __name__ == "__main__":
    unittest.main()

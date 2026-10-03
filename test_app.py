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

    def test_search_results_render_clickable_modal(self):
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


if __name__ == "__main__":
    unittest.main()

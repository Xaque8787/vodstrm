import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app.database import _SCHEMA
from app.tasks import strm


class StrmDownloadReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(_SCHEMA)

    def tearDown(self):
        self.conn.close()

    def _insert_completed(self, title, local_path, offline_path=None):
        self.conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title, year)
            VALUES ('movie-1', 'movie', ?, ?, 2026)
            """,
            (title, f"{title} (2026)"),
        )
        self.conn.execute(
            """
            INSERT INTO downloads (
                entry_id, status, container, local_path, offline_path,
                queued_at, completed_at, updated_at
            ) VALUES (
                'movie-1', 'completed', 'mp4', ?, ?, 'now', 'now', 'now'
            )
            """,
            (local_path, offline_path),
        )
        self.conn.commit()

    def test_reconcile_adopts_legacy_vod_file_into_offline_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            vod_root = os.path.join(directory, "vod")
            offline_root = os.path.join(directory, "offline")
            vod_path = os.path.join(
                vod_root, "movies", "Movie (2026)", "Movie (2026).mp4"
            )
            offline_path = os.path.join(
                offline_root, "movies", "Movie (2026)", "Movie (2026).mp4"
            )
            os.makedirs(os.path.dirname(vod_path), exist_ok=True)
            with open(vod_path, "wb") as media:
                media.write(b"legacy-media")
            self._insert_completed("Movie", vod_path)

            with patch("app.tasks.strm._offline_root", return_value=offline_root):
                reconciled = strm._reconcile_download_paths(self.conn, vod_root)

            self.assertEqual(reconciled, 1)
            self.assertTrue(os.path.samefile(offline_path, vod_path))
            self.assertEqual(os.stat(offline_path).st_nlink, 2)
            row = self.conn.execute(
                "SELECT local_path, offline_path FROM downloads WHERE entry_id='movie-1'"
            ).fetchone()
            self.assertEqual(row["local_path"], vod_path)
            self.assertEqual(row["offline_path"], offline_path)

    def test_reconcile_renames_offline_file_and_vod_link_together(self):
        with tempfile.TemporaryDirectory() as directory:
            vod_root = os.path.join(directory, "vod")
            offline_root = os.path.join(directory, "offline")
            old_vod = os.path.join(vod_root, "movies", "Old", "Old.mp4")
            old_offline = os.path.join(
                offline_root, "movies", "Old", "Old.mp4"
            )
            new_vod = os.path.join(
                vod_root, "movies", "Movie (2026)", "Movie (2026).mp4"
            )
            new_offline = os.path.join(
                offline_root, "movies", "Movie (2026)", "Movie (2026).mp4"
            )
            os.makedirs(os.path.dirname(old_offline), exist_ok=True)
            os.makedirs(os.path.dirname(old_vod), exist_ok=True)
            with open(old_offline, "wb") as media:
                media.write(b"offline-media")
            os.link(old_offline, old_vod)
            self._insert_completed("Movie", old_vod, old_offline)

            with patch("app.tasks.strm._offline_root", return_value=offline_root):
                reconciled = strm._reconcile_download_paths(self.conn, vod_root)

            self.assertEqual(reconciled, 1)
            self.assertFalse(os.path.exists(old_offline))
            self.assertFalse(os.path.exists(old_vod))
            self.assertTrue(os.path.samefile(new_offline, new_vod))
            self.assertEqual(os.stat(new_offline).st_nlink, 2)


if __name__ == "__main__":
    unittest.main()
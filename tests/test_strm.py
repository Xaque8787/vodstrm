import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app.database import _SCHEMA
from app.tasks import strm


class StrmDownloadCleanupTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(_SCHEMA)

    def tearDown(self):
        self.conn.close()

    def test_cleanup_never_deletes_untracked_offline_files(self):
        with tempfile.TemporaryDirectory() as directory:
            unrelated = os.path.join(
                directory, "personal", "only-copy-of-file.mkv"
            )
            os.makedirs(os.path.dirname(unrelated), exist_ok=True)
            payload = b"must-not-be-deleted"
            with open(unrelated, "wb") as media:
                media.write(payload)

            deleted = strm._cleanup_downloads(self.conn)

            self.assertEqual(deleted, 0)
            with open(unrelated, "rb") as media:
                self.assertEqual(media.read(), payload)

if __name__ == "__main__":
    unittest.main()
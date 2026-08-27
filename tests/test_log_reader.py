import os
import tempfile
import unittest
from unittest.mock import patch

from app.utils import log_reader


class LogReaderTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.log_dir = self.directory.name
        self.current = os.path.join(self.log_dir, "app.log")
        with open(self.current, "w", encoding="utf-8") as log:
            log.write(
                "2026-08-25 10:00:00 | INFO     | app.main | Started\n"
                "2026-08-25 10:00:01 | WARNING  | app.sync | Slow provider\n"
                "2026-08-25 10:00:02 | ERROR    | app.tasks | Failed\n"
                "Traceback line one\n"
                "Traceback line two\n"
                "2026-08-25 10:00:03 | DEBUG    | app.sql | SELECT 1\n"
            )
        with open(os.path.join(self.log_dir, "app.log.1"), "w") as log:
            log.write("2026-08-24 10:00:00 | INFO     | app.main | Older\n")
        with open(os.path.join(self.log_dir, "other.log"), "w") as log:
            log.write("must not be exposed")
        os.symlink(
            os.path.join(self.log_dir, "other.log"),
            os.path.join(self.log_dir, "app.log.2"),
        )
        self.patcher = patch("app.utils.log_reader._LOG_DIR", self.log_dir)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.directory.cleanup()

    def test_available_files_are_allowlisted_and_ordered(self):
        files = log_reader.available_log_files()

        self.assertEqual([value["name"] for value in files], ["app.log", "app.log.1"])

    def test_entries_are_newest_first_and_keep_tracebacks(self):
        entries = log_reader.read_log_entries(limit=50)

        self.assertEqual(entries[0]["level"], "DEBUG")
        error = next(value for value in entries if value["level"] == "ERROR")
        self.assertIn("Traceback line one\nTraceback line two", error["message"])

    def test_level_and_search_filters_apply_server_side(self):
        warnings = log_reader.read_log_entries(level="warning", search="provider")

        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["logger"], "app.sync")

    def test_arbitrary_paths_and_levels_are_rejected(self):
        with self.assertRaises(ValueError):
            log_reader.read_log_entries("../app.log")
        with self.assertRaises(ValueError):
            log_reader.read_log_entries("app.log.2")
        with self.assertRaises(ValueError):
            log_reader.read_log_entries(level="TRACE")


if __name__ == "__main__":
    unittest.main()
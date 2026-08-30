import asyncio
import json
import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from starlette.requests import Request

from app.database import _SCHEMA
from app.models import TokenData
from app.routes.admin import (
    cleanup_logs,
    clear_entries,
    clear_streams,
    legacy_library_page,
    logs_data,
)
from app.routes.library import list_entries


class AdminLibraryClearTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(_SCHEMA)
        self.user = TokenData(username="admin", user_id=1, is_admin=True)
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test')
            """
        )
        self.conn.execute(
            """
            INSERT INTO entries (
                entry_id, type, cleaned_title, raw_title, season, episode
            ) VALUES ('episode-1', 'series', 'Example Show',
                      'Example Show S01E01', 1, 1)
            """
        )
        self.conn.execute(
            """
            INSERT INTO streams (entry_id, stream_url, provider, batch_id)
            VALUES ('episode-1', 'https://example.test/episode', 'provider', 'batch')
            """
        )
        self.conn.execute(
            """
            INSERT INTO xtream_series_catalog (
                provider_slug, series_id, series_name, title_key,
                metadata_json, updated_at
            ) VALUES ('provider', '1', 'Example Show', 'example show', '{}', 'now')
            """
        )
        self.conn.execute(
            """
            INSERT INTO xtream_series_cache (
                provider_slug, series_id, series_name, episodes_json,
                fetch_status, fetched_at
            ) VALUES ('provider', '1', 'Example Show', '[]', 'ok', 'now')
            """
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    @contextmanager
    def _db(self):
        yield self.conn

    def _count(self, table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def _series_library(self):
        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(
                list_entries(
                    page=1,
                    per_page=50,
                    type="series",
                    search="",
                    owned="",
                    downloaded="",
                    current_user=self.user,
                )
            )
        return response, json.loads(response.body)

    def test_clear_streams_removes_native_tv_sources_but_keeps_entries(self):
        with patch("app.routes.admin.get_db", self._db):
            response = asyncio.run(clear_streams(current_user=self.user))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers["location"],
            "/admin/database?flash=streams_cleared&tab=streams",
        )
        self.assertEqual(self._count("streams"), 0)
        self.assertEqual(self._count("xtream_series_catalog"), 0)
        self.assertEqual(self._count("xtream_series_cache"), 0)
        self.assertEqual(self._count("entries"), 1)
        response, library = self._series_library()
        self.assertEqual(library["total"], 0)
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_clear_all_removes_entries_and_native_tv_sources(self):
        with patch("app.routes.admin.get_db", self._db):
            response = asyncio.run(clear_entries(current_user=self.user))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers["location"],
            "/admin/database?flash=cleared&tab=entries",
        )
        self.assertEqual(self._count("streams"), 0)
        self.assertEqual(self._count("entries"), 0)
        self.assertEqual(self._count("xtream_series_catalog"), 0)
        self.assertEqual(self._count("xtream_series_cache"), 0)

    def test_legacy_library_url_redirects_to_database_with_query(self):
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "scheme": "http",
                "server": ("test", 80),
                "path": "/admin/library",
                "raw_path": b"/admin/library",
                "query_string": b"tab=streams&page=2",
                "headers": [],
                "client": ("test", 1234),
            }
        )

        response = asyncio.run(
            legacy_library_page(request, current_user=self.user)
        )

        self.assertEqual(response.status_code, 308)
        self.assertEqual(
            response.headers["location"],
            "/admin/database?tab=streams&page=2",
        )

    def test_logs_data_is_filtered_and_not_cached(self):
        entries = [
            {
                "timestamp": "2026-08-25 10:00:00",
                "level": "ERROR",
                "logger": "app.tasks",
                "message": "Failed",
            }
        ]
        files = [
            {
                "name": "app.log",
                "size": 100,
                "modified_at": "2026-08-25T10:00:00",
                "rotation": 0,
            }
        ]
        with (
            patch("app.routes.admin.read_log_entries", return_value=entries) as reader,
            patch("app.routes.admin.available_log_files", return_value=files),
        ):
            response = asyncio.run(
                logs_data(
                    file="app.log",
                    level="ERROR",
                    search="failed",
                    limit=100,
                    current_user=self.user,
                )
            )

        data = json.loads(response.body)
        reader.assert_called_once_with("app.log", "ERROR", "failed", 100)
        self.assertEqual(data["entries"], entries)
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_logs_data_rejects_unknown_file(self):
        with patch(
            "app.routes.admin.read_log_entries",
            side_effect=ValueError("Unknown log file"),
        ):
            response = asyncio.run(
                logs_data(
                    file="../app.log",
                    level="ALL",
                    search="",
                    limit=250,
                    current_user=self.user,
                )
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.body)["error"], "Unknown log file")

    def test_cleanup_logs_returns_updated_file_list(self):
        result = {"name": "app.log.1", "bytes_removed": 500, "action": "deleted"}
        files = [{"name": "app.log", "size": 20, "modified_at": "now", "rotation": 0}]
        with (
            patch("app.routes.admin.cleanup_log_file", return_value=result) as cleanup,
            patch("app.routes.admin.available_log_files", return_value=files),
        ):
            response = asyncio.run(
                cleanup_logs(file="app.log.1", current_user=self.user)
            )

        data = json.loads(response.body)
        cleanup.assert_called_once_with("app.log.1")
        self.assertTrue(data["ok"])
        self.assertEqual(data["files"], files)

    def test_cleanup_logs_rejects_unknown_file(self):
        with patch(
            "app.routes.admin.cleanup_log_file",
            side_effect=ValueError("Unknown log file"),
        ):
            response = asyncio.run(
                cleanup_logs(file="../app.log", current_user=self.user)
            )

        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
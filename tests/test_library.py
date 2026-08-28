import asyncio
import json
import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from fastapi import BackgroundTasks

from app.database import _SCHEMA
from app.models import TokenData
from app.routes.library import (
    download_entry,
    download_series_episode,
    entry_details,
    delete_download,
    library_counts,
    list_entries,
    list_episodes,
    list_seasons,
)


class LibraryBrowseTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(_SCHEMA)
        self.user = TokenData(username="tester", user_id=1, is_admin=True)

    def tearDown(self):
        self.conn.close()

    @contextmanager
    def _db(self):
        yield self.conn

    def _list(self, **overrides):
        arguments = {
            "page": 1,
            "per_page": 48,
            "type": "",
            "search": "",
            "owned": "",
            "downloaded": "",
            "current_user": self.user,
        }
        arguments.update(overrides)
        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(list_entries(**arguments))
        return json.loads(response.body)

    def test_generate_all_content_is_browseable_but_not_importable(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode)
            VALUES ('Provider', 'provider', 'm3u', 'https://example.test/list.m3u', 'generate_all')
            """
        )
        self.conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title)
            VALUES ('entry-1', 'movie', 'Example Movie', 'Example Movie')
            """
        )
        self.conn.execute(
            """
            INSERT INTO streams (
                entry_id, stream_url, provider, batch_id,
                filtered_title, strm_path, last_written_url
            ) VALUES (
                'entry-1', 'https://example.test/movie', 'provider', 'batch-1',
                'Example Movie', '/tmp/example.strm', 'https://example.test/movie'
            )
            """
        )

        data = self._list()

        self.assertEqual(data["total"], 1)
        self.assertEqual(len(data["entries"]), 1)
        self.assertTrue(data["entries"][0]["is_owned"])
        self.assertFalse(data["entries"][0]["can_add"])

    def test_inactive_provider_content_is_not_browseable(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode, is_active)
            VALUES ('Provider', 'provider', 'm3u', 'https://example.test/list.m3u', 'generate_all', 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title)
            VALUES ('entry-1', 'movie', 'Example Movie', 'Example Movie')
            """
        )
        self.conn.execute(
            """
            INSERT INTO streams (entry_id, stream_url, provider, batch_id)
            VALUES ('entry-1', 'https://example.test/movie', 'provider', 'batch-1')
            """
        )

        data = self._list()

        self.assertEqual(data["total"], 0)
        self.assertEqual(data["entries"], [])

    def test_entry_details_returns_effective_strm_url(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode, priority)
            VALUES ('Provider', 'provider', 'm3u', 'https://example.test/list.m3u',
                    'generate_all', 1)
            """
        )
        self.conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title, year)
            VALUES ('movie-1', 'movie', 'Example Movie', 'Example Movie (2026)', 2026)
            """
        )
        self.conn.execute(
            """
            INSERT INTO streams (
                entry_id, stream_url, provider, batch_id, strm_path,
                last_written_url
            ) VALUES (
                'movie-1', 'https://example.test/original', 'provider', 'batch',
                '/tmp/movie.strm', 'https://example.test/written'
            )
            """
        )

        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(
                entry_details("movie-1", current_user=self.user)
            )
        data = json.loads(response.body)

        self.assertEqual(data["title"], "Example Movie")
        self.assertEqual(data["stream_url"], "https://example.test/written")
        self.assertEqual(data["provider"], "Provider")
        self.assertEqual(data["provider_slug"], "provider")
        self.assertTrue(data["strm_generated"])
        self.assertEqual(response.headers["cache-control"], "no-store")

    @patch("app.tasks.downloads.cancel_download", return_value=True)
    def test_delete_download_schedules_strm_regeneration(self, cancel_download):
        background_tasks = BackgroundTasks()

        response = asyncio.run(
            delete_download(
                "movie-1",
                background_tasks=background_tasks,
                current_user=self.user,
            )
        )

        self.assertEqual(response.status_code, 200)
        cancel_download.assert_called_once_with("movie-1", delete_file=True)
        self.assertEqual(len(background_tasks.tasks), 1)

    @patch("app.tasks.downloads.cancel_download", return_value=False)
    def test_delete_download_reports_missing_row(self, cancel_download):
        background_tasks = BackgroundTasks()

        response = asyncio.run(
            delete_download(
                "missing",
                background_tasks=background_tasks,
                current_user=self.user,
            )
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(len(background_tasks.tasks), 0)

    def test_series_episode_list_returns_episode_name(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode, priority)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test',
                    'generate_all', 1)
            """
        )
        self.conn.execute(
            """
            INSERT INTO entries (
                entry_id, type, cleaned_title, raw_title, season, episode,
                series_type
            ) VALUES (
                'show-s1e1', 'series', 'Example Show',
                'Example Show S01E01', 1, 1, 'season_episode'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO streams (
                entry_id, stream_url, provider, batch_id, filtered_title,
                metadata_json
            ) VALUES (
                'show-s1e1', 'https://example.test/episode', 'provider',
                'batch', 'Example Show', '{"episode-title":"Pilot"}'
            )
            """
        )

        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(
                list_episodes("Example Show", 1, current_user=self.user)
            )
        data = json.loads(response.body)

        self.assertEqual(data["episodes"][0]["episode_name"], "Pilot")
        self.assertEqual(data["episodes"][0]["display_title"], "Pilot")

    def test_series_episode_list_collapses_duplicate_provider_titles(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode, priority)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test',
                    'generate_all', 1)
            """
        )
        for episode, stream_id in ((1, "first"), (2, "duplicate"), (3, "second")):
            entry_id = f"show-s1e{episode}"
            self.conn.execute(
                """
                INSERT INTO entries (
                    entry_id, type, cleaned_title, raw_title, season, episode,
                    series_type
                ) VALUES (?, 'series', 'Example Show', ?, 1, ?, 'season_episode')
                """,
                (entry_id, f"Example Show S01E{episode:02d}", episode),
            )
            title = "Pilot" if episode < 3 else "Second"
            self.conn.execute(
                """
                INSERT INTO streams (
                    entry_id, stream_url, provider, batch_id, metadata_json
                ) VALUES (?, ?, 'provider', 'batch', ?)
                """,
                (
                    entry_id,
                    f"https://example.test/{stream_id}",
                    json.dumps({"episode-title": title}),
                ),
            )

        self.conn.execute(
            "UPDATE streams SET strm_path='/tmp/pilot.strm' "
            "WHERE entry_id IN ('show-s1e1', 'show-s1e2')"
        )

        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(
                list_episodes("Example Show", 1, current_user=self.user)
            )
        data = json.loads(response.body)

        self.assertEqual(len(data["episodes"]), 2)
        self.assertEqual(
            [episode["episode"] for episode in data["episodes"]], [1, 2]
        )
        self.assertEqual(
            [episode["episode_name"] for episode in data["episodes"]],
            ["Pilot", "Second"],
        )

        with (
            patch("app.routes.library.get_db", self._db),
            patch(
                "app.ingestion.xtream_native.ensure_series_loaded",
                return_value=3,
            ),
        ):
            response = asyncio.run(
                list_seasons("Example Show", current_user=self.user)
            )
        seasons = json.loads(response.body)["seasons"]
        self.assertEqual(seasons[0]["episode_count"], 2)
        self.assertEqual(seasons[0]["owned_count"], 1)

    @patch("app.tasks.downloads.queue_download", return_value=True)
    def test_series_episode_download_resolves_semantic_identity(self, queue_download):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, priority)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test', 1)
            """
        )
        for episode in (2, 3):
            entry_id = f"show-s1e{episode}"
            self.conn.execute(
                """
                INSERT INTO entries (
                    entry_id, type, cleaned_title, raw_title, season, episode
                ) VALUES (?, 'series', 'Example Show', ?, 1, ?)
                """,
                (entry_id, f"Example Show S01E{episode:02d}", episode),
            )
            self.conn.execute(
                """
                INSERT INTO streams (entry_id, stream_url, provider, batch_id)
                VALUES (?, ?, 'provider', 'batch')
                """,
                (entry_id, f"https://example.test/{episode}"),
            )
        self.conn.commit()

        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(
                download_series_episode(
                    "Example Show", 1, 3, current_user=self.user
                )
            )

        self.assertEqual(response.status_code, 200)
        queue_download.assert_called_once_with("show-s1e3")

    @patch("app.tasks.downloads.queue_download", return_value=True)
    def test_generic_download_supports_legacy_series_entry_id(self, queue_download):
        self.conn.execute(
            """
            INSERT INTO entries (
                entry_id, type, cleaned_title, raw_title, season, episode
            ) VALUES ('show-s1e3', 'series', 'Example Show',
                      'Example Show S01E03', 1, 3)
            """
        )
        self.conn.commit()

        with patch("app.routes.library.get_db", self._db):
            response = asyncio.run(
                download_entry("show-s1e3", current_user=self.user)
            )

        self.assertEqual(response.status_code, 200)
        queue_download.assert_called_once_with("show-s1e3")

    def test_series_groups_are_paginated_before_card_decoration(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode)
            VALUES ('Provider', 'provider', 'm3u', 'https://example.test/list.m3u', 'generate_all')
            """
        )
        for index, title in enumerate(("Alpha", "Beta", "Gamma"), start=1):
            entry_id = f"entry-{index}"
            self.conn.execute(
                """
                INSERT INTO entries (
                    entry_id, type, cleaned_title, raw_title, season, episode,
                    series_type
                ) VALUES (?, 'series', ?, ?, 1, 1, 'season_episode')
                """,
                (entry_id, title, f"{title} S01E01"),
            )
            self.conn.execute(
                """
                INSERT INTO streams (
                    entry_id, stream_url, provider, batch_id,
                    filtered_title, strm_path, last_written_url
                ) VALUES (?, ?, 'provider', 'batch-1', ?, ?, ?)
                """,
                (
                    entry_id,
                    f"https://example.test/{index}",
                    title,
                    f"/tmp/{title}.strm",
                    f"https://example.test/{index}",
                ),
            )

        data = self._list(page=2, per_page=1, type="series")

        self.assertEqual(data["total"], 3)
        self.assertEqual(len(data["entries"]), 1)
        self.assertEqual(data["entries"][0]["cleaned_title"], "Beta")

    def test_xtream_series_summary_is_visible_before_episodes_load(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test', 'generate_all')
            """
        )
        self.conn.execute(
            """
            INSERT INTO xtream_series_catalog (
                provider_slug, series_id, series_name, title_key, cover,
                category_id, metadata_json, updated_at
            ) VALUES (
                'provider', 'series-1', 'Special Ops: Lioness',
                'special ops: lioness',
                'https://images.test/lioness.jpg', 'shows', '{}',
                '2026-08-25T00:00:00+00:00'
            )
            """
        )

        data = self._list(type="series", search="Lioness")

        self.assertEqual(data["total"], 1)
        self.assertEqual(len(data["entries"]), 1)
        card = data["entries"][0]
        self.assertEqual(card["cleaned_title"], "Special Ops: Lioness")
        self.assertEqual(card["season_count"], 0)
        self.assertEqual(card["episode_count"], 0)
        self.assertFalse(card["episodes_loaded"])
        self.assertFalse(card["is_owned"])
        self.assertFalse(card["can_add"])

        with patch("app.routes.library.get_db", self._db):
            counts_response = asyncio.run(library_counts(current_user=self.user))
        counts = json.loads(counts_response.body)
        self.assertEqual(counts["series"], 1)
        self.assertEqual(counts["all"], 1)

    def test_opening_summary_only_series_loads_episode_details(self):
        self.conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, strm_mode)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test', 'generate_all')
            """
        )
        self.conn.execute(
            """
            INSERT INTO xtream_series_catalog (
                provider_slug, series_id, series_name, title_key,
                metadata_json, updated_at
            ) VALUES ('provider', 'series-1', 'Special Ops: Lioness',
                      'special ops: lioness', '{"releaseDate":"2023-07-23"}',
                      '2026-08-25T00:00:00+00:00')
            """
        )

        def fake_loader(title):
            self.assertEqual(title, "Special Ops: Lioness")
            self.conn.execute(
                """
                INSERT INTO entries (
                    entry_id, type, cleaned_title, raw_title, season, episode,
                    series_type
                ) VALUES (
                    'lioness-s1e1', 'series', 'Special Ops: Lioness',
                    'Special Ops: Lioness S01E01', 1, 1, 'season_episode'
                )
                """
            )
            self.conn.execute(
                """
                INSERT INTO streams (
                    entry_id, stream_url, provider, batch_id, metadata_json,
                    filtered_title, strm_path
                ) VALUES (
                    'lioness-s1e1', 'https://example.test/episode', 'provider',
                    'batch', '{}', 'Special Ops: Lioness', '/tmp/lioness.strm'
                )
                """
            )
            return 1

        with (
            patch("app.routes.library.get_db", self._db),
            patch(
                "app.ingestion.xtream_native.ensure_series_loaded",
                side_effect=fake_loader,
            ) as loader,
        ):
            response = asyncio.run(
                list_seasons("Special Ops: Lioness", current_user=self.user)
            )

        data = json.loads(response.body)
        loader.assert_called_once_with("Special Ops: Lioness")
        self.assertEqual(len(data["seasons"]), 1)
        self.assertEqual(data["year"], 2023)
        self.assertEqual(data["provider"], "Provider")
        self.assertEqual(data["seasons"][0]["season"], 1)
        self.assertEqual(data["seasons"][0]["episode_count"], 1)


if __name__ == "__main__":
    unittest.main()

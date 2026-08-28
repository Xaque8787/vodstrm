import json
import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

from app.database import _SCHEMA
from app.ingestion import xtream_native
from app.ingestion.sync import run_sync


def create_cache_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE xtream_series_cache (
            provider_slug TEXT NOT NULL,
            series_id TEXT NOT NULL,
            series_name TEXT NOT NULL,
            last_modified TEXT,
            episodes_json TEXT NOT NULL DEFAULT '[]',
            fetch_status TEXT NOT NULL DEFAULT 'ok',
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (provider_slug, series_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE xtream_series_catalog (
            provider_slug TEXT NOT NULL,
            series_id TEXT NOT NULL,
            series_name TEXT NOT NULL,
            title_key TEXT NOT NULL,
            cover TEXT,
            category_id TEXT,
            last_modified TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider_slug, series_id)
        )
        """
    )
    return conn


class FakeClient:
    base = "http://example.test:8080"
    username = "user/name"
    password = "pass word?"

    def __init__(self, series=None, details=None):
        self.series = series or []
        self.details = details or {}
        self.detail_calls = []
        self.authenticated = False

    def authenticate(self):
        self.authenticated = True

    def get_list(self, action):
        values = {
            "get_live_streams": [
                {
                    "stream_id": 10,
                    "name": "Example Channel",
                    "category_id": "1",
                    "stream_icon": "https://images.test/live.png",
                    "epg_channel_id": "channel-1",
                }
            ],
            "get_vod_streams": [
                {
                    "stream_id": 20,
                    "name": "Example Movie (2024)",
                    "category_id": "2",
                    "container_extension": "mkv",
                    "stream_icon": "https://images.test/movie.png",
                }
            ],
            "get_series": self.series,
            "get_live_categories": [
                {"category_id": "1", "category_name": "News"}
            ],
            "get_vod_categories": [
                {"category_id": "2", "category_name": "Movies"}
            ],
            "get_series_categories": [
                {"category_id": "3", "category_name": "Shows"}
            ],
        }
        return values[action]

    def get_json(self, action=None, **extra):
        self.detail_calls.append(str(extra.get("series_id")))
        value = self.details[str(extra.get("series_id"))]
        if isinstance(value, Exception):
            raise value
        return value


def series_payload(*episode_numbers):
    return {
        "episodes": {
            "1": [
                {
                    "id": f"episode-{number}",
                    "season": 1,
                    "episode_num": number,
                    "title": f"Episode {number}",
                    "container_extension": "mkv",
                    "info": {"movie_image": "https://images.test/episode.png"},
                }
                for number in episode_numbers
            ]
        }
    }


class XtreamNativeTests(unittest.TestCase):
    def setUp(self):
        self.conn = create_cache_db()
        self.provider = {
            "id": 1,
            "slug": "native-provider",
            "url": "http://example.test",
            "port": "8080",
            "username": "unused",
            "password": "unused",
            "stream_format": "ts",
        }

    def tearDown(self):
        self.conn.close()

    def test_playback_url_prefers_valid_direct_source(self):
        client = FakeClient()
        direct_source = "https://media.example.test/signed/stream.mkv"

        self.assertEqual(
            xtream_native._playback_url(
                client, "movie", 20, "mkv", direct_source
            ),
            direct_source,
        )
        constructed = xtream_native._playback_url(
            client, "movie", 20, "mkv", "ftp://invalid.example/stream"
        )
        self.assertIn("user%2Fname/pass%20word%3F", constructed)

    def test_episode_titles_correct_and_deduplicate_bad_provider_positions(self):
        payload = {
            "episodes": {
                "1": [
                    {
                        "id": "first-e1",
                        "season": 1,
                        "episode_num": 1,
                        "title": "Show - S01E01 - Pilot",
                    },
                    {
                        "id": "duplicate-e1",
                        "season": 1,
                        "episode_num": 2,
                        "title": "Show - S01E01 - Pilot",
                    },
                    {
                        "id": "first-e2",
                        "season": 1,
                        "episode_num": 3,
                        "title": "Show - S01E02 - Second",
                    },
                ]
            }
        }

        episodes = xtream_native._normalize_episode_payload(payload)

        self.assertEqual(
            [(item["episode"], item["id"]) for item in episodes],
            [(1, "first-e1"), (2, "first-e2")],
        )

    def test_playback_url_round_trips_special_credentials(self):
        client = FakeClient()
        allowed = "!$&'()*+,;=:@"
        client.username = " user" + allowed + "/?#%" + chr(0x00E9) + " "
        client.password = " pass" + allowed + "/?#%" + chr(0x00E9) + " "

        url = xtream_native._playback_url(client, "movie", "20/30", "mkv")
        path_parts = urlsplit(url).path.lstrip("/").split("/")

        self.assertEqual(path_parts[0], "movie")
        self.assertEqual(unquote(path_parts[1]), client.username)
        self.assertEqual(unquote(path_parts[2]), client.password)
        self.assertEqual(unquote(path_parts[3]), "20/30.mkv")
        for character in allowed:
            self.assertIn(character, path_parts[2])
        for encoded in ("%20", "%2F", "%3F", "%23", "%25", "%C3%A9"):
            self.assertIn(encoded, path_parts[2])

    def test_refresh_episode_stream_replaces_rotated_stream_id(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO providers (
                name, slug, type, url, username, password, port
            ) VALUES (
                'Provider', 'native-provider', 'xtream',
                'http://example.test', 'user', 'password', '8080'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO entries (
                entry_id, type, cleaned_title, raw_title, season, episode
            ) VALUES ('episode-1', 'series', 'Show', 'Show S01E01', 1, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO streams (
                entry_id, stream_url, provider, batch_id, metadata_json
            ) VALUES (
                'episode-1', 'http://example.test/old', 'native-provider',
                'batch', '{"xtream-series-id":"30","xtream-id":"old"}'
            )
            """
        )
        client = FakeClient(details={"30": series_payload(1)})

        with patch(
            "app.ingestion.xtream_native.XtreamClient", return_value=client
        ):
            stream_url = xtream_native.refresh_episode_stream(
                conn, "episode-1", "native-provider"
            )

        row = conn.execute(
            "SELECT stream_url, metadata_json FROM streams "
            "WHERE entry_id='episode-1' AND provider='native-provider'"
        ).fetchone()
        metadata = json.loads(row["metadata_json"])
        self.assertEqual(stream_url, row["stream_url"])
        self.assertTrue(stream_url.endswith("/episode-1.mkv"))
        self.assertEqual(metadata["xtream-id"], "episode-1")
        self.assertEqual(metadata["container-extension"], "mkv")
        conn.close()

    @patch("app.ingestion.xtream_native.requests.get")
    def test_player_api_passes_special_credentials_as_query_params(self, mock_get):
        username = " user&+% "
        password = " pass?#/ "
        response = mock_get.return_value
        response.status_code = 200
        response.json.return_value = {"user_info": {"auth": 1}}
        client = xtream_native.XtreamClient(
            {
                "url": "https://example.test",
                "username": username,
                "password": password,
            }
        )

        client.authenticate()

        params = mock_get.call_args.kwargs["params"]
        self.assertEqual(params["username"], username)
        self.assertEqual(params["password"], password)

    def test_sync_stores_summaries_and_reuses_on_demand_cache(self):
        series = [
            {
                "series_id": 30,
                "name": "Example Show",
                "category_id": "3",
                "cover": "https://images.test/show.png",
                "last_modified": "100",
            }
        ]
        client = FakeClient(series, {"30": series_payload(1, 2)})
        progress_events = []

        first = xtream_native.build_parsed_result(
            self.provider,
            self.conn,
            client=client,
            progress_callback=lambda **state: progress_events.append(state),
        )

        self.assertTrue(client.authenticated)
        phases = {event["phase"] for event in progress_events}
        self.assertTrue(
            {"authenticating", "catalog", "normalizing"}.issubset(phases)
        )
        self.assertNotIn("series", phases)
        self.assertEqual(len(first["live_tv"]), 1)
        self.assertEqual(len(first["movies"]), 1)
        self.assertEqual(first["series"], [])
        self.assertEqual(client.detail_calls, [])
        catalog = self.conn.execute(
            "SELECT series_name, cover FROM xtream_series_catalog"
        ).fetchall()
        self.assertEqual(len(catalog), 1)
        self.assertEqual(catalog[0]["series_name"], "Example Show")
        self.assertEqual(first["provider"], "native-provider")
        self.assertTrue(first["batch_id"])
        movie = first["movies"][0]
        self.assertEqual(movie["cleaned_title"], "Example Movie")
        self.assertEqual(movie["year"], 2024)
        self.assertEqual(first["summary"]["stats"]["series_pending"], 1)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM xtream_series_cache").fetchone()[0],
            0,
        )

        episodes = xtream_native._normalize_episode_payload(series_payload(1, 2))
        xtream_native._upsert_cache_success(
            self.conn,
            "native-provider",
            series[0],
            episodes,
            "2026-08-25T00:00:00+00:00",
        )
        self.conn.commit()

        second = xtream_native.build_parsed_result(
            self.provider, self.conn, client=client
        )
        self.assertEqual(client.detail_calls, [])
        self.assertEqual(len(second["series"]), 2)
        episode = second["series"][0]
        self.assertEqual((episode["season"], episode["episode"]), (1, 1))
        self.assertIn("user%2Fname/pass%20word%3F", episode["stream_url"])
        json.loads(episode["metadata_json"])

    def test_sync_never_fetches_missing_series_details(self):
        series = [
            {"series_id": 1, "name": "Broken", "last_modified": "1"},
            {"series_id": 2, "name": "Working", "last_modified": "1"},
        ]
        client = FakeClient(
            series,
            {
                "1": xtream_native.XtreamAPIError("failed"),
                "2": series_payload(1),
            },
        )

        first = xtream_native.build_parsed_result(
            self.provider, self.conn, client=client
        )
        self.assertEqual(first["series"], [])
        self.assertEqual(client.detail_calls, [])
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM xtream_series_catalog"
            ).fetchone()[0],
            2,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM xtream_series_cache").fetchone()[0],
            0,
        )

    def test_on_demand_loader_fetches_only_opened_show_once(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO providers (
                name, slug, type, url, username, password, port, stream_format
            ) VALUES (
                'Provider', 'native-provider', 'xtream', 'http://example.test',
                'user', 'password', '8080', 'ts'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO xtream_series_catalog (
                provider_slug, series_id, series_name, title_key,
                last_modified, metadata_json, updated_at
            ) VALUES (
                'native-provider', '30', 'Example Show', 'example show',
                '1', '{}', '2026-08-25T00:00:00+00:00'
            )
            """
        )
        conn.commit()
        client = FakeClient(details={"30": series_payload(1, 2)})

        @contextmanager
        def use_test_db():
            yield conn

        with (
            patch("app.ingestion.xtream_native.get_db", use_test_db),
            patch("app.ingestion.xtream_native.XtreamClient", return_value=client),
            patch("app.tasks.strm.generate_strm") as generate_strm,
        ):
            loaded = xtream_native.ensure_series_loaded("Example Show")
            loaded_again = xtream_native.ensure_series_loaded("Example Show")

        self.assertEqual(loaded, 2)
        self.assertEqual(loaded_again, 2)
        self.assertEqual(client.detail_calls, ["30"])
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM entries WHERE type='series'"
            ).fetchone()[0],
            2,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM xtream_series_cache").fetchone()[0],
            1,
        )
        generate_strm.assert_called_once_with()
        conn.close()

    def test_sync_does_not_refresh_changed_on_demand_cache(self):
        cached_series = [
            {"series_id": 5, "name": "Cached Show", "last_modified": "1"}
        ]
        episodes = xtream_native._normalize_episode_payload(series_payload(1))
        xtream_native._upsert_cache_success(
            self.conn,
            "native-provider",
            cached_series[0],
            episodes,
            "2026-08-25T00:00:00+00:00",
        )
        self.conn.commit()

        changed_series = [
            {"series_id": 5, "name": "Cached Show", "last_modified": "2"}
        ]
        failing_client = FakeClient(
            changed_series, {"5": xtream_native.XtreamAPIError("temporary")}
        )
        second = xtream_native.build_parsed_result(
            self.provider, self.conn, client=failing_client
        )

        self.assertEqual(len(second["series"]), 1)
        self.assertEqual(failing_client.detail_calls, [])
        cache_row = self.conn.execute(
            "SELECT fetch_status, last_modified, episodes_json "
            "FROM xtream_series_cache WHERE series_id = '5'"
        ).fetchone()
        self.assertEqual(cache_row["fetch_status"], "ok")
        self.assertEqual(cache_row["last_modified"], "1")
        self.assertEqual(len(json.loads(cache_row["episodes_json"])), 1)

    def test_native_result_runs_through_production_sync_schema(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO providers (
                name, slug, type, url, username, password, port, stream_format
            ) VALUES (?, ?, 'xtream', ?, ?, ?, ?, 'ts')
            """,
            (
                "Native Provider",
                "native-provider",
                "http://example.test",
                "user/name",
                "pass word?",
                "8080",
            ),
        )
        series = [
            {"series_id": 30, "name": "Example Show", "last_modified": "1"}
        ]
        client = FakeClient(series, {"30": series_payload(1, 2)})

        parsed = xtream_native.build_parsed_result(
            self.provider, conn, client=client
        )
        summary = run_sync(conn, parsed)

        self.assertEqual(summary["provider"], "native-provider")
        self.assertEqual(client.detail_calls, [])
        self.assertEqual(summary["inserted_streams"], 2)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM streams").fetchone()[0], 2
        )
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM streams WHERE source_file = 'xtream-player-api'"
            ).fetchone()[0],
            2,
        )
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM entries WHERE type = 'series'"
            ).fetchone()[0],
            0,
        )
        conn.close()


if __name__ == "__main__":
    unittest.main()
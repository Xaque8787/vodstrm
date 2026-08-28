import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import requests

from app.database import _SCHEMA
from app.tasks import downloads


class FakeResponse:
    def __init__(
        self,
        chunks,
        content_type="video/mp4",
        content_length=None,
        status_code=200,
        content_range=None,
    ):
        self.chunks = chunks
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        if content_length is not None:
            self.headers["content-length"] = str(content_length)
        if content_range is not None:
            self.headers["content-range"] = content_range

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        self.chunk_size = chunk_size
        for chunk in self.chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk


class DownloadsTests(unittest.TestCase):
    def test_all_vod_content_folders_are_configurable(self):
        custom = SimpleNamespace(
            vod_movies_folder="Films",
            vod_series_folder="Shows",
            vod_live_tv_folder="Channels",
            vod_unsorted_folder="Other",
            vod_unknown_year_folder="NoYear",
        )
        cases = (
            ("movie", "Film", 2026, None, None, None, "Films"),
            ("series", "Show", None, 1, 2, None, "Shows"),
            ("tv_vod", "Daily", None, None, None, "2026-08-25", "Shows"),
            ("live", "Channel", None, None, None, None, "Channels"),
            ("unsorted", "Clip", None, None, None, None, "Other"),
        )

        with patch("app.tasks.strm.settings", custom):
            for entry_type, title, year, season, episode, air_date, folder in cases:
                path = downloads._derive_media_path(
                    entry_type,
                    title,
                    year,
                    season,
                    episode,
                    "mkv",
                    "/vod",
                    air_date=air_date,
                )
                self.assertEqual(os.path.relpath(path, "/vod").split(os.sep)[0], folder)

            series_path = downloads._derive_media_path(
                "series", "Show", None, 1, 2, "mkv", "/vod"
            )
            tv_vod_path = downloads._derive_media_path(
                "tv_vod", "Daily", None, None, None, "mkv", "/vod"
            )
            self.assertIn(os.path.join("Shows", "Show", "Season 01"), series_path)
            self.assertIn(os.path.join("Shows", "Daily", "NoYear"), tv_vod_path)

    def test_remove_failed_download_deletes_row(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title)
            VALUES ('movie-1', 'movie', 'Movie', 'Movie')
            """
        )
        conn.execute(
            """
            INSERT INTO downloads (entry_id, status, fail_reason, queued_at, updated_at)
            VALUES ('movie-1', 'failed', 'ffmpeg_failed', 'now', 'now')
            """
        )
        conn.commit()

        @contextmanager
        def use_test_db():
            yield conn

        with patch("app.tasks.downloads.get_db", side_effect=use_test_db):
            self.assertTrue(downloads.cancel_download("movie-1", delete_file=True))

        self.assertIsNone(
            conn.execute(
                "SELECT status FROM downloads WHERE entry_id='movie-1'"
            ).fetchone()
        )
        conn.close()

    def test_cancel_pending_download_keeps_cancelled_row(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title)
            VALUES ('episode-1', 'series', 'Show', 'Show S01E01')
            """
        )
        conn.execute(
            """
            INSERT INTO downloads (
                entry_id, status, staging_path, offline_path, queued_at, updated_at
            ) VALUES ('episode-1', 'pending', '/tmp/part', '/tmp/final', 'now', 'now')
            """
        )
        conn.commit()

        @contextmanager
        def use_test_db():
            yield conn

        with (
            patch("app.tasks.downloads.get_db", side_effect=use_test_db),
            patch("app.tasks.downloads._remove_download_file"),
        ):
            self.assertTrue(
                downloads.cancel_download("episode-1", delete_file=True)
            )

        row = conn.execute(
            "SELECT status, fail_reason, staging_path, offline_path "
            "FROM downloads WHERE entry_id='episode-1'"
        ).fetchone()
        self.assertEqual(row["status"], "cancelled")
        self.assertEqual(row["fail_reason"], "cancelled_by_user")
        self.assertIsNone(row["staging_path"])
        self.assertIsNone(row["offline_path"])
        conn.close()

    def test_reset_stuck_downloads_preserves_live_progress(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        for entry_id in ("live", "stale"):
            conn.execute(
                "INSERT INTO entries (entry_id, type, cleaned_title, raw_title) "
                "VALUES (?, 'movie', ?, ?)",
                (entry_id, entry_id, entry_id),
            )
        conn.execute(
            "INSERT INTO downloads (entry_id, status, queued_at, updated_at) "
            "VALUES ('live', 'downloading', datetime('now'), datetime('now'))"
        )
        conn.execute(
            "INSERT INTO downloads (entry_id, status, queued_at, updated_at) "
            "VALUES ('stale', 'downloading', datetime('now', '-10 minutes'), "
            "datetime('now', '-10 minutes'))"
        )
        conn.commit()

        @contextmanager
        def use_test_db():
            yield conn

        with patch("app.tasks.downloads.get_db", side_effect=use_test_db):
            self.assertEqual(downloads.reset_stuck_downloads(), 1)

        statuses = dict(
            conn.execute("SELECT entry_id, status FROM downloads").fetchall()
        )
        self.assertEqual(statuses["live"], "downloading")
        self.assertEqual(statuses["stale"], "pending")
        conn.close()

    def test_startup_reclaims_active_rows_and_restarts_queue(self):
        with (
            patch("app.tasks.downloads.reset_stuck_downloads") as reset,
            patch("app.tasks.downloads._kick_processor") as kick,
        ):
            downloads.resume_downloads_on_startup()

        reset.assert_called_once_with(max_age_seconds=None)
        kick.assert_called_once_with()

    def test_processor_picks_up_next_item_after_first_is_deleted(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        for entry_id in ("first", "second"):
            conn.execute(
                "INSERT INTO entries (entry_id, type, cleaned_title, raw_title) "
                "VALUES (?, 'movie', ?, ?)",
                (entry_id, entry_id, entry_id),
            )
            conn.execute(
                "INSERT INTO downloads (entry_id, status, queued_at, updated_at) "
                "VALUES (?, 'pending', ?, ?)",
                (entry_id, entry_id, entry_id),
            )
        conn.execute(
            "INSERT INTO integrations (slug, settings) "
            "VALUES ('downloads', '{\"enabled\":true,\"max_concurrent\":1}')"
        )
        conn.commit()
        processed = []

        @contextmanager
        def use_test_db():
            yield conn

        def process_one(test_conn, row, _settings, cancel_event=None):
            processed.append(row["entry_id"])
            if row["entry_id"] == "first":
                test_conn.execute(
                    "DELETE FROM downloads WHERE entry_id='first'"
                )
            else:
                test_conn.execute(
                    "UPDATE downloads SET status='completed' "
                    "WHERE entry_id='second'"
                )
            test_conn.commit()
            return row["entry_id"] == "second"

        downloads._active_count = 0
        with (
            patch("app.tasks.downloads.get_db", side_effect=use_test_db),
            patch("app.tasks.downloads.reset_stuck_downloads"),
            patch("app.tasks.downloads._offline_root", return_value="/tmp"),
            patch("app.tasks.downloads.os.makedirs"),
            patch("app.tasks.downloads._process_one", side_effect=process_one),
            patch("app.tasks.strm.generate_strm"),
        ):
            downloads.process_downloads()

        self.assertEqual(processed, ["first", "second"])
        self.assertEqual(
            conn.execute(
                "SELECT status FROM downloads WHERE entry_id='second'"
            ).fetchone()[0],
            "completed",
        )
        self.assertEqual(downloads._active_count, 0)
        conn.close()

    def test_processor_restarts_for_work_queued_during_finalization(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        for entry_id in ("first", "late"):
            conn.execute(
                "INSERT INTO entries (entry_id, type, cleaned_title, raw_title) "
                "VALUES (?, 'movie', ?, ?)",
                (entry_id, entry_id, entry_id),
            )
        conn.execute(
            "INSERT INTO downloads (entry_id, status, queued_at, updated_at) "
            "VALUES ('first', 'pending', 'first', 'first')"
        )
        conn.commit()

        @contextmanager
        def use_test_db():
            yield conn

        def complete_first(test_conn, row, _settings, cancel_event=None):
            test_conn.execute(
                "UPDATE downloads SET status='completed' WHERE entry_id=?",
                (row["entry_id"],),
            )
            test_conn.commit()
            return True

        def queue_during_finalization():
            conn.execute(
                "INSERT INTO downloads (entry_id, status, queued_at, updated_at) "
                "VALUES ('late', 'pending', 'late', 'late')"
            )
            conn.commit()

        downloads._active_count = 0
        with (
            patch("app.tasks.downloads.get_db", side_effect=use_test_db),
            patch("app.tasks.downloads.reset_stuck_downloads"),
            patch("app.tasks.downloads._offline_root", return_value="/tmp"),
            patch("app.tasks.downloads.os.makedirs"),
            patch("app.tasks.downloads._process_one", side_effect=complete_first),
            patch(
                "app.tasks.strm.generate_strm",
                side_effect=queue_during_finalization,
            ),
            patch("app.tasks.downloads._kick_processor") as kick,
        ):
            downloads.process_downloads()

        kick.assert_called_once_with()
        self.assertEqual(downloads._active_count, 0)
        conn.close()

    def test_delete_completed_download_removes_exact_offline_path(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title)
            VALUES ('movie-1', 'movie', 'Movie', 'Movie')
            """
        )

        with tempfile.TemporaryDirectory() as directory:
            offline_root = os.path.join(directory, "offline")
            offline_path = os.path.join(offline_root, "movies", "Movie.mp4")
            os.makedirs(os.path.dirname(offline_path), exist_ok=True)
            with open(offline_path, "wb") as media:
                media.write(b"media")
            conn.execute(
                """
                INSERT INTO downloads (
                    entry_id, status, local_path, offline_path, queued_at, updated_at
                ) VALUES ('movie-1', 'completed', ?, ?, 'now', 'now')
                """,
                (offline_path, offline_path),
            )
            conn.commit()

            @contextmanager
            def use_test_db():
                yield conn

            with (
                patch("app.tasks.downloads.get_db", side_effect=use_test_db),
                patch("app.tasks.downloads._offline_root", return_value=offline_root),
            ):
                self.assertTrue(
                    downloads.cancel_download("movie-1", delete_file=True)
                )

            self.assertFalse(os.path.exists(offline_path))
            self.assertIsNone(
                conn.execute(
                    "SELECT status FROM downloads WHERE entry_id='movie-1'"
                ).fetchone()
            )
        conn.close()

    def test_cancel_removes_only_empty_item_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            category = os.path.join(directory, "tvshows")
            show = os.path.join(category, "Show")
            season = os.path.join(show, "Season 01")
            staging = os.path.join(season, "Show S01E02.mkv.part")
            sibling = os.path.join(season, "Show S01E01.mkv")
            os.makedirs(season)
            with open(staging, "wb") as media:
                media.write(b"partial")
            with open(sibling, "wb") as media:
                media.write(b"episode-one")

            with patch("app.tasks.downloads._offline_root", return_value=directory):
                downloads._remove_download_file(staging)

            self.assertFalse(os.path.exists(staging))
            self.assertTrue(os.path.isfile(sibling))
            self.assertTrue(os.path.isdir(season))
            self.assertTrue(os.path.isdir(show))
            self.assertTrue(os.path.isdir(category))

    def test_cancel_removes_empty_season_and_show_but_keeps_category(self):
        with tempfile.TemporaryDirectory() as directory:
            category = os.path.join(directory, "tvshows")
            season = os.path.join(category, "Show", "Season 01")
            staging = os.path.join(season, "Show S01E01.mkv.part")
            os.makedirs(season)
            with open(staging, "wb") as media:
                media.write(b"partial")

            with patch("app.tasks.downloads._offline_root", return_value=directory):
                downloads._remove_download_file(staging)

            self.assertFalse(os.path.exists(season))
            self.assertFalse(os.path.exists(os.path.dirname(season)))
            self.assertTrue(os.path.isdir(category))

    def test_progressive_mp4_source_preserves_source_container(self):
        probe = {"format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2"}}

        self.assertEqual(
            downloads._progressive_source_container(
                "https://example.test/movie/123.mp4", probe
            ),
            "mp4",
        )
        self.assertIsNone(
            downloads._progressive_source_container(
                "https://example.test/movie/playlist.m3u8", probe
            )
        )

    @patch("app.tasks.downloads.requests.get")
    @patch("app.tasks.downloads.subprocess.run")
    def test_ffprobe_recovers_mislabeled_mpegts(self, mock_run, mock_get):
        prefix = bytearray(564)
        for offset in (0, 188, 376):
            prefix[offset] = 0x47
        mock_get.return_value = FakeResponse(
            [bytes(prefix[:100]), bytes(prefix[100:300]), bytes(prefix[300:])]
        )
        mock_run.side_effect = [
            SimpleNamespace(returncode=1, stdout="", stderr="invalid EBML"),
            SimpleNamespace(
                returncode=0,
                stdout='{"format":{"format_name":"mpegts"},"streams":[]}',
                stderr="",
            ),
        ]

        probe = downloads._ffprobe_stream("https://example.test/movie.mkv")

        self.assertEqual(probe["format"]["format_name"], "mpegts")
        self.assertEqual(probe["_vodstrm_input_format"], "mpegts")
        retry_command = mock_run.call_args_list[1].args[0]
        self.assertEqual(retry_command[retry_command.index("-f") + 1], "mpegts")
        self.assertEqual(
            mock_get.call_args.kwargs["headers"]["Range"], "bytes=0-563"
        )

    def test_failed_series_probe_retries_refreshed_xtream_stream(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO providers (name, slug, type, url, username, password)
            VALUES ('Provider', 'provider', 'xtream', 'https://example.test', 'u', 'p')
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
            INSERT INTO streams (entry_id, stream_url, provider, batch_id)
            VALUES ('episode-1', 'https://example.test/old', 'provider', 'batch')
            """
        )
        conn.execute(
            """
            INSERT INTO downloads (entry_id, status, queued_at, updated_at)
            VALUES ('episode-1', 'pending', 'now', 'now')
            """
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM downloads WHERE entry_id='episode-1'"
        ).fetchone()
        probe = {"format": {"format_name": "mpegts", "size": "100"}}

        with (
            patch(
                "app.tasks.downloads._ffprobe_stream",
                side_effect=[None, probe],
            ) as ffprobe,
            patch(
                "app.ingestion.xtream_native.refresh_episode_stream",
                return_value="https://example.test/current",
            ),
            patch("app.tasks.downloads._derive_media_path", return_value="/tmp/out.mkv"),
            patch("app.tasks.downloads.os.makedirs"),
            patch("app.tasks.downloads._ffmpeg_download", return_value=False),
            patch("app.tasks.downloads._remove_download_file"),
        ):
            downloads._process_one(conn, row, downloads._DEFAULTS)

        self.assertEqual(
            [call.args[0] for call in ffprobe.call_args_list],
            ["https://example.test/old", "https://example.test/current"],
        )
        saved = conn.execute(
            "SELECT stream_url FROM downloads WHERE entry_id='episode-1'"
        ).fetchone()[0]
        self.assertEqual(saved, "https://example.test/current")
        conn.close()

    def test_series_path_uses_offline_root(self):
        offline_path = downloads._derive_media_path(
            "series", "Example Show", None, 1, 2, "mkv", "/offline"
        )

        expected = os.path.join(
            downloads.settings.vod_series_folder,
            "Example Show",
            "Season 01",
            "Example Show S01E02.mkv",
        )
        self.assertEqual(os.path.relpath(offline_path, "/offline"), expected)

    @patch("app.tasks.downloads.requests.get")
    def test_http_download_is_byte_for_byte(self, mock_get):
        payload = b"original-container\x00with-all-streams"
        progress = []
        mock_get.return_value = FakeResponse(
            [payload[:10], payload[10:]], content_length=len(payload)
        )
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "movie.mp4")

            result = downloads._http_download_exact(
                "https://example.test/movie.mp4",
                output,
                progress_callback=lambda received, total: progress.append(
                    (received, total)
                ),
            )

            self.assertTrue(result)
            with open(output, "rb") as source:
                self.assertEqual(source.read(), payload)
            self.assertEqual(progress[0], (0, len(payload)))
            self.assertEqual(progress[-1], (len(payload), len(payload)))

    @patch("app.tasks.downloads.time.sleep")
    @patch("app.tasks.downloads.requests.get")
    def test_http_download_rejects_truncated_file(self, mock_get, _sleep):
        mock_get.return_value = FakeResponse([b"short"], content_length=100)
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "movie.mp4")

            result = downloads._http_download_exact(
                "https://example.test/movie.mp4", output
            )

            self.assertFalse(result)

    @patch("app.tasks.downloads.time.sleep")
    @patch("app.tasks.downloads.requests.get")
    def test_http_download_resumes_after_interruption(self, mock_get, _sleep):
        payload = b"firstsecond"
        mock_get.side_effect = [
            FakeResponse(
                [b"first", requests.exceptions.ChunkedEncodingError()],
                content_length=len(payload),
            ),
            FakeResponse(
                [b"second"],
                content_length=6,
                status_code=206,
                content_range="bytes 5-10/11",
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "movie.mp4")

            result = downloads._http_download_exact(
                "https://example.test/movie.mp4", output
            )

            self.assertTrue(result)
            with open(output, "rb") as source:
                self.assertEqual(source.read(), payload)
            self.assertEqual(
                mock_get.call_args_list[0].kwargs["headers"]["Range"],
                "bytes=0-4194303",
            )
            self.assertEqual(
                mock_get.call_args_list[1].kwargs["headers"]["Range"],
                "bytes=5-10",
            )

    @patch("app.tasks.downloads.subprocess.Popen")
    def test_ffmpeg_fallback_maps_every_stream_without_transcoding(self, mock_popen):
        progress = []
        output = tempfile.NamedTemporaryFile(delete=False, suffix=".mkv")
        output.write(b"media")
        output.close()
        process = mock_popen.return_value
        process.poll.return_value = 0
        process.returncode = 0
        process.communicate.return_value = ("", "")
        try:
            self.assertTrue(
                downloads._ffmpeg_download(
                    "https://example.test/stream",
                    output.name,
                    progress_callback=lambda received, total: progress.append(
                        (received, total)
                    ),
                    expected_size=100,
                    input_format="mpegts",
                    output_container="mkv",
                )
            )
            command = mock_popen.call_args.args[0]
            input_format_index = command.index("-f")
            output_format_index = command.index("-f", input_format_index + 1)
            self.assertEqual(command[input_format_index + 1], "mpegts")
            self.assertLess(input_format_index, command.index("-i"))
            self.assertEqual(command[output_format_index + 1], "matroska")
            self.assertLess(output_format_index, len(command) - 1)
            self.assertIn("-copy_unknown", command)
            self.assertIn("-map_metadata", command)
            self.assertIn("-map_chapters", command)
            map_positions = [
                index for index, value in enumerate(command) if value == "-map"
            ]
            self.assertEqual(len(map_positions), 1)
            self.assertEqual(command[map_positions[0] + 1], "0")
            self.assertEqual(command[command.index("-c") + 1], "copy")
            self.assertNotIn("-vn", command)
            self.assertNotIn("-an", command)
            self.assertNotIn("-sn", command)
            self.assertNotIn("-dn", command)
            self.assertEqual(progress[0], (0, 100))
            self.assertEqual(progress[-1], (len(b"media"), 100))
        finally:
            os.remove(output.name)

    @patch("app.tasks.downloads.requests.get")
    def test_http_download_stops_when_cancelled(self, mock_get):
        cancel_event = threading.Event()
        cancel_event.set()
        mock_get.return_value = FakeResponse(
            [b"should-not-be-written"], content_length=21
        )
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "movie.mp4")

            result = downloads._http_download_exact(
                "https://example.test/movie.mp4",
                output,
                cancel_event=cancel_event,
            )

            self.assertFalse(result)
            self.assertEqual(os.path.getsize(output), 0)

    @patch("app.tasks.downloads.subprocess.Popen")
    def test_ffmpeg_download_terminates_when_cancelled(self, mock_popen):
        cancel_event = threading.Event()
        cancel_event.set()
        process = mock_popen.return_value
        process.poll.return_value = None
        process.communicate.return_value = ("", "")

        result = downloads._ffmpeg_download(
            "https://example.test/stream",
            "/tmp/cancelled-download.mkv",
            cancel_event=cancel_event,
        )

        self.assertFalse(result)
        process.terminate.assert_called_once_with()

    def test_processor_preserves_progressive_source_container(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO providers (name, slug, type, url)
            VALUES ('Provider', 'provider', 'm3u', 'https://example.test')
            """
        )
        conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title, year)
            VALUES ('movie-1', 'movie', 'Movie', 'Movie (2026)', 2026)
            """
        )
        conn.execute(
            """
            INSERT INTO streams (entry_id, stream_url, provider, batch_id)
            VALUES ('movie-1', 'https://example.test/movie.mp4', 'provider', 'batch')
            """
        )
        conn.execute(
            """
            INSERT INTO downloads (entry_id, status, queued_at, updated_at)
            VALUES ('movie-1', 'pending', 'now', 'now')
            """
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM downloads WHERE entry_id='movie-1'"
        ).fetchone()
        probe = {
            "format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2"},
            "streams": [
                {"codec_type": "video", "codec_name": "h264"},
                {"codec_type": "audio", "codec_name": "aac"},
                {"codec_type": "subtitle", "codec_name": "mov_text"},
                {"codec_type": "data", "codec_name": "bin_data"},
            ],
        }

        with tempfile.TemporaryDirectory() as directory:
            offline_root = os.path.join(directory, "offline")
            offline = os.path.join(
                offline_root, "movies", "Movie (2026)", "Movie (2026).mp4"
            )

            def exact_copy(
                _url, path, progress_callback=None, cancel_event=None
            ):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as output:
                    output.write(b"exact-source-bytes")
                if progress_callback:
                    progress_callback(
                        len(b"exact-source-bytes"), len(b"exact-source-bytes")
                    )
                return True

            with (
                patch("app.tasks.downloads._ffprobe_stream", return_value=probe),
                patch("app.tasks.downloads._http_download_exact", side_effect=exact_copy) as exact,
                patch("app.tasks.downloads._ffmpeg_download") as ffmpeg,
                patch("app.tasks.downloads._offline_root", return_value=offline_root),
            ):
                result = downloads._process_one(
                    conn,
                    row,
                    {"default_container": "mkv", "ffmpeg_timeout": 3600},
                )

            self.assertTrue(result)
            exact.assert_called_once()
            ffmpeg.assert_not_called()
            completed = conn.execute(
                "SELECT status, container, local_path, offline_path, file_size "
                "FROM downloads WHERE entry_id='movie-1'"
            ).fetchone()
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(completed["container"], "mp4")
            self.assertEqual(completed["local_path"], offline)
            self.assertEqual(completed["offline_path"], offline)
            self.assertEqual(completed["file_size"], len(b"exact-source-bytes"))
            self.assertTrue(os.path.isfile(offline))
        conn.close()

    def test_database_cancellation_cannot_be_overwritten_by_completion(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.execute(
            """
            INSERT INTO providers (name, slug, type, url)
            VALUES ('Provider', 'provider', 'm3u', 'https://example.test')
            """
        )
        conn.execute(
            """
            INSERT INTO entries (entry_id, type, cleaned_title, raw_title, season, episode)
            VALUES ('episode-1', 'series', 'Show', 'Show S01E01', 1, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO streams (entry_id, stream_url, provider, batch_id)
            VALUES ('episode-1', 'https://example.test/episode.mkv', 'provider', 'batch')
            """
        )
        conn.execute(
            """
            INSERT INTO downloads (entry_id, status, queued_at, updated_at)
            VALUES ('episode-1', 'pending', 'now', 'now')
            """
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM downloads WHERE entry_id='episode-1'"
        ).fetchone()
        probe = {"format": {"format_name": "matroska"}, "streams": []}

        with tempfile.TemporaryDirectory() as offline_root:
            final_path = os.path.join(
                offline_root,
                downloads.settings.vod_series_folder,
                "Show",
                "Season 01",
                "Show S01E01.mkv",
            )

            def cancel_during_download(_url, path, **_kwargs):
                with open(path, "wb") as media:
                    media.write(b"complete-payload")
                conn.execute(
                    "UPDATE downloads SET status='cancelled', cancelled_at='now' "
                    "WHERE entry_id='episode-1'"
                )
                conn.commit()
                return True

            with (
                patch("app.tasks.downloads._ffprobe_stream", return_value=probe),
                patch(
                    "app.tasks.downloads._progressive_source_container",
                    return_value="mkv",
                ),
                patch(
                    "app.tasks.downloads._http_download_exact",
                    side_effect=cancel_during_download,
                ),
                patch("app.tasks.downloads._offline_root", return_value=offline_root),
            ):
                result = downloads._process_one(
                    conn,
                    row,
                    {"default_container": "mkv", "ffmpeg_timeout": 3600},
                )

            current = conn.execute(
                "SELECT status FROM downloads WHERE entry_id='episode-1'"
            ).fetchone()
            self.assertFalse(result)
            self.assertEqual(current["status"], "cancelled")
            self.assertFalse(os.path.exists(final_path))
            self.assertTrue(
                os.path.isdir(os.path.join(offline_root, downloads.settings.vod_series_folder))
            )
            self.assertFalse(os.path.exists(os.path.dirname(final_path)))
        conn.close()


if __name__ == "__main__":
    unittest.main()
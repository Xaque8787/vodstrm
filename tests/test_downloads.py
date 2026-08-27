import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from app.database import _SCHEMA
from app.tasks import downloads


class FakeResponse:
    def __init__(self, chunks, content_type="video/mp4", content_length=None):
        self.chunks = chunks
        self.headers = {"content-type": content_type}
        if content_length is not None:
            self.headers["content-length"] = str(content_length)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        self.chunk_size = chunk_size
        yield from self.chunks


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

    def test_storage_roots_support_hardlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            vod_root = os.path.join(directory, "vod")
            offline_root = os.path.join(directory, "offline")
            with (
                patch("app.tasks.downloads._vod_root", return_value=vod_root),
                patch(
                    "app.tasks.downloads._offline_root",
                    return_value=offline_root,
                ),
            ):
                downloads.validate_storage_roots()

            self.assertEqual(os.listdir(vod_root), [])
            self.assertEqual(os.listdir(offline_root), [])

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

    def test_delete_completed_download_unlinks_vod_and_offline_paths(self):
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
            vod_root = os.path.join(directory, "vod")
            offline_root = os.path.join(directory, "offline")
            vod_path = os.path.join(vod_root, "movies", "Movie.mp4")
            offline_path = os.path.join(offline_root, "movies", "Movie.mp4")
            os.makedirs(os.path.dirname(vod_path), exist_ok=True)
            os.makedirs(os.path.dirname(offline_path), exist_ok=True)
            with open(offline_path, "wb") as media:
                media.write(b"media")
            os.link(offline_path, vod_path)
            conn.execute(
                """
                INSERT INTO downloads (
                    entry_id, status, local_path, offline_path, queued_at, updated_at
                ) VALUES ('movie-1', 'completed', ?, ?, 'now', 'now')
                """,
                (vod_path, offline_path),
            )
            conn.commit()

            @contextmanager
            def use_test_db():
                yield conn

            with (
                patch("app.tasks.downloads.get_db", side_effect=use_test_db),
                patch("app.tasks.downloads._vod_root", return_value=vod_root),
                patch("app.tasks.downloads._offline_root", return_value=offline_root),
            ):
                self.assertTrue(
                    downloads.cancel_download("movie-1", delete_file=True)
                )

            self.assertFalse(os.path.exists(vod_path))
            self.assertFalse(os.path.exists(offline_path))
            self.assertIsNone(
                conn.execute(
                    "SELECT status FROM downloads WHERE entry_id='movie-1'"
                ).fetchone()
            )
        conn.close()

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

    def test_series_paths_match_between_vod_and_offline_roots(self):
        vod_path = downloads._derive_media_path(
            "series", "Example Show", None, 1, 2, "mkv", "/vod"
        )
        offline_path = downloads._derive_media_path(
            "series", "Example Show", None, 1, 2, "mkv", "/offline"
        )

        expected = os.path.join(
            "series", "Example Show", "Season 01", "Example Show S01E02.mkv"
        )
        self.assertEqual(os.path.relpath(vod_path, "/vod"), expected)
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

    @patch("app.tasks.downloads.requests.get")
    def test_http_download_rejects_truncated_file(self, mock_get):
        mock_get.return_value = FakeResponse([b"short"], content_length=100)
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "movie.mp4")

            result = downloads._http_download_exact(
                "https://example.test/movie.mp4", output
            )

            self.assertFalse(result)

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
                )
            )
            command = mock_popen.call_args.args[0]
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
            vod_root = os.path.join(directory, "vod")
            offline = os.path.join(
                offline_root, "movies", "Movie (2026)", "Movie (2026).mp4"
            )
            final = os.path.join(
                vod_root, "movies", "Movie (2026)", "Movie (2026).mp4"
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
                patch("app.tasks.downloads._vod_root", return_value=vod_root),
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
            self.assertEqual(completed["local_path"], final)
            self.assertEqual(completed["offline_path"], offline)
            self.assertEqual(completed["file_size"], len(b"exact-source-bytes"))
            self.assertTrue(os.path.samefile(offline, final))
            self.assertEqual(os.stat(offline).st_nlink, 2)
        conn.close()


if __name__ == "__main__":
    unittest.main()
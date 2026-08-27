import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from app.models import ProviderXtreamCreate, ProviderXtreamUpdate
from app.tasks import downloader


class FakeResponse:
    def __init__(self, status_code, content=b"", headers=None, json_data=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self._json_data = json_data

    def json(self):
        if self._json_data is None:
            raise ValueError("No JSON response")
        return self._json_data


class DownloaderTests(unittest.TestCase):
    def tearDown(self):
        with downloader._running_providers_guard:
            downloader._running_providers.clear()

    def test_safe_origin_removes_credentials_and_resource_details(self):
        origin = downloader._safe_origin(
            "https://user:secret@example.com:8443/private/list.m3u?token=hidden#part"
        )

        self.assertEqual(origin, "https://example.com:8443")

    def test_xtream_playlist_url_round_trips_special_credentials(self):
        username = " user/&+#?%+" + chr(0x00E9) + " "
        password = " p@ss*/&+#?%+" + chr(0x00E9) + " "
        url = downloader._build_xtream_url(
            {
                "url": "https://example.test",
                "port": "8443",
                "username": username,
                "password": password,
                "stream_format": "hls",
            }
        )

        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        self.assertEqual(parsed.netloc, "example.test:8443")
        self.assertEqual(parsed.path, "/get.php")
        self.assertEqual(query["username"], [username])
        self.assertEqual(query["password"], [password])
        self.assertEqual(query["type"], ["m3u_plus"])
        self.assertEqual(query["output"], ["m3u8"])
        self.assertNotIn(username, url)
        self.assertNotIn(password, url)

    def test_xtream_models_preserve_credential_characters(self):
        values = {
            "name": "Provider",
            "server_scheme": "https://",
            "server_url": "example.test",
            "username": " user+name ",
            "password": " pass word&% ",
        }

        created = ProviderXtreamCreate(**values)
        updated = ProviderXtreamUpdate(**values)

        self.assertEqual(created.username, values["username"])
        self.assertEqual(created.password, values["password"])
        self.assertEqual(updated.username, values["username"])
        self.assertEqual(updated.password, values["password"])

    @patch("app.ingestion.xtream_native.ingest_native_provider")
    @patch("app.tasks.downloader.requests.get")
    def test_api_only_xtream_provider_uses_redacted_native_fallback(
        self, mock_get, mock_native
    ):
        mock_get.return_value = FakeResponse(404, content=b"not found")
        mock_native.return_value = {
            "parse": {
                "stats": {
                    "live": 2,
                    "movie": 3,
                    "series": 4,
                    "series_ready": 1,
                    "series_catalog": 2,
                    "series_pending": 1,
                }
            },
            "sync": {},
        }
        provider = {
            "id": 1,
            "slug": "example-provider",
            "type": "xtream",
            "url": "http://example.com",
            "username": "mock-user",
            "password": "mock-password",
            "port": "8080",
            "stream_format": "ts",
        }

        with self.assertLogs("app.tasks.downloader", level="INFO") as captured:
            result = downloader._download_provider(provider, "/unused")

        logs = "\n".join(captured.output)
        self.assertTrue(result)
        self.assertIn("trying native Player API ingestion", logs)
        self.assertIn("Native Xtream ingest complete", logs)
        self.assertNotIn("mock-user", logs)
        self.assertNotIn("mock-password", logs)
        self.assertNotIn("username=", logs)
        self.assertEqual(mock_get.call_count, 1)
        mock_native.assert_called_once()

    @patch("app.ingestion.xtream_native.ingest_native_provider")
    @patch("app.tasks.downloader.requests.get")
    def test_native_xtream_failure_returns_false(self, mock_get, mock_native):
        mock_get.return_value = FakeResponse(403, content=b"forbidden")
        mock_native.side_effect = RuntimeError("authentication failed")
        provider = {
            "id": 1,
            "slug": "example-provider",
            "type": "xtream",
            "url": "http://example.com",
            "username": "mock-user",
            "password": "mock-password",
            "port": "8080",
            "stream_format": "ts",
        }

        with self.assertLogs("app.tasks.downloader", level="INFO") as captured:
            result = downloader._download_provider(provider, "/unused")

        logs = "\n".join(captured.output)
        self.assertFalse(result)
        self.assertIn("Native Xtream ingest failed", logs)
        self.assertNotIn("mock-user", logs)
        self.assertNotIn("mock-password", logs)

    @patch("app.tasks.downloader.requests.get")
    def test_http_200_non_playlist_response_is_rejected(self, mock_get):
        mock_get.return_value = FakeResponse(
            200,
            content=b"<html><body>Login required</body></html>",
            headers={"content-type": "text/html"},
        )
        provider = {
            "id": 2,
            "slug": "html-provider",
            "type": "m3u",
            "url": "https://example.com/list?token=hidden",
            "username": None,
            "password": None,
            "port": None,
            "stream_format": "ts",
        }

        with self.assertLogs("app.tasks.downloader", level="INFO") as captured:
            result = downloader._download_provider(provider, "/unused")

        self.assertFalse(result)
        self.assertIn("response is not an M3U playlist", "\n".join(captured.output))

    @patch("app.tasks.downloader.requests.get")
    def test_duplicate_provider_trigger_is_skipped(self, mock_get):
        provider = {
            "id": 3,
            "slug": "busy-provider",
            "type": "m3u",
            "url": "https://example.com/list.m3u",
            "username": None,
            "password": None,
            "port": None,
            "stream_format": "ts",
        }
        self.assertTrue(downloader._claim_provider("busy-provider"))

        with self.assertLogs("app.tasks.downloader", level="WARNING") as captured:
            result = downloader._download_provider(provider, "/unused")

        self.assertIsNone(result)
        self.assertIn("already running", "\n".join(captured.output))
        mock_get.assert_not_called()

    @patch("app.tasks.downloader._download_provider_unlocked")
    def test_provider_lock_releases_after_unexpected_failure(self, mock_download):
        mock_download.side_effect = RuntimeError("unexpected")
        provider = {"id": 4, "slug": "failing-provider"}

        with self.assertRaises(RuntimeError):
            downloader._download_provider(provider, "/unused")

        self.assertFalse(downloader.is_provider_running("failing-provider"))


if __name__ == "__main__":
    unittest.main()

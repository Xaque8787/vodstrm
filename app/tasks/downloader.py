"""
M3U downloader tasks.

Handles downloading M3U playlist files from all active providers.
- M3U providers: download directly from the provider URL
- Xtream providers: construct the get.php URL using server, port, credentials,
  and stream format (ts → output=ts, hls → output=m3u8)
"""
import logging
import os
import sqlite3
import threading
from urllib.parse import quote, urlencode, urlsplit

import requests

from app.config import settings
from app.database import get_db
from app.ingestion.sync import purge_inactive_and_deleted_providers
from app.tasks.base import task
from app.tasks import progress as sync_progress
from app.utils.env import resolve_path

logger = logging.getLogger("app.tasks.downloader")

_M3U_DIR_RELATIVE = settings.m3u_dir

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

_running_providers: set[str] = set()
_running_providers_guard = threading.Lock()


def is_provider_running(provider_slug: str) -> bool:
    with _running_providers_guard:
        return provider_slug in _running_providers


def _claim_provider(provider_slug: str) -> bool:
    with _running_providers_guard:
        if provider_slug in _running_providers:
            return False
        _running_providers.add(provider_slug)
    sync_progress.start(provider_slug)
    return True


def _release_provider(provider_slug: str) -> None:
    with _running_providers_guard:
        _running_providers.discard(provider_slug)


def _m3u_dir() -> str:
    path = resolve_path(_M3U_DIR_RELATIVE)
    os.makedirs(path, exist_ok=True)
    return path


def _build_xtream_url(row: sqlite3.Row) -> str:
    server = row["url"] or ""
    server = server.rstrip("/")
    port = (row["port"] or "").strip()
    username = row["username"] or ""
    password = row["password"] or ""
    stream_format = (row["stream_format"] or "ts").lower()

    output_param = "m3u8" if stream_format == "hls" else "ts"

    if port:
        base = f"{server}:{port}"
    else:
        base = server

    query = urlencode(
        {
            "username": username,
            "password": password,
            "type": "m3u_plus",
            "output": output_param,
        },
        quote_via=quote,
        safe="",
    )
    return f"{base}/get.php?{query}"


def _safe_origin(url: str) -> str:
    """Return a log-safe origin with user info, paths, and query values removed."""
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname or "unknown-host"
        if ":" in hostname:
            hostname = f"[{hostname}]"
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme or 'unknown'}://{hostname}{port}"
    except ValueError:
        return "invalid-provider-url"


def _try_native_xtream(provider: sqlite3.Row, playlist_failure: str) -> bool:
    slug = provider["slug"] or str(provider["id"])
    if playlist_failure == "import_selected mode":
        logger.info(
            "[DOWNLOADER] Using Player API for import_selected xtream provider '%s'",
            slug,
        )
    else:
        logger.warning(
            "[DOWNLOADER] Xtream playlist unavailable for '%s' (%s); "
            "trying native Player API ingestion",
            slug,
            playlist_failure,
        )
    try:
        from app.ingestion.xtream_native import ingest_native_provider

        result = ingest_native_provider(
            dict(provider),
            progress_callback=lambda **state: sync_progress.update(slug, **state),
        )
        stats = result["parse"]["stats"]
        logger.info(
            "[DOWNLOADER] Native Xtream ingest complete — provider=%s "
            "live=%d movies=%d episodes=%d series_ready=%d/%d pending=%d",
            slug,
            stats.get("live", 0),
            stats.get("movie", 0),
            stats.get("series", 0),
            stats.get("series_ready", 0),
            stats.get("series_catalog", 0),
            stats.get("series_pending", 0),
        )
        return True
    except Exception as exc:
        logger.error(
            "[DOWNLOADER] Native Xtream ingest failed for '%s' (%s)",
            slug,
            type(exc).__name__,
        )
        return False


def _download_provider_unlocked(provider: sqlite3.Row, m3u_dir: str) -> bool:
    slug = provider["slug"] or str(provider["id"])
    provider_type = provider["type"]

    if provider_type == "local_file":
        sync_progress.update(
            slug,
            phase="catalog",
            message="Reading local M3U playlist",
        )
        logger.info("[DOWNLOADER] Local file provider '%s' — skipping download, triggering ingest directly", slug)
        try:
            from app.tasks.ingestion import ingest_provider_file
            ingest_provider_file(slug)
        except Exception as exc:
            logger.error("[DOWNLOADER] Ingestion failed for local provider '%s': %s", slug, exc, exc_info=True)
            return False
        return True  # generate_strm called by download_all_providers / download_provider after all providers

    if provider_type == "m3u":
        url = provider["url"] or ""
    elif provider_type == "xtream":
        strm_mode = provider["strm_mode"] if "strm_mode" in provider.keys() else "generate_all"
        if strm_mode == "import_selected":
            logger.info(
                "[DOWNLOADER] Xtream provider '%s' is import_selected — using Player API for catalog browsing",
                slug,
            )
            return _try_native_xtream(provider, "import_selected mode")
        url = _build_xtream_url(provider)
    else:
        logger.warning("[DOWNLOADER] Unknown provider type '%s' for '%s', skipping", provider_type, slug)
        return False

    if not url:
        logger.warning("[DOWNLOADER] Provider '%s' has no URL configured, skipping", slug)
        return False

    logger.info(
        "[DOWNLOADER] Downloading '%s' (%s) from %s",
        slug,
        provider_type,
        _safe_origin(url),
    )
    sync_progress.update(
        slug,
        phase="catalog",
        message="Fetching provider playlist",
    )

    try:
        response = requests.get(url, headers=_HEADERS, timeout=60)
    except requests.RequestException as exc:
        logger.error(
            "[DOWNLOADER] Request failed for provider '%s' (%s)",
            slug,
            type(exc).__name__,
        )
        if provider_type == "xtream":
            return _try_native_xtream(provider, type(exc).__name__)
        return False

    if response.status_code != 200:
        if provider_type == "xtream":
            return _try_native_xtream(provider, f"HTTP {response.status_code}")
        logger.error(
            "[DOWNLOADER] Provider '%s' returned HTTP %s",
            slug,
            response.status_code,
        )
        return False

    content = response.content
    playlist_head = content[:1024 * 1024].lstrip(b"\xef\xbb\xbf \t\r\n")
    if not playlist_head.startswith(b"#EXTM3U") and b"#EXTINF" not in playlist_head:
        if provider_type == "xtream":
            return _try_native_xtream(provider, "non-M3U response")
        logger.error(
            "[DOWNLOADER] Provider '%s' returned HTTP 200 but the response is not "
            "an M3U playlist (content-type=%s)",
            slug,
            response.headers.get("content-type", "unknown"),
        )
        return False

    filename = f"{slug}.m3u"
    file_path = os.path.join(m3u_dir, filename)

    with open(file_path, "wb") as f:
        f.write(content)

    logger.info(
        "[DOWNLOADER] Saved '%s' (%d bytes) → %s",
        slug,
        len(content),
        file_path,
    )

    # Trigger ingestion immediately after a successful download.
    try:
        from app.tasks.ingestion import ingest_provider_file
        ingest_provider_file(slug)
    except Exception as exc:
        logger.error(
            "[DOWNLOADER] Ingestion failed for provider '%s' after download: %s",
            slug, exc, exc_info=True,
        )

    return True  # generate_strm called by download_all_providers / download_provider after all providers


def _download_provider(provider: sqlite3.Row, m3u_dir: str) -> bool | None:
    slug = provider["slug"] or str(provider["id"])
    if not _claim_provider(slug):
        logger.warning(
            "[DOWNLOADER] Provider '%s' is already running; duplicate trigger skipped",
            slug,
        )
        return None
    try:
        result = _download_provider_unlocked(provider, m3u_dir)
        if result:
            sync_progress.finish(slug)
        else:
            sync_progress.fail(slug)
        return result
    except Exception:
        sync_progress.fail(slug)
        raise
    finally:
        _release_provider(slug)


def _purge() -> None:
    with get_db() as conn:
        purge_inactive_and_deleted_providers(conn)


@task("download_all_providers")
def download_all_providers() -> None:
    m3u_dir = _m3u_dir()

    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM providers WHERE is_active = 1"
        ).fetchall()

    eligible = [r for r in rows if not r["schedule_omitted"]]
    omitted  = [r for r in rows if r["schedule_omitted"]]

    if omitted:
        logger.info(
            "[DOWNLOADER] Skipping %d omitted provider(s): %s",
            len(omitted), [r["slug"] for r in omitted],
        )

    if not eligible:
        logger.info("[DOWNLOADER] No active non-omitted providers found, nothing to download")
        _purge()
        return

    logger.info("[DOWNLOADER] Starting download for %d active provider(s)", len(eligible))

    success = 0
    failed = 0
    skipped = 0
    claimed_slugs: list[str] = []

    try:
        for provider in eligible:
            slug = provider["slug"] or str(provider["id"])
            if not _claim_provider(slug):
                logger.warning(
                    "[DOWNLOADER] Provider '%s' is already running; duplicate trigger skipped",
                    slug,
                )
                skipped += 1
                continue
            claimed_slugs.append(slug)
            if _download_provider_unlocked(provider, m3u_dir):
                success += 1
            else:
                failed += 1
                sync_progress.fail(slug)

        logger.info(
            "[DOWNLOADER] Completed — %d succeeded, %d failed, %d already running",
            success,
            failed,
            skipped,
        )

        _purge()

        from app.tasks.strm import generate_strm
        for slug in claimed_slugs:
            if sync_progress.snapshot(slug).get("status") == "running":
                sync_progress.update(
                    slug,
                    phase="strm",
                    message="Generating STRM files",
                )
        try:
            generate_strm()
        except Exception as exc:
            logger.error("[DOWNLOADER] generate_strm failed after all downloads: %s", exc, exc_info=True)

        from app.tasks.live_m3u import generate_live_m3u
        for slug in claimed_slugs:
            if sync_progress.snapshot(slug).get("status") == "running":
                sync_progress.update(
                    slug,
                    phase="live_m3u",
                    message="Generating live TV playlists",
                )
        try:
            generate_live_m3u()
        except Exception as exc:
            logger.error("[DOWNLOADER] generate_live_m3u failed after all downloads: %s", exc, exc_info=True)

        from app.tasks.tmdb import trigger_tmdb_enrichment
        try:
            trigger_tmdb_enrichment(triggered_by="download:all")
        except Exception as exc:
            logger.error("[DOWNLOADER] TMDB trigger failed after all downloads: %s", exc, exc_info=True)

        if failed and not success:
            raise RuntimeError("All provider syncs failed")
        for slug in claimed_slugs:
            if sync_progress.snapshot(slug).get("status") == "running":
                sync_progress.finish(slug)
    except Exception:
        for slug in claimed_slugs:
            if sync_progress.snapshot(slug).get("status") == "running":
                sync_progress.fail(slug)
        raise
    finally:
        for slug in claimed_slugs:
            _release_provider(slug)


@task("download_provider")
def download_provider(provider_slug: str) -> None:
    m3u_dir = _m3u_dir()

    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM providers WHERE slug = ? AND is_active = 1",
            (provider_slug,),
        ).fetchone()

    if not row:
        logger.warning(
            "[DOWNLOADER] Provider '%s' not found or inactive, skipping", provider_slug
        )
        return

    if not _claim_provider(provider_slug):
        logger.warning(
            "[DOWNLOADER] Provider '%s' is already running; duplicate trigger skipped",
            provider_slug,
        )
        return

    try:
        if not _download_provider_unlocked(row, m3u_dir):
            sync_progress.fail(provider_slug)
            raise RuntimeError(f"Provider sync failed: {provider_slug}")

        from app.tasks.strm import generate_strm
        sync_progress.update(
            provider_slug,
            phase="strm",
            message="Generating STRM files",
        )
        try:
            generate_strm()
        except Exception as exc:
            logger.error("[DOWNLOADER] generate_strm failed after download of '%s': %s", provider_slug, exc, exc_info=True)

        from app.tasks.live_m3u import generate_live_m3u
        sync_progress.update(
            provider_slug,
            phase="live_m3u",
            message="Generating live TV playlists",
        )
        try:
            generate_live_m3u()
        except Exception as exc:
            logger.error("[DOWNLOADER] generate_live_m3u failed after download of '%s': %s", provider_slug, exc, exc_info=True)

        from app.tasks.tmdb import trigger_tmdb_enrichment
        try:
            trigger_tmdb_enrichment(triggered_by=f"download:{provider_slug}")
        except Exception as exc:
            logger.error("[DOWNLOADER] TMDB trigger failed after download of '%s': %s", provider_slug, exc, exc_info=True)
        sync_progress.finish(provider_slug)
    except Exception:
        sync_progress.fail(provider_slug)
        raise
    finally:
        _release_provider(provider_slug)

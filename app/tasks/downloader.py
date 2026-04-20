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

import requests

from app.database import get_db
from app.tasks.base import task
from app.utils.env import resolve_path

logger = logging.getLogger("app.tasks.downloader")

_M3U_DIR_RELATIVE = os.getenv("M3U_DIR", "data/m3u")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


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

    return (
        f"{base}/get.php"
        f"?username={username}"
        f"&password={password}"
        f"&type=m3u_plus"
        f"&output={output_param}"
    )


def _download_provider(provider: sqlite3.Row, m3u_dir: str) -> bool:
    slug = provider["slug"] or str(provider["id"])
    provider_type = provider["type"]

    if provider_type == "m3u":
        url = provider["url"] or ""
    elif provider_type == "xtream":
        url = _build_xtream_url(provider)
    else:
        logger.warning("[DOWNLOADER] Unknown provider type '%s' for '%s', skipping", provider_type, slug)
        return False

    if not url:
        logger.warning("[DOWNLOADER] Provider '%s' has no URL configured, skipping", slug)
        return False

    logger.info("[DOWNLOADER] Downloading '%s' from %s", slug, url)

    try:
        response = requests.get(url, headers=_HEADERS, timeout=60)
    except requests.RequestException as exc:
        logger.error("[DOWNLOADER] Request failed for provider '%s': %s", slug, exc)
        return False

    if response.status_code != 200:
        logger.error(
            "[DOWNLOADER] Provider '%s' returned HTTP %s", slug, response.status_code
        )
        return False

    filename = f"{slug}.m3u"
    file_path = os.path.join(m3u_dir, filename)

    with open(file_path, "wb") as f:
        f.write(response.content)

    logger.info(
        "[DOWNLOADER] Saved '%s' (%d bytes) → %s",
        slug,
        len(response.content),
        file_path,
    )

    # Trigger ingestion immediately after a successful download.
    # Import here to avoid a circular dependency at module load time.
    try:
        from app.tasks.ingestion import ingest_provider_file
        ingest_provider_file(slug)
    except Exception as exc:
        logger.error(
            "[DOWNLOADER] Ingestion failed for provider '%s' after download: %s",
            slug, exc, exc_info=True,
        )

    return True


@task("download_all_providers")
def download_all_providers() -> None:
    m3u_dir = _m3u_dir()

    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM providers WHERE is_active = 1"
        ).fetchall()

    if not rows:
        logger.info("[DOWNLOADER] No active providers found, nothing to download")
        return

    logger.info("[DOWNLOADER] Starting download for %d active provider(s)", len(rows))

    success = 0
    failed = 0

    for provider in rows:
        ok = _download_provider(provider, m3u_dir)
        if ok:
            success += 1
        else:
            failed += 1

    logger.info(
        "[DOWNLOADER] Completed — %d succeeded, %d failed", success, failed
    )


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

    _download_provider(row, m3u_dir)

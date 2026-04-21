"""
Ingestion tasks — parse downloaded M3U files and sync them to the database.

These tasks are triggered by the downloader after a successful download.
They are also registerable as standalone scheduled tasks if needed.

Flow per remote provider (m3u / xtream):
  1. Locate the downloaded .m3u file for the provider slug
  2. Parse it into structured entry dicts   (ingestion.parser)
  3. Sync parsed entries to the database    (ingestion.sync)
  4. Delete the .m3u file to keep disk clean

Flow per local_file provider:
  Steps 1-3 are the same, but step 4 is skipped — the file is owned by the
  user and must not be removed. Stale-stream cleanup is also skipped because
  the file is always present and its full content is re-read on every run.
"""
import logging
import os

from app.database import get_db
from app.ingestion.parser import parse_m3u
from app.ingestion.sync import run_sync
from app.tasks.base import task
from app.utils.env import resolve_path

logger = logging.getLogger("app.tasks.ingestion")

_M3U_DIR_RELATIVE = os.getenv("M3U_DIR", "data/m3u")


def _m3u_path(provider_slug: str) -> str:
    m3u_dir = resolve_path(_M3U_DIR_RELATIVE)
    return os.path.join(m3u_dir, f"{provider_slug}.m3u")


def _delete_m3u(file_path: str, provider_slug: str) -> None:
    try:
        os.remove(file_path)
        logger.info("[INGESTION] M3U file deleted after ingest — %s", file_path)
    except OSError as exc:
        logger.warning(
            "[INGESTION] Could not delete M3U file for '%s': %s",
            provider_slug, exc,
        )


def _get_provider_row(provider_slug: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT type, local_file_path FROM providers WHERE slug = ?",
            (provider_slug,),
        ).fetchone()
    return dict(row) if row else None


def ingest_provider_file(provider_slug: str) -> None:
    """
    Parse and sync a single provider's M3U file.

    For remote providers (m3u / xtream) the downloaded file is deleted after
    ingestion.  For local_file providers the file is never deleted and stale
    stream cleanup is skipped — the user controls the file on disk.
    """
    provider = _get_provider_row(provider_slug)
    is_local = provider is not None and provider["type"] == "local_file"

    if is_local:
        stored_path = (provider.get("local_file_path") or "").strip()
        if not stored_path:
            logger.warning(
                "[INGESTION] local_file provider '%s' has no file configured", provider_slug
            )
            return
        # stored_path may be absolute (from file browser) or a bare filename
        # (legacy entries saved before the browser was added)
        if os.path.isabs(stored_path):
            file_path = stored_path
        else:
            file_path = os.path.join(resolve_path(_M3U_DIR_RELATIVE), stored_path)
    else:
        file_path = _m3u_path(provider_slug)

    if not os.path.exists(file_path):
        logger.warning(
            "[INGESTION] M3U file not found for provider '%s': %s",
            provider_slug, file_path,
        )
        return

    logger.info(
        "[INGESTION] Starting ingest — provider=%s  file=%s  local=%s",
        provider_slug, file_path, is_local,
    )

    parsed = parse_m3u(file_path, provider=provider_slug)

    parse_stats = parsed["summary"]["stats"]
    logger.info(
        "[INGESTION] Parse complete — provider=%s  completed=%d  errors=%d",
        provider_slug,
        parse_stats.get("entries_completed", 0),
        parse_stats.get("errors", 0),
    )

    with get_db() as conn:
        sync_summary = run_sync(conn, parsed, skip_stale_cleanup=is_local)

    logger.info(
        "[INGESTION] Sync complete — provider=%s  "
        "entries[new=%d updated=%d]  streams[new=%d updated=%d]  "
        "stale_removed=%d  orphans_removed=%d",
        provider_slug,
        sync_summary["inserted_entries"],
        sync_summary["updated_entries"],
        sync_summary["inserted_streams"],
        sync_summary["updated_streams"],
        sync_summary["stale_streams_removed"],
        sync_summary["orphan_entries_removed"],
    )

    if not is_local:
        _delete_m3u(file_path, provider_slug)


@task("ingest_all_providers")
def ingest_all_providers() -> None:
    """
    Ingest M3U files for all active providers.

    For remote providers, scans the M3U directory for downloaded files.
    For local_file providers, reads the file path from the database.

    After ingestion, streams belonging to inactive or deleted providers are
    purged so the library only reflects currently active sources.
    """
    m3u_dir = resolve_path(_M3U_DIR_RELATIVE)

    with get_db() as conn:
        all_provider_rows = conn.execute(
            "SELECT slug, type, local_file_path, is_active FROM providers"
        ).fetchall()

    active_slugs = {r["slug"] for r in all_provider_rows if r["is_active"]}
    all_slugs    = {r["slug"] for r in all_provider_rows}

    if not active_slugs:
        logger.info("[INGESTION] No active providers found, nothing to ingest")
    else:
        logger.info("[INGESTION] Found %d active provider(s) to ingest", len(active_slugs))
        for slug in active_slugs:
            try:
                ingest_provider_file(slug)
            except Exception as exc:
                logger.error(
                    "[INGESTION] Failed to ingest '%s': %s", slug, exc, exc_info=True
                )

    # Collect provider slugs whose streams should be wiped:
    # - providers that exist but are inactive
    # - providers that appear in streams but no longer exist in providers table
    with get_db() as conn:
        stream_providers = {
            r[0] for r in conn.execute("SELECT DISTINCT provider FROM streams").fetchall()
        }
        inactive_slugs = {r["slug"] for r in all_provider_rows if not r["is_active"]}
        slugs_to_purge = inactive_slugs | (stream_providers - all_slugs)

        if slugs_to_purge:
            placeholders = ",".join("?" * len(slugs_to_purge))
            deleted_streams = conn.execute(
                f"DELETE FROM streams WHERE provider IN ({placeholders})",
                tuple(slugs_to_purge),
            ).rowcount
            deleted_entries = conn.execute(
                "DELETE FROM entries WHERE entry_id NOT IN (SELECT DISTINCT entry_id FROM streams)"
            ).rowcount
            logger.info(
                "[INGESTION] Purged streams for inactive/removed providers %s — "
                "streams=%d  orphan_entries=%d",
                sorted(slugs_to_purge), deleted_streams, deleted_entries,
            )
        else:
            logger.debug("[INGESTION] No inactive or removed providers to purge")


@task("ingest_provider")
def ingest_provider(provider_slug: str) -> None:
    """Ingest a single provider's M3U file. Used for targeted re-ingestion."""
    ingest_provider_file(provider_slug)

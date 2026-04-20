"""
Ingestion tasks — parse downloaded M3U files and sync them to the database.

These tasks are triggered by the downloader after a successful download.
They are also registerable as standalone scheduled tasks if needed.

Flow per provider:
  1. Locate the downloaded .m3u file for the provider slug
  2. Parse it into structured entry dicts   (ingestion.parser)
  3. Sync parsed entries to the database    (ingestion.sync)
  4. Delete the .m3u file to keep disk clean
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


def ingest_provider_file(provider_slug: str) -> None:
    """
    Parse and sync a single provider's downloaded M3U file, then delete it.

    Called directly from the downloader after a successful download.
    Can also be invoked as a standalone task.
    """
    file_path = _m3u_path(provider_slug)

    if not os.path.exists(file_path):
        logger.warning(
            "[INGESTION] M3U file not found for provider '%s': %s",
            provider_slug, file_path,
        )
        return

    logger.info(
        "[INGESTION] Starting ingest — provider=%s  file=%s",
        provider_slug, file_path,
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
        sync_summary = run_sync(conn, parsed)

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

    _delete_m3u(file_path, provider_slug)


@task("ingest_all_providers")
def ingest_all_providers() -> None:
    """
    Ingest downloaded M3U files for all active providers.

    Scans the M3U directory for any .m3u files and ingests each one,
    deriving the provider slug from the filename.
    """
    m3u_dir = resolve_path(_M3U_DIR_RELATIVE)

    if not os.path.isdir(m3u_dir):
        logger.info("[INGESTION] M3U directory does not exist, nothing to ingest: %s", m3u_dir)
        return

    m3u_files = [f for f in os.listdir(m3u_dir) if f.endswith(".m3u")]

    if not m3u_files:
        logger.info("[INGESTION] No .m3u files found in %s", m3u_dir)
        return

    logger.info("[INGESTION] Found %d .m3u file(s) to ingest", len(m3u_files))

    for filename in m3u_files:
        provider_slug = filename[:-4]  # strip .m3u
        try:
            ingest_provider_file(provider_slug)
        except Exception as exc:
            logger.error(
                "[INGESTION] Failed to ingest '%s': %s", provider_slug, exc, exc_info=True
            )


@task("ingest_provider")
def ingest_provider(provider_slug: str) -> None:
    """Ingest a single provider's M3U file. Used for targeted re-ingestion."""
    ingest_provider_file(provider_slug)

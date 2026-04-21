"""
Database Sync Layer — normalization, upsert, and cleanup.

Accepts parsed entry dicts from parser.py and persists them to SQLite.
Responsibilities:
  - Upsert entries (content identity, keyed by entry_id)
  - Upsert streams  (provider source, keyed by entry_id + provider)
  - Cleanup stale streams from a provider that are not in the latest batch
  - Cleanup orphaned entries that have no streams remaining

This layer never modifies raw parsed data and never touches the M3U files.
"""
import logging
import sqlite3
from typing import Iterable

from app.utils.env import local_now_iso

logger = logging.getLogger("app.ingestion.sync")


# ---------------------------------------------------------------------------
# UPSERT HELPERS
# ---------------------------------------------------------------------------

def _upsert_entry(conn: sqlite3.Connection, entry: dict) -> None:
    """Insert or update a content entry row. entry_id is the conflict key."""
    now = local_now_iso()
    sql = """
    INSERT INTO entries (
        entry_id, type, cleaned_title, raw_title,
        year, season, episode, air_date, series_type,
        created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(entry_id) DO UPDATE SET
        cleaned_title = excluded.cleaned_title,
        raw_title     = excluded.raw_title,
        year          = excluded.year,
        season        = excluded.season,
        episode       = excluded.episode,
        air_date      = excluded.air_date,
        series_type   = excluded.series_type,
        updated_at    = excluded.updated_at
    """
    conn.execute(sql, (
        entry["entry_id"],
        entry.get("type"),
        entry.get("cleaned_title"),
        entry.get("raw_title"),
        entry.get("year"),
        entry.get("season"),
        entry.get("episode"),
        entry.get("air_date"),
        entry.get("series_type"),
        now,
        now,
    ))


def _upsert_stream(conn: sqlite3.Connection, entry: dict) -> None:
    """
    Insert or update a stream row.
    Conflict key: (entry_id, provider) — one active stream URL per provider per entry.
    metadata_json is always refreshed so provider metadata changes are captured
    even when the stream URL has not changed.
    """
    sql = """
    INSERT INTO streams (
        entry_id, stream_url, provider,
        source_file, ingested_at, batch_id, metadata_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(entry_id, provider) DO UPDATE SET
        stream_url    = excluded.stream_url,
        source_file   = excluded.source_file,
        ingested_at   = excluded.ingested_at,
        batch_id      = excluded.batch_id,
        metadata_json = excluded.metadata_json
    """
    conn.execute(sql, (
        entry["entry_id"],
        entry.get("stream_url"),
        entry.get("provider"),
        entry.get("source_file"),
        entry.get("ingested_at"),
        entry.get("batch_id"),
        entry.get("metadata_json"),
    ))


# ---------------------------------------------------------------------------
# BATCH WRITE
# ---------------------------------------------------------------------------

def persist_entries(conn: sqlite3.Connection, entries: Iterable[dict]) -> dict:
    """
    Upsert a collection of parsed entries into entries + streams tables.

    Returns a summary dict with insert/update counts.
    """
    inserted_entries = 0
    updated_entries = 0
    inserted_streams = 0
    updated_streams = 0

    for entry in entries:
        entry_id = entry.get("entry_id")
        if not entry_id:
            logger.warning("[SYNC] Entry missing entry_id, skipping: %s", entry.get("raw_title", "?"))
            continue

        # Determine whether this entry already exists so we can count accurately
        existing = conn.execute(
            "SELECT 1 FROM entries WHERE entry_id = ?", (entry_id,)
        ).fetchone()

        _upsert_entry(conn, entry)

        if existing:
            updated_entries += 1
            logger.debug(
                "[SYNC] Entry UPDATED  id=%s  type=%-8s  title=%s",
                entry_id[:12], entry.get("type", "?"), entry.get("cleaned_title", "?")[:60],
            )
        else:
            inserted_entries += 1
            logger.debug(
                "[SYNC] Entry INSERTED id=%s  type=%-8s  title=%s",
                entry_id[:12], entry.get("type", "?"), entry.get("cleaned_title", "?")[:60],
            )

        # Stream check
        existing_stream = conn.execute(
            "SELECT 1 FROM streams WHERE entry_id = ? AND provider = ?",
            (entry_id, entry.get("provider")),
        ).fetchone()

        _upsert_stream(conn, entry)

        if existing_stream:
            updated_streams += 1
            logger.debug(
                "[SYNC] Stream UPDATED  entry=%s  provider=%s",
                entry_id[:12], entry.get("provider", "?"),
            )
        else:
            inserted_streams += 1
            logger.debug(
                "[SYNC] Stream INSERTED entry=%s  provider=%s  url=%s",
                entry_id[:12], entry.get("provider", "?"),
                (entry.get("stream_url") or "")[:80],
            )

    summary = {
        "inserted_entries": inserted_entries,
        "updated_entries": updated_entries,
        "inserted_streams": inserted_streams,
        "updated_streams": updated_streams,
    }

    logger.info(
        "[SYNC] Persist complete — entries new=%d updated=%d | streams new=%d updated=%d",
        inserted_entries, updated_entries, inserted_streams, updated_streams,
    )
    return summary


# ---------------------------------------------------------------------------
# CLEANUP
# ---------------------------------------------------------------------------

def cleanup_stale_streams(conn: sqlite3.Connection, provider: str, current_batch_id: str) -> int:
    """
    Delete streams for *provider* that were NOT seen in *current_batch_id*.
    This removes content that the provider has dropped since the last ingest.
    Returns the number of rows deleted.
    """
    cursor = conn.execute(
        "DELETE FROM streams WHERE provider = ? AND batch_id != ?",
        (provider, current_batch_id),
    )
    deleted = cursor.rowcount
    if deleted:
        logger.info(
            "[SYNC] Stale streams removed — provider=%s  batch=%s  count=%d",
            provider, current_batch_id[:12], deleted,
        )
    else:
        logger.debug(
            "[SYNC] No stale streams to remove — provider=%s  batch=%s",
            provider, current_batch_id[:12],
        )
    return deleted


def cleanup_orphan_entries(conn: sqlite3.Connection) -> int:
    """
    Delete entries that have no streams remaining.
    Happens when all providers have dropped a piece of content.
    Returns the number of rows deleted.
    """
    cursor = conn.execute(
        "DELETE FROM entries WHERE entry_id NOT IN (SELECT DISTINCT entry_id FROM streams)"
    )
    deleted = cursor.rowcount
    if deleted:
        logger.info("[SYNC] Orphaned entries removed — count=%d", deleted)
    else:
        logger.debug("[SYNC] No orphaned entries to remove")
    return deleted


def purge_inactive_and_deleted_providers(conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Delete streams (and then orphaned entries) for any provider that is either
    inactive or no longer present in the providers table.

    Returns (streams_deleted, entries_deleted).
    """
    all_rows = conn.execute("SELECT slug, is_active FROM providers").fetchall()
    all_slugs      = {r["slug"] for r in all_rows}
    inactive_slugs = {r["slug"] for r in all_rows if not r["is_active"]}

    stream_providers = {
        r[0] for r in conn.execute("SELECT DISTINCT provider FROM streams").fetchall()
    }
    slugs_to_purge = inactive_slugs | (stream_providers - all_slugs)

    if not slugs_to_purge:
        logger.debug("[SYNC] No inactive or removed providers to purge")
        return 0, 0

    placeholders = ",".join("?" * len(slugs_to_purge))
    deleted_streams = conn.execute(
        f"DELETE FROM streams WHERE provider IN ({placeholders})",
        tuple(slugs_to_purge),
    ).rowcount
    deleted_entries = conn.execute(
        "DELETE FROM entries WHERE entry_id NOT IN (SELECT DISTINCT entry_id FROM streams)"
    ).rowcount
    logger.info(
        "[SYNC] Purged inactive/removed providers %s — streams=%d  orphan_entries=%d",
        sorted(slugs_to_purge), deleted_streams, deleted_entries,
    )
    return deleted_streams, deleted_entries


# ---------------------------------------------------------------------------
# FULL SYNC PIPELINE
# ---------------------------------------------------------------------------

def run_sync(conn: sqlite3.Connection, parsed_result: dict, skip_stale_cleanup: bool = False) -> dict:
    """
    Full ingest pipeline for a single provider's parse result.

    Steps:
      1. Flatten all entry lists from the parsed result
      2. Upsert entries + streams
      3. Remove stale streams from this provider (skipped when skip_stale_cleanup=True)
      4. Remove orphaned entries

    skip_stale_cleanup should be True for local_file providers whose file is
    always present on disk — stale detection is meaningless for them.

    Returns a combined summary dict.
    """
    provider = None
    batch_id = parsed_result.get("batch_id", "")

    all_entries: list[dict] = (
        parsed_result.get("movies", [])
        + parsed_result.get("series", [])
        + parsed_result.get("live_tv", [])
        + parsed_result.get("tv_vod", [])
        + parsed_result.get("unsorted", [])
    )

    if all_entries:
        provider = all_entries[0].get("provider")

    logger.info(
        "[SYNC] Starting sync — provider=%s  batch=%s  total_entries=%d",
        provider, batch_id[:12] if batch_id else "?", len(all_entries),
    )

    persist_summary = persist_entries(conn, all_entries)

    stale_removed = 0
    orphans_removed = 0

    if provider and batch_id and not skip_stale_cleanup:
        stale_removed = cleanup_stale_streams(conn, provider, batch_id)
        orphans_removed = cleanup_orphan_entries(conn)

    summary = {
        **persist_summary,
        "stale_streams_removed": stale_removed,
        "orphan_entries_removed": orphans_removed,
        "provider": provider,
        "batch_id": batch_id,
    }

    logger.info(
        "[SYNC] Sync complete — provider=%s  stale_streams=%d  orphans=%d",
        provider, stale_removed, orphans_removed,
    )
    return summary

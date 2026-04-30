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
import os
import sqlite3
from typing import Iterable

from app.utils.env import local_now_iso, resolve_path

logger = logging.getLogger("app.ingestion.sync")

_VOD_ROOT_RELATIVE = os.getenv("VOD_DIR", "data/vod")


def _delete_strm_file(path: str) -> None:
    """Delete a .strm file and remove empty parent directories up to vod_root."""
    try:
        if os.path.exists(path):
            os.remove(path)
        vod_root = os.path.abspath(resolve_path(_VOD_ROOT_RELATIVE))
        parent = os.path.dirname(os.path.abspath(path))
        while parent and parent != vod_root:
            if os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
                parent = os.path.dirname(parent)
            else:
                break
    except OSError as exc:
        logger.warning("[SYNC] Failed to delete strm file %s: %s", path, exc)


# ---------------------------------------------------------------------------
# UPSERT HELPERS
# ---------------------------------------------------------------------------

def _upsert_entry(conn: sqlite3.Connection, entry: dict) -> None:
    """Insert or update a content entry row. entry_id is the conflict key."""
    now = local_now_iso()
    # tvg-logo is a raw EXTINF attribute present directly on the parsed entry dict.
    # For series entries, each episode upsert may overwrite cover_art so the last
    # processed stream's logo ends up stored — this is intentional (last wins).
    cover_art = (entry.get("tvg-logo") or "").strip() or None
    sql = """
    INSERT INTO entries (
        entry_id, type, cleaned_title, raw_title,
        year, season, episode, air_date, series_type,
        cover_art, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(entry_id) DO UPDATE SET
        cleaned_title   = excluded.cleaned_title,
        raw_title       = excluded.raw_title,
        year            = excluded.year,
        season          = excluded.season,
        episode         = excluded.episode,
        air_date        = excluded.air_date,
        series_type     = excluded.series_type,
        cover_art       = CASE WHEN excluded.cover_art IS NOT NULL THEN excluded.cover_art ELSE entries.cover_art END,
        tmdb_skipped_at = NULL,
        updated_at      = excluded.updated_at
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
        cover_art,
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
    Deletes .strm files for any stale rows that owned them before removing rows.
    Returns the number of rows deleted.
    """
    stale_owned = conn.execute(
        "SELECT strm_path FROM streams WHERE provider = ? AND batch_id != ? AND strm_path IS NOT NULL",
        (provider, current_batch_id),
    ).fetchall()
    for row in stale_owned:
        _delete_strm_file(row["strm_path"])

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


def purge_provider_data(conn: sqlite3.Connection, provider_slug: str) -> tuple[int, int]:
    """
    Delete all streams for a specific provider and then remove any orphaned
    entries that have no remaining streams.

    Also deletes any .strm files owned by this provider's streams.

    Returns (streams_deleted, entries_deleted).
    """
    owned_rows = conn.execute(
        "SELECT strm_path FROM streams WHERE provider = ? AND strm_path IS NOT NULL",
        (provider_slug,),
    ).fetchall()
    for row in owned_rows:
        _delete_strm_file(row["strm_path"])

    deleted_streams = conn.execute(
        "DELETE FROM streams WHERE provider = ?", (provider_slug,)
    ).rowcount
    deleted_entries = conn.execute(
        "DELETE FROM entries WHERE entry_id NOT IN (SELECT DISTINCT entry_id FROM streams)"
    ).rowcount

    if deleted_streams or deleted_entries:
        logger.info(
            "[SYNC] Purged provider '%s' — streams=%d  orphan_entries=%d",
            provider_slug, deleted_streams, deleted_entries,
        )
    return deleted_streams, deleted_entries


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
    owned_rows = conn.execute(
        f"SELECT strm_path FROM streams WHERE provider IN ({placeholders}) AND strm_path IS NOT NULL",
        tuple(slugs_to_purge),
    ).fetchall()
    for row in owned_rows:
        _delete_strm_file(row["strm_path"])

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
# FOLLOW RULES ENGINE
# ---------------------------------------------------------------------------

def apply_follow_rules(conn: sqlite3.Connection, provider_id: int) -> int:
    """
    Mark streams as eligible (imported=1) for follow rules matching provider_id.

    For each follow rule, finds streams from that provider where the entry
    type and title match the rule pattern. Season NULL matches all seasons;
    an integer season matches only that exact season number.

    Only sets imported=1 (never clears it — removal is a separate manual action).
    Returns count of newly marked streams.
    """
    provider_row = conn.execute(
        "SELECT slug FROM providers WHERE id = ?", (provider_id,)
    ).fetchone()
    if not provider_row:
        return 0

    rules = conn.execute(
        "SELECT entry_type, entry_title, season FROM follows WHERE provider_id = ?",
        (provider_id,),
    ).fetchall()
    if not rules:
        return 0

    marked = 0
    slug = provider_row["slug"]
    for rule in rules:
        if rule["season"] is not None and rule["entry_type"] == "tv_vod":
            # Year-specific tv_vod follow: season stores the year integer,
            # matched against the first 4 chars of air_date.
            conn.execute(
                """
                UPDATE streams SET imported = 1
                WHERE provider = ? AND imported = 0
                  AND entry_id IN (
                      SELECT entry_id FROM entries
                      WHERE type = 'tv_vod'
                        AND substr(air_date, 1, 4) = ?
                        AND lower(cleaned_title) LIKE lower(?)
                  )
                """,
                (slug, str(rule["season"]), f"%{rule['entry_title']}%"),
            )
        elif rule["season"] is not None:
            conn.execute(
                """
                UPDATE streams SET imported = 1
                WHERE provider = ? AND imported = 0
                  AND entry_id IN (
                      SELECT entry_id FROM entries
                      WHERE type = ? AND season = ?
                        AND lower(cleaned_title) LIKE lower(?)
                  )
                """,
                (slug, rule["entry_type"], rule["season"], f"%{rule['entry_title']}%"),
            )
        else:
            conn.execute(
                """
                UPDATE streams SET imported = 1
                WHERE provider = ? AND imported = 0
                  AND entry_id IN (
                      SELECT entry_id FROM entries
                      WHERE type = ?
                        AND lower(cleaned_title) LIKE lower(?)
                  )
                """,
                (slug, rule["entry_type"], f"%{rule['entry_title']}%"),
            )
        marked += conn.execute("SELECT changes()").fetchone()[0]

    return marked


# ---------------------------------------------------------------------------
# FULL SYNC PIPELINE
# ---------------------------------------------------------------------------

def run_sync(conn: sqlite3.Connection, parsed_result: dict) -> dict:
    """
    Full ingest pipeline for a single provider's parse result.

    Steps:
      1. Flatten all entry lists from the parsed result
      2. Upsert entries + streams
      3. Remove stale streams from this provider
      4. Remove orphaned entries

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

    if provider and batch_id:
        stale_removed = cleanup_stale_streams(conn, provider, batch_id)
        orphans_removed = cleanup_orphan_entries(conn)

    summary = {
        **persist_summary,
        "stale_streams_removed": stale_removed,
        "orphan_entries_removed": orphans_removed,
        "provider": provider,
        "batch_id": batch_id,
        "filter_streams_updated": 0,
        "follow_streams_marked": 0,
    }

    logger.info(
        "[SYNC] Sync complete — provider=%s  stale_streams=%d  orphans=%d",
        provider, stale_removed, orphans_removed,
    )

    try:
        from app.filters.engine import load_filters, run_filters_for_provider
        filters = load_filters(conn)
        if filters:
            summary["filter_streams_updated"] = run_filters_for_provider(conn, filters, provider=provider)
    except Exception as exc:
        logger.warning("[SYNC] Filter apply step failed (non-fatal): %s", exc)

    try:
        provider_row = conn.execute(
            "SELECT id, strm_mode FROM providers WHERE slug = ?", (provider,)
        ).fetchone()
        if provider_row and provider_row["strm_mode"] == "import_selected":
            follow_marked = apply_follow_rules(conn, provider_row["id"])
            summary["follow_streams_marked"] = follow_marked
            if follow_marked:
                logger.info(
                    "[SYNC] Follow rules matched — provider=%s  streams_marked=%d",
                    provider, follow_marked,
                )
    except Exception as exc:
        logger.warning("[SYNC] Follow rules step failed (non-fatal): %s", exc)

    return summary

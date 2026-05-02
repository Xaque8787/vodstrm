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
import json
import logging
import os
import re
import sqlite3
from typing import Iterable

from app.utils.env import local_now_iso, resolve_path

logger = logging.getLogger("app.ingestion.sync")

_VOD_ROOT_RELATIVE = os.getenv("VOD_DIR", "data/vod")


# ---------------------------------------------------------------------------
# QUALITY SCORING
# ---------------------------------------------------------------------------

def _quality_score(raw_title: str, quality_terms: list[str]) -> int:
    """
    Count how many quality terms appear in raw_title as whole words.
    Each term is matched with word boundaries (case insensitive) so that
    e.g. 'hd' does not match 'uhd' or 'hdr', and '1080p' does not match
    '21080p'. Terms are treated as plain text — special regex characters
    are escaped before comparison.
    """
    if not quality_terms or not raw_title:
        return 0
    score = 0
    for term in quality_terms:
        pattern = r"\b" + re.escape(term) + r"\b"
        if re.search(pattern, raw_title, re.IGNORECASE):
            score += 1
    return score


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
    Insert or fully update a stream row.
    Used when the incoming stream wins the quality check or when no quality
    terms are configured (unconditional overwrite, preserving old behaviour).
    Conflict key: (entry_id, provider).
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


def _stamp_stream_batch_id(conn: sqlite3.Connection, entry_id: str, provider: str, batch_id: str) -> None:
    """
    Update only batch_id on an existing stream row without touching any other
    fields. Used when the existing stream wins the quality check — the row
    must be stamped with the current batch_id so stale cleanup does not
    incorrectly delete it (stale cleanup removes rows whose batch_id differs
    from the current run's batch_id).
    """
    conn.execute(
        "UPDATE streams SET batch_id = ? WHERE entry_id = ? AND provider = ?",
        (batch_id, entry_id, provider),
    )


# ---------------------------------------------------------------------------
# BATCH WRITE
# ---------------------------------------------------------------------------

def persist_entries(conn: sqlite3.Connection, entries: Iterable[dict], quality_terms: list[str] | None = None) -> dict:
    """
    Upsert a collection of parsed entries into entries + streams tables.

    When quality_terms is a non-empty list, incoming streams are scored
    against existing streams for the same (entry_id, provider). The
    higher-scoring stream wins; ties keep the existing row. When
    quality_terms is empty or None the existing unconditional-overwrite
    behaviour is preserved.

    Returns a summary dict with insert/update counts.
    """
    terms = quality_terms or []
    use_quality = bool(terms)

    inserted_entries = 0
    updated_entries = 0
    inserted_streams = 0
    updated_streams = 0
    quality_kept = 0

    for entry in entries:
        entry_id = entry.get("entry_id")
        if not entry_id:
            logger.warning("[SYNC] Entry missing entry_id, skipping: %s", entry.get("raw_title", "?"))
            continue

        existing = conn.execute(
            "SELECT 1 FROM entries WHERE entry_id = ?", (entry_id,)
        ).fetchone()

        provider = entry.get("provider")
        batch_id = entry.get("batch_id", "")

        # Fetch existing stream's raw_title BEFORE upserting the entry so that
        # the entry upsert (which overwrites raw_title) does not corrupt the
        # quality comparison. raw_title lives on entries; the stream row itself
        # does not store it independently.
        existing_stream = conn.execute(
            "SELECT e.raw_title, s.batch_id AS stream_batch_id FROM entries e "
            "JOIN streams s ON s.entry_id = e.entry_id "
            "WHERE s.entry_id = ? AND s.provider = ?",
            (entry_id, provider),
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

        if existing_stream is None:
            # Fresh insert — quality check does not apply
            _upsert_stream(conn, entry)
            inserted_streams += 1
            logger.debug(
                "[SYNC] Stream INSERTED entry=%s  provider=%s  url=%s",
                entry_id[:12], provider or "?",
                (entry.get("stream_url") or "")[:80],
            )
        elif use_quality:
            existing_raw = existing_stream["raw_title"] or ""
            incoming_raw = entry.get("raw_title") or ""
            incoming_score = _quality_score(incoming_raw, terms)
            existing_score = _quality_score(existing_raw, terms)

            # If the existing stream is from a previous run it was not present
            # in the current M3U, so its URL may be dead. The incoming stream
            # is the only live option and must always win regardless of score.
            existing_is_stale = existing_stream["stream_batch_id"] != batch_id

            if existing_is_stale or incoming_score > existing_score:
                _upsert_stream(conn, entry)
                updated_streams += 1
                if existing_is_stale:
                    logger.debug(
                        "[SYNC] Stream UPDATED (prior winner absent, incoming takes over) entry=%s  provider=%s",
                        entry_id[:12], provider or "?",
                    )
                else:
                    logger.debug(
                        "[SYNC] Stream UPDATED (quality win %d>%d) entry=%s  provider=%s",
                        incoming_score, existing_score, entry_id[:12], provider or "?",
                    )
            else:
                # Existing wins or tie — stamp batch_id only so stale cleanup
                # does not remove the winning row at the end of this run.
                _stamp_stream_batch_id(conn, entry_id, provider, batch_id)
                # _upsert_entry already ran above and overwrote entries.raw_title
                # with the loser's title. Restore the winner's raw_title so that
                # the filter engine (which reads from entries) sees the correct
                # title for this stream.
                conn.execute(
                    "UPDATE entries SET raw_title = ? WHERE entry_id = ?",
                    (existing_raw, entry_id),
                )
                quality_kept += 1
                logger.debug(
                    "[SYNC] Stream KEPT   (quality %s %d<=%d) entry=%s  provider=%s",
                    "tie" if incoming_score == existing_score else "loss",
                    incoming_score, existing_score, entry_id[:12], provider or "?",
                )
        else:
            # No quality terms — unconditional overwrite (original behaviour)
            _upsert_stream(conn, entry)
            updated_streams += 1
            logger.debug(
                "[SYNC] Stream UPDATED  entry=%s  provider=%s",
                entry_id[:12], provider or "?",
            )

    summary = {
        "inserted_entries": inserted_entries,
        "updated_entries": updated_entries,
        "inserted_streams": inserted_streams,
        "updated_streams": updated_streams,
        "quality_kept": quality_kept,
    }

    logger.info(
        "[SYNC] Persist complete — entries new=%d updated=%d | streams new=%d updated=%d kept=%d",
        inserted_entries, updated_entries, inserted_streams, updated_streams, quality_kept,
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

def apply_follow_rules(conn: sqlite3.Connection, provider_slug: str) -> int:
    """
    Mark streams as imported=1 for any follow rule that matches content from provider_slug.

    Rules are global — scoped only by entry_type/entry_title/season, not by which
    provider they were originally created against. This ensures that a follow created
    while only Provider A existed will also auto-import matching streams from Provider B
    when Provider B is first ingested.

    Season NULL matches all seasons; an integer season matches only that exact season.
    For tv_vod, the season column stores the year as an integer.

    Only sets imported=1 (never clears — removal is a manual action).
    Returns count of newly marked streams.
    """
    rules = conn.execute(
        "SELECT DISTINCT entry_type, entry_title, season FROM follows"
    ).fetchall()
    if not rules:
        return 0

    marked = 0
    for rule in rules:
        if rule["season"] is not None and rule["entry_type"] == "tv_vod":
            conn.execute(
                """
                UPDATE streams SET imported = 1
                WHERE provider = ? AND imported = 0
                  AND entry_id IN (
                      SELECT entry_id FROM entries
                      WHERE type = 'tv_vod'
                        AND substr(air_date, 1, 4) = ?
                        AND lower(cleaned_title) = lower(?)
                  )
                """,
                (provider_slug, str(rule["season"]), rule["entry_title"]),
            )
        elif rule["season"] is not None:
            conn.execute(
                """
                UPDATE streams SET imported = 1
                WHERE provider = ? AND imported = 0
                  AND entry_id IN (
                      SELECT entry_id FROM entries
                      WHERE type = ? AND season = ?
                        AND lower(cleaned_title) = lower(?)
                  )
                """,
                (provider_slug, rule["entry_type"], rule["season"], rule["entry_title"]),
            )
        else:
            conn.execute(
                """
                UPDATE streams SET imported = 1
                WHERE provider = ? AND imported = 0
                  AND entry_id IN (
                      SELECT entry_id FROM entries
                      WHERE type = ?
                        AND lower(cleaned_title) = lower(?)
                  )
                """,
                (provider_slug, rule["entry_type"], rule["entry_title"]),
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

    quality_terms: list[str] = []
    if provider:
        prow = conn.execute(
            "SELECT quality_terms FROM providers WHERE slug = ?", (provider,)
        ).fetchone()
        if prow and prow["quality_terms"]:
            try:
                quality_terms = json.loads(prow["quality_terms"]) or []
            except (json.JSONDecodeError, TypeError):
                quality_terms = []

    persist_summary = persist_entries(conn, all_entries, quality_terms=quality_terms)

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
        summary["filter_streams_updated"] = run_filters_for_provider(conn, filters, provider=provider)
    except Exception as exc:
        logger.warning("[SYNC] Filter apply step failed (non-fatal): %s", exc)

    try:
        provider_row = conn.execute(
            "SELECT strm_mode FROM providers WHERE slug = ?", (provider,)
        ).fetchone()
        if provider_row and provider_row["strm_mode"] == "import_selected":
            follow_marked = apply_follow_rules(conn, provider)
            summary["follow_streams_marked"] = follow_marked
            if follow_marked:
                logger.info(
                    "[SYNC] Follow rules matched — provider=%s  streams_marked=%d",
                    provider, follow_marked,
                )
    except Exception as exc:
        logger.warning("[SYNC] Follow rules step failed (non-fatal): %s", exc)

    return summary

"""
STRM file sync engine.

Generates and maintains .strm files from the streams table, acting as a
synchronisation layer between the database (source of truth) and the
filesystem (projection).

Provider eligibility
────────────────────
A provider participates in STRM generation only when:
  - is_active = 1
  - strm_mode = 'generate_all'

Inactive providers and providers in import_selected mode are excluded.
Their stream rows remain in the DB for filter tracking but produce no files.

Provider priority
─────────────────
Each provider has a numeric `priority` column (lower = higher priority,
default 10).  When two or more eligible providers supply the same entry,
only the stream from the highest-priority (lowest number) provider generates
the .strm file.  Ties are broken alphabetically by provider slug so the
winner is always deterministic.  Lower-priority streams for the same entry
are tracked in the DB but produce no file.

Path derivation
───────────────
Paths are built from the stream's filtered_title (falling back to
entry.cleaned_title) and the entry type:

  movie   → <vod_root>/movies/<title> (<year>)/<title> (<year>).strm
  series  → <vod_root>/series/<title>/Season <SS>/<title> S<SS>E<EE>.strm
  tv_vod  → <vod_root>/series/<title>/<title>.strm
  live    → <vod_root>/livetv/<title>.strm
  unsorted→ <vod_root>/unsorted/<title>.strm

Sync rules (per winning stream row)
────────────────────────────────────
  New stream (strm_path IS NULL):
    create file, write URL, store path + URL in DB.

  URL changed (last_written_url != stream_url):
    overwrite file, update last_written_url in DB.

  Path changed (derived path != strm_path):
    move file to new location, update strm_path in DB.
    After moving, remove the old parent directory if now empty.
    Do NOT delete-and-recreate — move preserves inode history.

  Unchanged:
    do nothing.

Non-winning streams for an entry have their strm_path/last_written_url
cleared if they somehow acquired values from a previous priority ordering.

Cleanup
───────
After processing all eligible streams, scan the entire vod_root for .strm
files. Any file whose absolute path does not appear in the DB is deleted.
Empty directories left behind are also removed.

deactivate_provider_strm(provider_slug)
────────────────────────────────────────
Called when a provider is disabled (is_active → 0) or switches to
import_selected mode.  For each entry where this provider currently owns the
.strm file (has strm_path), the function:
  1. Searches for the next eligible winner from the remaining active
     generate_all providers, excluding the departing provider.
  2. If a replacement exists:
       - Derives the replacement's target path.
       - If the current file path matches the replacement's path: overwrite
         the URL in place.
       - If the paths differ: move the file to the replacement's path.
       - Records the replacement's strm_path + last_written_url in the DB.
  3. If no replacement exists: deletes the file and removes empty dirs.
  4. Clears strm_path + last_written_url on the departing provider's row.
"""
import logging
import os
import re
import sqlite3

from app.database import get_db
from app.tasks.base import task
from app.utils.env import resolve_path

logger = logging.getLogger("app.tasks.strm")

_VOD_ROOT_RELATIVE = os.getenv("VOD_DIR", "data/vod")


def _vod_root() -> str:
    return resolve_path(_VOD_ROOT_RELATIVE)


# ---------------------------------------------------------------------------
# Path derivation helpers
# ---------------------------------------------------------------------------

_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _safe(name: str) -> str:
    """Strip filesystem-unsafe characters from a path component."""
    return _UNSAFE.sub("", name).strip(". ") or "_"


def _derive_path(
    entry_type: str,
    title: str,
    year: int | None,
    season: int | None,
    episode: int | None,
    vod_root: str,
) -> str:
    t = _safe(title)

    if entry_type == "movie":
        label = f"{t} ({year})" if year else t
        return os.path.join(vod_root, "movies", label, f"{label}.strm")

    if entry_type == "series":
        s = season if season is not None else 0
        e = episode if episode is not None else 0
        season_dir = f"Season {s:02d}"
        filename   = f"{t} S{s:02d}E{e:02d}.strm"
        return os.path.join(vod_root, "series", t, season_dir, filename)

    if entry_type == "tv_vod":
        return os.path.join(vod_root, "series", t, f"{t}.strm")

    if entry_type == "live":
        return os.path.join(vod_root, "livetv", f"{t}.strm")

    # unsorted / fallback
    return os.path.join(vod_root, "unsorted", f"{t}.strm")


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def _write_strm(path: str, url: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(url)


def _move_strm(old_path: str, new_path: str) -> None:
    os.makedirs(os.path.dirname(new_path), exist_ok=True)
    os.rename(old_path, new_path)
    _remove_empty_dirs(os.path.dirname(old_path))


def _remove_empty_dirs(directory: str) -> None:
    """Walk upward from directory, removing empty folders until vod_root."""
    vod = _vod_root()
    current = directory
    while current and os.path.abspath(current) != os.path.abspath(vod):
        try:
            if not os.listdir(current):
                os.rmdir(current)
                current = os.path.dirname(current)
            else:
                break
        except OSError:
            break


# ---------------------------------------------------------------------------
# Priority resolution
# ---------------------------------------------------------------------------

def _winning_stream_ids(conn: sqlite3.Connection, provider_slug: str | None = None) -> set[int]:
    """
    Return the set of stream_ids that are the priority winner for their entry.

    For each entry_id that has at least one generate_all stream, exactly one
    stream wins: the one from the provider with the lowest priority number,
    breaking ties by provider slug alphabetically.

    If provider_slug is given, only entries that have a stream from that
    provider are considered — but the winner is still chosen globally across
    all generate_all providers for those entries.
    """
    if provider_slug:
        rows = conn.execute(
            """
            SELECT s.stream_id, s.entry_id, p.priority, p.slug
            FROM streams s
            JOIN providers p ON p.slug = s.provider
            WHERE p.is_active = 1
              AND p.strm_mode = 'generate_all'
              AND s.exclude = 0
              AND s.entry_id IN (
                  SELECT entry_id FROM streams WHERE provider = ?
              )
            ORDER BY p.priority, p.slug
            """,
            (provider_slug,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT s.stream_id, s.entry_id, p.priority, p.slug
            FROM streams s
            JOIN providers p ON p.slug = s.provider
            WHERE p.is_active = 1
              AND p.strm_mode = 'generate_all'
              AND s.exclude = 0
            ORDER BY p.priority, p.slug
            """,
        ).fetchall()

    seen: set[str] = set()
    winners: set[int] = set()
    for row in rows:
        entry_id = row["entry_id"]
        if entry_id not in seen:
            seen.add(entry_id)
            winners.add(row["stream_id"])
    return winners


# ---------------------------------------------------------------------------
# Core sync logic
# ---------------------------------------------------------------------------

def _sync_streams(conn: sqlite3.Connection, vod_root: str) -> dict:
    stats = {
        "skipped_priority": 0,
        "created":          0,
        "url_updated":      0,
        "moved":            0,
        "unchanged":        0,
        "errors":           0,
    }

    winners = _winning_stream_ids(conn)

    rows = conn.execute(
        """
        SELECT s.stream_id, s.stream_url, s.provider,
               s.strm_path, s.last_written_url,
               s.filtered_title,
               e.type, e.cleaned_title, e.year, e.season, e.episode
        FROM streams s
        JOIN entries e ON e.entry_id = s.entry_id
        JOIN providers p ON p.slug = s.provider
        WHERE p.is_active = 1
          AND p.strm_mode = 'generate_all'
          AND s.exclude = 0
        ORDER BY s.stream_id
        """
    ).fetchall()

    for row in rows:
        if row["stream_id"] not in winners:
            # This provider lost the priority contest for this entry.
            # If it somehow holds a strm_path (e.g. priority was changed),
            # clear it — the file belongs to the winner now.
            if row["strm_path"]:
                conn.execute(
                    "UPDATE streams SET strm_path = NULL, last_written_url = NULL WHERE stream_id = ?",
                    (row["stream_id"],),
                )
            stats["skipped_priority"] += 1
            continue
        try:
            _sync_one(conn, row, vod_root, stats)
        except Exception as exc:
            logger.error(
                "[STRM] Error syncing stream_id=%s: %s", row["stream_id"], exc, exc_info=True
            )
            stats["errors"] += 1

    return stats


def _sync_one(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    vod_root: str,
    stats: dict,
) -> None:
    stream_id        = row["stream_id"]
    stream_url       = row["stream_url"]
    stored_path      = row["strm_path"]
    last_written_url = row["last_written_url"]
    title            = (row["filtered_title"] or row["cleaned_title"] or "").strip()

    if not title:
        logger.warning("[STRM] stream_id=%s has no title — skipping", stream_id)
        return

    target_path = _derive_path(
        entry_type=row["type"],
        title=title,
        year=row["year"],
        season=row["season"],
        episode=row["episode"],
        vod_root=vod_root,
    )

    # ── New stream ────────────────────────────────────────────────────────
    if not stored_path:
        _write_strm(target_path, stream_url)
        conn.execute(
            "UPDATE streams SET strm_path = ?, last_written_url = ? WHERE stream_id = ?",
            (target_path, stream_url, stream_id),
        )
        stats["created"] += 1
        return

    path_changed = os.path.abspath(stored_path) != os.path.abspath(target_path)
    url_changed  = last_written_url != stream_url

    # ── Path changed → move first ─────────────────────────────────────────
    if path_changed:
        if os.path.exists(stored_path):
            _move_strm(stored_path, target_path)
        else:
            # Old file missing — write fresh at new location
            _write_strm(target_path, stream_url)
        conn.execute(
            "UPDATE streams SET strm_path = ? WHERE stream_id = ?",
            (target_path, stream_id),
        )
        stats["moved"] += 1
        stored_path = target_path

    # ── URL changed → overwrite ───────────────────────────────────────────
    if url_changed:
        _write_strm(stored_path, stream_url)
        conn.execute(
            "UPDATE streams SET last_written_url = ? WHERE stream_id = ?",
            (stream_url, stream_id),
        )
        stats["url_updated"] += 1
        return

    if not path_changed:
        stats["unchanged"] += 1


# ---------------------------------------------------------------------------
# Orphan cleanup
# ---------------------------------------------------------------------------

def _cleanup_orphans(conn: sqlite3.Connection, vod_root: str) -> int:
    """
    Delete any .strm file on disk that has no matching strm_path row in DB.
    Returns count of files deleted.
    """
    known_paths: set[str] = set()
    for (path,) in conn.execute(
        "SELECT strm_path FROM streams WHERE strm_path IS NOT NULL"
    ).fetchall():
        known_paths.add(os.path.abspath(path))

    deleted = 0
    for dirpath, _dirnames, filenames in os.walk(vod_root):
        for fname in filenames:
            if not fname.endswith(".strm"):
                continue
            full = os.path.abspath(os.path.join(dirpath, fname))
            if full not in known_paths:
                try:
                    os.remove(full)
                    deleted += 1
                    _remove_empty_dirs(dirpath)
                    logger.debug("[STRM] Orphan deleted: %s", full)
                except OSError as exc:
                    logger.warning("[STRM] Could not delete orphan %s: %s", full, exc)

    return deleted


# ---------------------------------------------------------------------------
# Provider deactivation — handover + cleanup
# ---------------------------------------------------------------------------

def deactivate_provider_strm(provider_slug: str) -> dict:
    """
    Handle STRM state when a provider is disabled or switched to import_selected.

    For every entry where provider_slug currently owns the .strm file
    (strm_path IS NOT NULL), finds the next eligible winner from the remaining
    active generate_all providers and either:
      - hands the file over to that winner (overwrite URL in place, or move if
        the replacement's derived path differs), or
      - deletes the file if no replacement exists.

    Always clears strm_path + last_written_url on provider_slug's stream rows.

    The caller must have already committed the is_active=0 or strm_mode change
    to the DB before calling this, so the departing provider is excluded from
    the replacement search automatically.

    Returns a stats dict with keys: handed_over, deleted, errors.
    """
    stats = {"handed_over": 0, "deleted": 0, "errors": 0}
    vod_root = _vod_root()

    with get_db() as conn:
        # Streams that this provider currently owns (wrote to disk)
        owned = conn.execute(
            """
            SELECT s.stream_id, s.entry_id, s.strm_path,
                   s.filtered_title,
                   e.type, e.cleaned_title, e.year, e.season, e.episode
            FROM streams s
            JOIN entries e ON e.entry_id = s.entry_id
            WHERE s.provider = ?
              AND s.strm_path IS NOT NULL
            """,
            (provider_slug,),
        ).fetchall()

        for owned_row in owned:
            entry_id   = owned_row["entry_id"]
            owned_path = owned_row["strm_path"]

            # Find the best replacement: active, generate_all, not this provider,
            # lowest priority then slug alphabetically
            replacement = conn.execute(
                """
                SELECT s.stream_id, s.stream_url,
                       s.filtered_title,
                       e.type, e.cleaned_title, e.year, e.season, e.episode
                FROM streams s
                JOIN entries e ON e.entry_id = s.entry_id
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ?
                  AND s.provider != ?
                  AND p.is_active = 1
                  AND p.strm_mode = 'generate_all'
                  AND s.exclude = 0
                ORDER BY p.priority, p.slug
                LIMIT 1
                """,
                (entry_id, provider_slug),
            ).fetchone()

            try:
                if replacement:
                    rep_title = (
                        replacement["filtered_title"] or replacement["cleaned_title"] or ""
                    ).strip()
                    if not rep_title:
                        # Replacement has no usable title; fall through to delete
                        replacement = None
                    else:
                        rep_path = _derive_path(
                            entry_type=replacement["type"],
                            title=rep_title,
                            year=replacement["year"],
                            season=replacement["season"],
                            episode=replacement["episode"],
                            vod_root=vod_root,
                        )
                        rep_url = replacement["stream_url"]

                        if owned_path and os.path.exists(owned_path):
                            if os.path.abspath(owned_path) == os.path.abspath(rep_path):
                                # Same path — just overwrite the URL
                                _write_strm(rep_path, rep_url)
                            else:
                                # Different path — move then the file is at rep_path;
                                # content will be overwritten with the replacement URL
                                _move_strm(owned_path, rep_path)
                                _write_strm(rep_path, rep_url)
                        else:
                            # Owned file missing — write fresh
                            _write_strm(rep_path, rep_url)

                        conn.execute(
                            "UPDATE streams SET strm_path = ?, last_written_url = ? WHERE stream_id = ?",
                            (rep_path, rep_url, replacement["stream_id"]),
                        )
                        stats["handed_over"] += 1
                        logger.debug(
                            "[STRM] Handed over entry %s from %s to stream_id=%s",
                            entry_id, provider_slug, replacement["stream_id"],
                        )

                if not replacement:
                    # No eligible successor — delete the file
                    if owned_path and os.path.exists(owned_path):
                        os.remove(owned_path)
                        _remove_empty_dirs(os.path.dirname(owned_path))
                    stats["deleted"] += 1
                    logger.debug(
                        "[STRM] No replacement for entry %s — file deleted", entry_id
                    )

            except Exception as exc:
                logger.error(
                    "[STRM] Handover error for entry %s: %s", entry_id, exc, exc_info=True
                )
                stats["errors"] += 1

        # Clear all strm state for this provider regardless of what happened above
        conn.execute(
            "UPDATE streams SET strm_path = NULL, last_written_url = NULL WHERE provider = ?",
            (provider_slug,),
        )

    logger.info(
        "[STRM] Deactivation — provider=%s  handed_over=%d  deleted=%d  errors=%d",
        provider_slug, stats["handed_over"], stats["deleted"], stats["errors"],
    )

    # Final pass: catch any files that slipped through (e.g. mid-handover failures)
    with get_db() as conn:
        orphans = _cleanup_orphans(conn, vod_root)
    if orphans:
        logger.info("[STRM] Post-deactivation orphan sweep — deleted=%d", orphans)

    return stats


# ---------------------------------------------------------------------------
# Public task entry points
# ---------------------------------------------------------------------------

@task("clean_strm_orphans")
def clean_strm_orphans() -> None:
    """
    Scan the vod_root and delete any .strm file that has no matching strm_path
    row in the DB.  Safe to run at any time — treats the DB as ground truth.
    """
    vod_root = _vod_root()
    if not os.path.isdir(vod_root):
        logger.info("[STRM] Orphan sweep: vod_root does not exist — nothing to do")
        return
    with get_db() as conn:
        deleted = _cleanup_orphans(conn, vod_root)
    logger.info("[STRM] Orphan sweep complete — deleted=%d", deleted)


@task("generate_strm")
def generate_strm(provider_slug: str | None = None) -> None:
    """
    Synchronise .strm files for all generate_all providers (or one provider).

    provider_slug=None runs the full sync for every eligible provider and
    performs the orphan cleanup pass.  Passing a slug limits processing to
    that provider's streams only (but priority is still evaluated globally
    across all providers for those entries) and skips the global orphan sweep.
    """
    vod_root = _vod_root()
    os.makedirs(vod_root, exist_ok=True)

    logger.info("[STRM] Sync start — provider=%s  vod_root=%s", provider_slug or "*", vod_root)

    with get_db() as conn:
        if provider_slug:
            row = conn.execute(
                "SELECT is_active, strm_mode FROM providers WHERE slug = ?", (provider_slug,)
            ).fetchone()
            if not row:
                logger.warning("[STRM] Provider '%s' not found — aborting", provider_slug)
                return
            if not row["is_active"]:
                logger.info("[STRM] Provider '%s' is inactive — skipping", provider_slug)
                return
            if row["strm_mode"] != "generate_all":
                logger.info(
                    "[STRM] Provider '%s' is in import_selected mode — skipping", provider_slug
                )
                return

            winners = _winning_stream_ids(conn, provider_slug=provider_slug)

            rows = conn.execute(
                """
                SELECT s.stream_id, s.stream_url, s.provider,
                       s.strm_path, s.last_written_url,
                       s.filtered_title,
                       e.type, e.cleaned_title, e.year, e.season, e.episode
                FROM streams s
                JOIN entries e ON e.entry_id = s.entry_id
                WHERE s.provider = ?
                  AND s.exclude = 0
                ORDER BY s.stream_id
                """,
                (provider_slug,),
            ).fetchall()

            stats: dict = {
                "skipped_priority": 0, "created": 0, "url_updated": 0,
                "moved": 0, "unchanged": 0, "errors": 0,
            }
            for row_data in rows:
                if row_data["stream_id"] not in winners:
                    if row_data["strm_path"]:
                        conn.execute(
                            "UPDATE streams SET strm_path = NULL, last_written_url = NULL WHERE stream_id = ?",
                            (row_data["stream_id"],),
                        )
                    stats["skipped_priority"] += 1
                    continue
                try:
                    _sync_one(conn, row_data, vod_root, stats)
                except Exception as exc:
                    logger.error(
                        "[STRM] Error syncing stream_id=%s: %s",
                        row_data["stream_id"], exc, exc_info=True,
                    )
                    stats["errors"] += 1
        else:
            stats = _sync_streams(conn, vod_root)
            orphans = _cleanup_orphans(conn, vod_root)
            logger.info("[STRM] Orphan cleanup — deleted=%d", orphans)

    logger.info(
        "[STRM] Sync done — provider=%s  created=%d  moved=%d  url_updated=%d  "
        "skipped_priority=%d  unchanged=%d  errors=%d",
        provider_slug or "*",
        stats["created"],
        stats["moved"],
        stats["url_updated"],
        stats["skipped_priority"],
        stats["unchanged"],
        stats["errors"],
    )

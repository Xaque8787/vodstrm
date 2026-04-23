"""
STRM file sync engine.

Generates and maintains .strm files from the streams table, acting as a
synchronisation layer between the database (source of truth) and the
filesystem (projection).

Provider gate
─────────────
Only providers with strm_mode = 'generate_all' participate. Providers in
'import_selected' mode are skipped; their streams still exist and are
filtered normally — STRM generation for them is deferred to a future
UI-driven pipeline that will use streams.imported = TRUE.

Provider priority
─────────────────
Each provider has a numeric `priority` column (lower = higher priority,
default 10).  When two or more generate_all providers supply the same
entry, only the stream from the highest-priority (lowest number) provider
generates the .strm file.  Ties are broken alphabetically by provider slug
so the winner is always deterministic.  Lower-priority streams for the same
entry are tracked in the DB but produce no file.

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

clear_provider_strm_files(provider_slug)
────────────────────────────────────────
Called when a provider switches to import_selected mode.  Deletes every
.strm file owned by that provider, removes resulting empty directories, and
clears strm_path + last_written_url on the affected stream rows so the
state is consistent (no file on disk → NULL in DB).
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
            WHERE p.strm_mode = 'generate_all'
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
            WHERE p.strm_mode = 'generate_all'
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
        WHERE p.strm_mode = 'generate_all'
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
# Mode-switch cleanup
# ---------------------------------------------------------------------------

def clear_provider_strm_files(provider_slug: str) -> int:
    """
    Delete all .strm files owned by provider_slug and clear the DB fields.

    Called when a provider is switched to import_selected mode so that the
    filesystem stays consistent with the DB (no file = NULL strm_path).
    Returns the number of files deleted.
    """
    deleted = 0
    with get_db() as conn:
        rows = conn.execute(
            "SELECT stream_id, strm_path FROM streams WHERE provider = ? AND strm_path IS NOT NULL",
            (provider_slug,),
        ).fetchall()

        for row in rows:
            path = row["strm_path"]
            try:
                if path and os.path.exists(path):
                    os.remove(path)
                    deleted += 1
                    _remove_empty_dirs(os.path.dirname(path))
                    logger.debug("[STRM] Removed file on mode switch: %s", path)
            except OSError as exc:
                logger.warning("[STRM] Could not remove %s: %s", path, exc)

        conn.execute(
            "UPDATE streams SET strm_path = NULL, last_written_url = NULL WHERE provider = ?",
            (provider_slug,),
        )

    logger.info(
        "[STRM] Mode-switch cleanup — provider=%s  files_deleted=%d", provider_slug, deleted
    )
    return deleted


# ---------------------------------------------------------------------------
# Public task entry points
# ---------------------------------------------------------------------------

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
                "SELECT strm_mode FROM providers WHERE slug = ?", (provider_slug,)
            ).fetchone()
            if not row:
                logger.warning("[STRM] Provider '%s' not found — aborting", provider_slug)
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

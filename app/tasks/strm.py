"""
STRM file sync engine.

Generates and maintains .strm files from the streams table, acting as a
synchronisation layer between the database (source of truth) and the
filesystem (projection).

Provider gate
─────────────
Only providers with strm_mode = 'generate_all' participate. Providers in
'import_selected' mode are skipped entirely; their streams rows still exist
and are filtered normally — STRM generation for them is deferred to a future
UI-driven pipeline that will use streams.imported = TRUE.

Path derivation
───────────────
Paths are built from the stream's filtered_title (falling back to
entry.cleaned_title) and the entry type:

  movie   → <vod_root>/movies/<title> (<year>)/<title> (<year>).strm
  series  → <vod_root>/series/<title>/Season <SS>/<title> S<SS>E<EE>.strm
  tv_vod  → <vod_root>/series/<title>/<title>.strm
  live    → <vod_root>/livetv/<title>.strm
  unsorted→ <vod_root>/unsorted/<title>.strm

Sync rules (per stream row)
───────────────────────────
  New stream (strm_path IS NULL):
    create file, write URL, store path + URL in DB.

  URL changed (last_written_url != stream_url):
    overwrite file, update last_written_url in DB.

  Path changed (derived path != strm_path):
    move file to new location, update strm_path in DB.
    After moving, remove the old parent directory if it is now empty.
    Do NOT delete-and-recreate — move preserves inode history.

  Unchanged:
    do nothing.

Cleanup
───────
After processing all eligible streams, scan the entire vod_root for .strm
files. Any file whose absolute path does not appear in the DB is deleted.
Empty directories left behind are also removed.
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
# Core sync logic
# ---------------------------------------------------------------------------

def _sync_streams(conn: sqlite3.Connection, vod_root: str) -> dict:
    stats = {
        "skipped_provider": 0,
        "created":  0,
        "url_updated": 0,
        "moved":    0,
        "unchanged": 0,
        "errors":   0,
    }

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
# Public task entry points
# ---------------------------------------------------------------------------

@task("generate_strm")
def generate_strm(provider_slug: str | None = None) -> None:
    """
    Synchronise .strm files for all generate_all providers (or one provider).

    provider_slug=None runs the full sync for every eligible provider and
    performs the orphan cleanup pass.  Passing a slug limits processing to
    that provider's streams only and skips the global orphan sweep (to avoid
    deleting files that belong to other providers whose paths haven't been
    evaluated yet).
    """
    vod_root = _vod_root()
    os.makedirs(vod_root, exist_ok=True)

    logger.info("[STRM] Sync start — provider=%s  vod_root=%s", provider_slug or "*", vod_root)

    with get_db() as conn:
        if provider_slug:
            # Check provider exists and is in generate_all mode
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

            # Scope the sync to this provider only
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
                "skipped_provider": 0, "created": 0, "url_updated": 0,
                "moved": 0, "unchanged": 0, "errors": 0,
            }
            for row_data in rows:
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
        "unchanged=%d  errors=%d",
        provider_slug or "*",
        stats["created"],
        stats["moved"],
        stats["url_updated"],
        stats["unchanged"],
        stats["errors"],
    )

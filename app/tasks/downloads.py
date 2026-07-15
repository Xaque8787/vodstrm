"""
Download processor — converts remote stream URLs to local media files via ffmpeg.

State machine: pending → probing → downloading → completed | failed | cancelled

The processor job wakes on a schedule, claims up to max_concurrent rows, probes
each stream with ffprobe, downloads via ffmpeg remux (stream copy) into a
staging file, then moves the completed file to its final VOD path.

Failed rows are retained for retry. The STRM engine excludes entries with
active or completed downloads from .strm generation (Model B).
"""
import json
import logging
import os
import shutil
import subprocess
import threading

from app.database import get_db
from app.tasks.base import task
from app.utils.env import local_now_iso, resolve_path

logger = logging.getLogger("app.tasks.downloads")

_VOD_ROOT_RELATIVE = os.getenv("VOD_DIR", "data/vod")
_STAGING_DIR_RELATIVE = "data/downloads"

_lock = threading.Lock()
_active_count = 0

# Defaults; overridden by integrations settings at runtime
_DEFAULTS = {
    "enabled": True,
    "max_concurrent": 2,
    "default_container": "mkv",
    "retention_days": 90,
    "ffmpeg_timeout": 3600,
}


def _load_settings(conn) -> dict:
    row = conn.execute(
        "SELECT settings FROM integrations WHERE slug = 'downloads'"
    ).fetchone()
    if not row:
        return dict(_DEFAULTS)
    try:
        saved = json.loads(row["settings"] or "{}")
    except (ValueError, TypeError):
        return dict(_DEFAULTS)
    merged = dict(_DEFAULTS)
    merged.update(saved)
    return merged


def _staging_dir() -> str:
    path = resolve_path(_STAGING_DIR_RELATIVE)
    os.makedirs(path, exist_ok=True)
    return path


def _vod_root() -> str:
    return resolve_path(_VOD_ROOT_RELATIVE)


def _derive_media_path(entry_type, title, year, season, episode,
                       container, air_date=None):
    """Import the path derivation from strm to keep paths in sync."""
    from app.tasks.strm import _derive_media_path as _dmp
    return _dmp(entry_type, title, year, season, episode,
                _vod_root(), container=container, air_date=air_date)


def _ffprobe_stream(url: str, timeout: int = 30) -> dict | None:
    """Run ffprobe on a URL, return parsed JSON or None on failure."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", url],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            logger.warning("[DOWNLOADS] ffprobe failed: %s", result.stderr[:200])
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as exc:
        logger.warning("[DOWNLOADS] ffprobe error: %s", exc)
        return None


def _ffmpeg_download(url: str, dest_path: str, timeout: int = 3600) -> bool:
    """Download via ffmpeg stream-copy remux. Returns True on success.

    No hardcoded bitstream filter — ffmpeg auto-detects the codec.
    h264_mp4toannexb only works for H.264 and fails on HEVC/MPEG-2/AV1
    streams common in IPTV.
    """
    cmd = [
        "ffmpeg", "-y",
        "-err_detect", "ignore_err",
        "-i", url,
        "-c", "copy",
        dest_path,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            logger.warning("[DOWNLOADS] ffmpeg failed: %s", result.stderr[:300])
            return False
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 0
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.warning("[DOWNLOADS] ffmpeg error: %s", exc)
        return False


def _winning_stream(conn, entry_id: str) -> tuple[str, str, str] | None:
    """Return (stream_url, provider_slug, filtered_title) for the highest-priority eligible stream.

    Downloads use any active, non-excluded stream regardless of the provider's
    strm_mode — the user explicitly requested a download, so the STRM import
    gating does not apply.
    """
    row = conn.execute(
        """
        SELECT s.stream_url, s.provider, s.filtered_title
        FROM streams s
        JOIN providers p ON p.slug = s.provider
        WHERE s.entry_id = ?
          AND p.is_active = 1
          AND s.exclude = 0
          AND (s.include_only_active = 0 OR s.include_only = 1)
        ORDER BY p.priority, p.slug
        LIMIT 1
        """,
        (entry_id,),
    ).fetchone()
    if not row:
        return None
    return row["stream_url"], row["provider"], row["filtered_title"] or ""


def _process_one(conn, row, settings) -> bool:
    """Process a single download row through probe → download → finalize. Returns True on completion."""
    entry_id = row["entry_id"]
    now = local_now_iso()
    container = settings["default_container"]
    timeout = settings.get("ffmpeg_timeout", 3600)

    # Get winning stream
    stream_info = _winning_stream(conn, entry_id)
    if not stream_info:
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason='no_eligible_stream', "
            "failed_at=?, updated_at=? WHERE entry_id=?",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    stream_url, provider, filtered_title = stream_info

    # Transition to probing
    conn.execute(
        "UPDATE downloads SET status='probing', stream_url=?, provider=?, "
        "probing_at=?, updated_at=? WHERE entry_id=?",
        (stream_url, provider, now, now, entry_id),
    )
    conn.commit()

    # Probe
    probe_data = _ffprobe_stream(stream_url)
    if not probe_data:
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason='probe_failed', "
            "failed_at=?, updated_at=?, retry_count=retry_count+1 WHERE entry_id=?",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    # Transition to downloading
    staging = os.path.join(_staging_dir(), f"{entry_id}.part")
    conn.execute(
        "UPDATE downloads SET status='downloading', probe_data=?, "
        "staging_path=?, downloading_at=?, updated_at=? WHERE entry_id=?",
        (json.dumps(probe_data), staging, now, now, entry_id),
    )
    conn.commit()

    # Download
    success = _ffmpeg_download(stream_url, staging, timeout=timeout)
    if not success:
        if os.path.exists(staging):
            try:
                os.remove(staging)
            except OSError:
                pass
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason='ffmpeg_failed', "
            "failed_at=?, updated_at=?, retry_count=retry_count+1 WHERE entry_id=?",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    # Derive final path from entry metadata
    entry = conn.execute(
        "SELECT type, cleaned_title, year, season, episode, air_date FROM entries WHERE entry_id=?",
        (entry_id,),
    ).fetchone()
    if not entry:
        conn.execute(
            "UPDATE downloads SET status='cancelled', fail_reason='entry_deleted', "
            "cancelled_at=?, updated_at=? WHERE entry_id=?",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    title = filtered_title or entry["cleaned_title"]
    final_path = _derive_media_path(
        entry["type"], title, entry["year"], entry["season"],
        entry["episode"], container, air_date=entry["air_date"],
    )

    # Move staging file to final location
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    shutil.move(staging, final_path)

    file_size = os.path.getsize(final_path)
    conn.execute(
        "UPDATE downloads SET status='completed', local_path=?, container=?, "
        "file_size=?, staging_path=NULL, completed_at=?, updated_at=? WHERE entry_id=?",
        (final_path, container, file_size, now, now, entry_id),
    )
    conn.commit()
    logger.info("[DOWNLOADS] Completed — entry=%s file=%s size=%d",
                entry_id[:12], final_path, file_size)
    return True


@task("process_downloads")
def process_downloads(force: bool = False) -> None:
    """Wake up, claim pending download rows up to max_concurrent, process each.

    When force=True, skip the enabled check (manual trigger from the UI).
    """
    global _active_count

    with _lock:
        if _active_count > 0:
            logger.debug("[DOWNLOADS] Already processing, skipping this cycle")
            return

    with get_db() as conn:
        settings = _load_settings(conn)
        max_concurrent = settings["max_concurrent"]

        if max_concurrent < 1:
            logger.warning("[DOWNLOADS] max_concurrent is %d, skipping", max_concurrent)
            return

        if not force and not settings.get("enabled", False):
            logger.debug("[DOWNLOADS] Integration is disabled, skipping")
            return

        # Ensure the staging directory exists
        staging = _staging_dir()
        os.makedirs(staging, exist_ok=True)

        # Claim pending rows (atomic-ish: mark as probing to prevent double-claim)
        pending = conn.execute(
            "SELECT entry_id FROM downloads WHERE status='pending' "
            "ORDER BY queued_at LIMIT ?",
            (max_concurrent,),
        ).fetchall()

        if not pending:
            logger.debug("[DOWNLOADS] No pending downloads")
            return

        logger.info("[DOWNLOADS] Processing %d pending download(s)", len(pending))

    for row in pending:
        entry_id = row["entry_id"]
        with _lock:
            _active_count += 1
        try:
            with get_db() as conn:
                dl_row = conn.execute(
                    "SELECT * FROM downloads WHERE entry_id=? AND status='pending'",
                    (entry_id,),
                ).fetchone()
                if not dl_row:
                    continue
                settings = _load_settings(conn)
                _process_one(conn, dl_row, settings)
        except Exception as exc:
            logger.error("[DOWNLOADS] Error processing entry=%s: %s", entry_id[:12], exc, exc_info=True)
            try:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE downloads SET status='failed', fail_reason=?, "
                        "failed_at=?, updated_at=? WHERE entry_id=?",
                        (str(exc)[:200], local_now_iso(), local_now_iso(), entry_id),
                    )
                    conn.commit()
            except Exception:
                pass
        finally:
            with _lock:
                _active_count -= 1

    # Trigger STRM regeneration so .strm files are cleaned up for completed downloads
    try:
        from app.tasks.strm import generate_strm
        generate_strm()
    except Exception as exc:
        logger.error("[DOWNLOADS] generate_strm after processing failed: %s", exc, exc_info=True)


def _kick_processor() -> None:
    """Launch process_downloads in a background thread (fire-and-forget).

    The _active_count guard inside process_downloads prevents duplicate runs,
    so calling this after every queue_download is safe. Respects the
    integration's enabled setting — if disabled, the processor will no-op.
    """
    import threading
    threading.Thread(target=process_downloads, daemon=True).start()


def queue_download(entry_id: str, conn=None) -> bool:
    """Insert a pending download row for an entry. Returns True if newly queued.

    Automatically kicks the processor so the download starts without manual
    intervention from the Integrations page. Pass an existing connection via
    `conn` to avoid "database is locked" errors when called from within a
    transaction.
    """
    now = local_now_iso()
    owns_conn = conn is None
    if owns_conn:
        conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT status FROM downloads WHERE entry_id=?", (entry_id,)
        ).fetchone()
        if existing:
            # Re-queue if previously failed or cancelled
            if existing["status"] in ("failed", "cancelled"):
                conn.execute(
                    "UPDATE downloads SET status='pending', fail_reason=NULL, "
                    "retry_count=0, queued_at=?, updated_at=? WHERE entry_id=?",
                    (now, now, entry_id),
                )
                if owns_conn:
                    conn.commit()
                logger.info("[DOWNLOADS] Re-queued failed download — entry=%s", entry_id[:12])
                _kick_processor()
                return True
            return False

        conn.execute(
            "INSERT INTO downloads (entry_id, status, queued_at, updated_at) "
            "VALUES (?, 'pending', ?, ?)",
            (entry_id, now, now),
        )
        if owns_conn:
            conn.commit()
        logger.info("[DOWNLOADS] Queued new download — entry=%s", entry_id[:12])
        _kick_processor()
        return True
    finally:
        if owns_conn:
            conn.close()


def cancel_download(entry_id: str, delete_file: bool = False) -> bool:
    """Cancel a download. If delete_file, remove the local media file too."""
    now = local_now_iso()
    with get_db() as conn:
        row = conn.execute(
            "SELECT local_path, staging_path, status FROM downloads WHERE entry_id=?",
            (entry_id,),
        ).fetchone()
        if not row:
            return False

        if delete_file:
            if row["local_path"] and os.path.exists(row["local_path"]):
                try:
                    os.remove(row["local_path"])
                except OSError:
                    pass
            if row["staging_path"] and os.path.exists(row["staging_path"]):
                try:
                    os.remove(row["staging_path"])
                except OSError:
                    pass

        if row["status"] in ("probing", "downloading"):
            conn.execute(
                "UPDATE downloads SET status='cancelled', cancelled_at=?, updated_at=? WHERE entry_id=?",
                (now, now, entry_id),
            )
        elif row["status"] == "completed" and delete_file:
            conn.execute("DELETE FROM downloads WHERE entry_id=?", (entry_id,))
        else:
            conn.execute(
                "UPDATE downloads SET status='cancelled', cancelled_at=?, updated_at=? WHERE entry_id=?",
                (now, now, entry_id),
            )
        conn.commit()
        return True


def reset_stuck_downloads() -> int:
    """Reset rows stuck in probing/downloading back to pending. Called at startup."""
    now = local_now_iso()
    with get_db() as conn:
        cursor = conn.execute(
            "UPDATE downloads SET status='pending', updated_at=? "
            "WHERE status IN ('probing','downloading')",
            (now,),
        )
        count = cursor.rowcount
        if count:
            conn.commit()
            logger.info("[DOWNLOADS] Reset %d stuck download(s) to pending", count)
        return count

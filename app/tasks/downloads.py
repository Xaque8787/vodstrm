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
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit

import requests

from app.config import settings
from app.database import get_connection, get_db
from app.tasks.base import task
from app.utils.env import local_now_iso, resolve_path

logger = logging.getLogger("app.tasks.downloads")

_VOD_ROOT_RELATIVE = settings.vod_path
_OFFLINE_ROOT_RELATIVE = settings.vod_offline_path

_lock = threading.Lock()
_active_count = 0
_cancel_lock = threading.Lock()
_cancel_events: dict[str, threading.Event] = {}

# Defaults; overridden by integrations settings at runtime
_DEFAULTS = {
    "enabled": True,
    "max_concurrent": 2,
    "default_container": "mkv",
    "retention_days": 90,
    "ffmpeg_timeout": 3600,
}

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}
_PROGRESSIVE_CONTAINERS = {
    "mp4": {"mov", "mp4", "m4a", "3gp", "3g2", "mj2"},
    "m4v": {"mov", "mp4", "m4a", "3gp", "3g2", "mj2"},
    "mov": {"mov", "mp4", "m4a", "3gp", "3g2", "mj2"},
    "mkv": {"matroska", "webm"},
    "webm": {"matroska", "webm"},
    "ts": {"mpegts"},
    "m2ts": {"mpegts"},
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


def _offline_root() -> str:
    path = resolve_path(_OFFLINE_ROOT_RELATIVE)
    os.makedirs(path, exist_ok=True)
    return path


def _vod_root() -> str:
    return resolve_path(_VOD_ROOT_RELATIVE)


def validate_storage_roots() -> None:
    """Fail fast when VOD and offline roots cannot share hard links."""
    offline_root = _offline_root()
    vod_root = _vod_root()
    os.makedirs(offline_root, exist_ok=True)
    os.makedirs(vod_root, exist_ok=True)
    descriptor, source_path = tempfile.mkstemp(
        prefix=".vodstrm-hardlink-", dir=offline_root
    )
    os.close(descriptor)
    link_path = os.path.join(vod_root, os.path.basename(source_path))
    try:
        os.link(source_path, link_path)
    except OSError as exc:
        raise RuntimeError(
            "VOD_PATH and VOD_OFFLINE_PATH must be writable and on the same "
            "filesystem so completed media can be hard-linked"
        ) from exc
    finally:
        for path in (link_path, source_path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass


def _remove_empty_dirs(directory: str, root: str) -> None:
    """Walk upward from directory, removing empty folders until root."""
    current = directory
    while current and os.path.abspath(current) != os.path.abspath(root):
        try:
            if not os.listdir(current):
                os.rmdir(current)
                current = os.path.dirname(current)
            else:
                break
        except OSError:
            break


def _derive_media_path(entry_type, title, year, season, episode,
                       container, root, air_date=None):
    """Import the path derivation from strm to keep paths in sync."""
    from app.tasks.strm import _derive_media_path as _dmp
    return _dmp(entry_type, title, year, season, episode,
                root, container=container, air_date=air_date)


def _ensure_hardlink(source_path: str, link_path: str) -> None:
    """Create or replace link_path as a hard link to source_path."""
    os.makedirs(os.path.dirname(link_path), exist_ok=True)
    if os.path.exists(link_path):
        if os.path.samefile(source_path, link_path):
            return
        os.remove(link_path)
    os.link(source_path, link_path)


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


def _ffmpeg_download(
    url: str,
    dest_path: str,
    timeout: int = 3600,
    progress_callback=None,
    expected_size: int | None = None,
    cancel_event: threading.Event | None = None,
) -> bool:
    """Download via ffmpeg stream-copy remux. Returns True on success.

    No hardcoded bitstream filter — ffmpeg auto-detects the codec.
    h264_mp4toannexb only works for H.264 and fails on HEVC/MPEG-2/AV1
    streams common in IPTV.
    """
    cmd = [
        "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
        "-err_detect", "ignore_err",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-copy_unknown",
        "-i", url,
        "-map", "0",
        "-map_metadata", "0",
        "-map_chapters", "0",
        "-c", "copy",
        dest_path,
    ]
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        logger.warning("[DOWNLOADS] ffmpeg error: %s", exc)
        return False

    if progress_callback:
        progress_callback(0, expected_size)
    started_at = time.monotonic()
    while process.poll() is None:
        if cancel_event and cancel_event.wait(timeout=0.5):
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
            logger.info("[DOWNLOADS] ffmpeg transfer cancelled")
            return False
        if not cancel_event:
            time.sleep(0.5)
        if time.monotonic() - started_at >= timeout:
            process.kill()
            process.communicate()
            logger.warning("[DOWNLOADS] ffmpeg timed out after %d seconds", timeout)
            return False
        if progress_callback and os.path.exists(dest_path):
            progress_callback(os.path.getsize(dest_path), expected_size)

    _, stderr = process.communicate()
    if process.returncode != 0:
        logger.warning("[DOWNLOADS] ffmpeg failed (exit %d): %s", process.returncode, stderr[-800:])
        return False
    if progress_callback and os.path.exists(dest_path):
        progress_callback(os.path.getsize(dest_path), expected_size)
    return os.path.exists(dest_path) and os.path.getsize(dest_path) > 0


def _progressive_source_container(url: str, probe_data: dict) -> str | None:
    """Return the source extension when URL and probe agree on a file container."""
    extension = os.path.splitext(urlsplit(url).path)[1].lower().lstrip(".")
    compatible_formats = _PROGRESSIVE_CONTAINERS.get(extension)
    if not compatible_formats:
        return None
    format_names = {
        value.strip().lower()
        for value in str(probe_data.get("format", {}).get("format_name") or "").split(",")
        if value.strip()
    }
    if not format_names.intersection(compatible_formats):
        return None
    return "mp4" if extension in {"mp4", "m4v", "mov"} else extension


def _http_download_exact(
    url: str,
    dest_path: str,
    progress_callback=None,
    cancel_event: threading.Event | None = None,
) -> bool:
    """Copy a progressive HTTP media object byte-for-byte without remuxing."""
    try:
        with requests.get(
            url,
            headers=_HTTP_HEADERS,
            stream=True,
            allow_redirects=True,
            timeout=(30, 60),
        ) as response:
            response.raise_for_status()
            content_type = (response.headers.get("content-type") or "").lower()
            if content_type.startswith("text/") or "json" in content_type:
                logger.warning(
                    "[DOWNLOADS] Direct media download returned non-media content-type=%s",
                    content_type or "unknown",
                )
                return False
            expected_size = None
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    expected_size = int(content_length)
                except ValueError:
                    expected_size = None
            bytes_written = 0
            last_reported_bytes = 0
            last_reported_at = time.monotonic()
            if progress_callback:
                progress_callback(0, expected_size)
            with open(dest_path, "wb") as destination:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if cancel_event and cancel_event.is_set():
                        logger.info("[DOWNLOADS] Direct transfer cancelled")
                        return False
                    if chunk:
                        destination.write(chunk)
                        bytes_written += len(chunk)
                        now = time.monotonic()
                        if progress_callback and (
                            bytes_written == expected_size
                            or bytes_written - last_reported_bytes >= 16 * 1024 * 1024
                            or now - last_reported_at >= 1
                        ):
                            progress_callback(bytes_written, expected_size)
                            last_reported_bytes = bytes_written
                            last_reported_at = now
            if progress_callback and bytes_written != last_reported_bytes:
                progress_callback(bytes_written, expected_size)
        if expected_size is not None and bytes_written != expected_size:
            logger.warning(
                "[DOWNLOADS] Direct download incomplete — expected=%d received=%d",
                expected_size,
                bytes_written,
            )
            return False
        return os.path.exists(dest_path) and bytes_written > 0
    except (requests.RequestException, OSError) as exc:
        logger.warning(
            "[DOWNLOADS] Direct lossless download failed (%s)",
            type(exc).__name__,
        )
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


def _process_one(conn, row, settings, cancel_event=None) -> bool:
    """Process a single download row through probe → download → finalize. Returns True on completion."""
    entry_id = row["entry_id"]
    now = local_now_iso()
    configured_container = settings["default_container"]
    timeout = settings.get("ffmpeg_timeout", 3600)

    def is_cancelled():
        return cancel_event is not None and cancel_event.is_set()

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
    if is_cancelled():
        return False

    # Transition to probing
    conn.execute(
        "UPDATE downloads SET status='probing', stream_url=?, provider=?, "
        "probing_at=?, updated_at=? WHERE entry_id=?",
        (stream_url, provider, now, now, entry_id),
    )
    conn.commit()

    # Probe
    probe_data = _ffprobe_stream(stream_url)
    if is_cancelled():
        return False
    if not probe_data:
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason='probe_failed', "
            "failed_at=?, updated_at=?, retry_count=retry_count+1 WHERE entry_id=?",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    source_container = _progressive_source_container(stream_url, probe_data)
    container = source_container or configured_container
    try:
        expected_size = int(probe_data.get("format", {}).get("size") or 0) or None
    except (TypeError, ValueError):
        expected_size = None

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
    offline_path = _derive_media_path(
        entry["type"], title, entry["year"], entry["season"],
        entry["episode"], container, _offline_root(), air_date=entry["air_date"],
    )
    vod_path = _derive_media_path(
        entry["type"], title, entry["year"], entry["season"],
        entry["episode"], container, _vod_root(), air_date=entry["air_date"],
    )
    staging = f"{offline_path}.part"
    os.makedirs(os.path.dirname(staging), exist_ok=True)

    # Transition to downloading
    conn.execute(
        "UPDATE downloads SET status='downloading', probe_data=?, container=?, "
        "staging_path=?, offline_path=?, expected_size=?, downloaded_bytes=0, "
        "downloading_at=?, updated_at=? WHERE entry_id=?",
        (json.dumps(probe_data), container, staging, offline_path, expected_size, now, now, entry_id),
    )
    conn.commit()

    def record_progress(bytes_written, total_bytes):
        if is_cancelled():
            return
        conn.execute(
            "UPDATE downloads SET downloaded_bytes=?, "
            "expected_size=COALESCE(?, expected_size), updated_at=? "
            "WHERE entry_id=? AND status='downloading'",
            (bytes_written, total_bytes, local_now_iso(), entry_id),
        )
        conn.commit()

    # Download
    if source_container:
        logger.info(
            "[DOWNLOADS] Preserving original %s container with byte-for-byte download",
            source_container,
        )

        success = _http_download_exact(
            stream_url,
            staging,
            progress_callback=record_progress,
            cancel_event=cancel_event,
        )
        failure_reason = "direct_download_failed"
    else:
        success = _ffmpeg_download(
            stream_url,
            staging,
            timeout=timeout,
            progress_callback=record_progress,
            expected_size=expected_size,
            cancel_event=cancel_event,
        )
        failure_reason = "ffmpeg_failed"
    if not success:
        if os.path.exists(staging):
            try:
                os.remove(staging)
            except OSError:
                pass
        if is_cancelled():
            return False
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason=?, "
            "failed_at=?, updated_at=?, retry_count=retry_count+1 WHERE entry_id=?",
            (failure_reason, now, now, entry_id),
        )
        conn.commit()
        return False

    if is_cancelled():
        if os.path.exists(staging):
            try:
                os.remove(staging)
            except OSError:
                pass
        return False

    # Finalization and cancellation signaling share a lock so the last-chunk
    # race either cancels staging or cleanly deletes an already-completed file.
    with _cancel_lock:
        if is_cancelled():
            if os.path.exists(staging):
                try:
                    os.remove(staging)
                except OSError:
                    pass
            return False
        os.makedirs(os.path.dirname(offline_path), exist_ok=True)
        os.replace(staging, offline_path)
        try:
            _ensure_hardlink(offline_path, vod_path)
        except OSError as exc:
            conn.execute(
                "UPDATE downloads SET status='failed', fail_reason='hardlink_failed', "
                "offline_path=?, staging_path=NULL, failed_at=?, updated_at=?, "
                "retry_count=retry_count+1 WHERE entry_id=?",
                (offline_path, now, now, entry_id),
            )
            conn.commit()
            logger.error(
                "[DOWNLOADS] Could not hard-link offline media into VOD tree (%s)",
                exc,
            )
            return False

        file_size = os.path.getsize(offline_path)
        conn.execute(
            "UPDATE downloads SET status='completed', local_path=?, offline_path=?, container=?, "
            "file_size=?, expected_size=COALESCE(expected_size, ?), "
            "downloaded_bytes=?, staging_path=NULL, completed_at=?, updated_at=? "
            "WHERE entry_id=?",
            (vod_path, offline_path, container, file_size, file_size, file_size, now, now, entry_id),
        )
        conn.commit()
    logger.info(
        "[DOWNLOADS] Completed — entry=%s offline=%s vod_link=%s size=%d",
        entry_id[:12], offline_path, vod_path, file_size,
    )
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

        os.makedirs(_offline_root(), exist_ok=True)

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
        cancel_event = threading.Event()
        with _cancel_lock:
            _cancel_events[entry_id] = cancel_event
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
                _process_one(conn, dl_row, settings, cancel_event=cancel_event)
        except Exception as exc:
            logger.error("[DOWNLOADS] Error processing entry=%s: %s", entry_id[:12], exc, exc_info=True)
            try:
                with get_db() as conn:
                    if not cancel_event.is_set():
                        conn.execute(
                            "UPDATE downloads SET status='failed', fail_reason=?, "
                            "failed_at=?, updated_at=? WHERE entry_id=?",
                            (str(exc)[:200], local_now_iso(), local_now_iso(), entry_id),
                        )
                        conn.commit()
            except Exception:
                pass
        finally:
            with _cancel_lock:
                if _cancel_events.get(entry_id) is cancel_event:
                    _cancel_events.pop(entry_id, None)
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

    A short delay gives the caller's transaction time to commit before the
    processor tries to read pending rows. The _active_count guard inside
    process_downloads prevents duplicate runs, so calling this after every
    queue_download is safe. Respects the integration's enabled setting.
    """
    import threading
    def _run():
        import time
        time.sleep(0.5)
        process_downloads()
    threading.Thread(target=_run, daemon=True).start()


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
                    "retry_count=0, expected_size=NULL, downloaded_bytes=0, "
                    "queued_at=?, updated_at=? WHERE entry_id=?",
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
    with _cancel_lock:
        cancel_event = _cancel_events.get(entry_id)
        if cancel_event:
            cancel_event.set()
    now = local_now_iso()
    with get_db() as conn:
        row = conn.execute(
            "SELECT local_path, offline_path, staging_path, status FROM downloads WHERE entry_id=?",
            (entry_id,),
        ).fetchone()
        if not row:
            return False

        if delete_file:
            if row["local_path"] and os.path.exists(row["local_path"]):
                try:
                    os.remove(row["local_path"])
                    _remove_empty_dirs(os.path.dirname(row["local_path"]), _vod_root())
                except OSError:
                    pass
            if row["offline_path"] and os.path.exists(row["offline_path"]):
                try:
                    os.remove(row["offline_path"])
                    _remove_empty_dirs(
                        os.path.dirname(row["offline_path"]), _offline_root()
                    )
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
        elif delete_file:
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

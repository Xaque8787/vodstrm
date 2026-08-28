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
import re
import subprocess
import threading
import time
from urllib.parse import urlsplit

import requests

from app.config import settings
from app.database import get_connection, get_db
from app.tasks.base import task
from app.utils.env import local_now_iso, resolve_path

logger = logging.getLogger("app.tasks.downloads")

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
_FFMPEG_OUTPUT_FORMATS = {
    "mkv": "matroska",
    "mp4": "mp4",
    "ts": "mpegts",
}


def _redact_media_error(message: str) -> str:
    return re.sub(r"https?://[^\s]+", "[REDACTED_URL]", message)


def _looks_like_mpegts(url: str) -> bool:
    """Check three packet boundaries without downloading the media object."""
    try:
        with requests.get(
            url,
            headers={**_HTTP_HEADERS, "Range": "bytes=0-563"},
            stream=True,
            allow_redirects=True,
            timeout=(15, 15),
        ) as response:
            response.raise_for_status()
            prefix = bytearray()
            for chunk in response.iter_content(chunk_size=564):
                prefix.extend(chunk)
                if len(prefix) >= 564:
                    break
    except requests.RequestException:
        return False
    return len(prefix) > 376 and all(prefix[offset] == 0x47 for offset in (0, 188, 376))


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


def _remove_empty_offline_dirs(path: str) -> None:
    """Remove empty item ancestors without removing the category or root."""
    root = os.path.abspath(_offline_root())
    current = os.path.abspath(os.path.dirname(path))
    try:
        if os.path.commonpath((root, current)) != root:
            return
    except ValueError:
        return
    relative = os.path.relpath(current, root)
    category = relative.split(os.sep, 1)[0]
    if category in {".", ".."}:
        return
    category_root = os.path.join(root, category)
    while current != category_root:
        try:
            os.rmdir(current)
        except OSError:
            break
        current = os.path.dirname(current)


def _remove_download_file(path: str | None) -> None:
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        return
    _remove_empty_offline_dirs(path)


def _derive_media_path(entry_type, title, year, season, episode,
                       container, root, air_date=None):
    """Import the path derivation from strm to keep paths in sync."""
    from app.tasks.strm import _derive_media_path as _dmp
    return _dmp(entry_type, title, year, season, episode,
                root, container=container, air_date=air_date)


def _ffprobe_stream(url: str, timeout: int = 30) -> dict | None:
    """Run ffprobe on a URL, return parsed JSON or None on failure."""
    try:
        base_command = [
            "ffprobe", "-v", "error", "-user_agent", _HTTP_HEADERS["User-Agent"],
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5", "-print_format", "json",
            "-show_format", "-show_streams",
        ]
        result = subprocess.run(
            [*base_command, url], capture_output=True, text=True, timeout=timeout,
        )
        input_format = None
        if result.returncode != 0 and _looks_like_mpegts(url):
            logger.warning(
                "[DOWNLOADS] Provider mislabeled MPEG-TS media; retrying probe "
                "with forced input format"
            )
            input_format = "mpegts"
            result = subprocess.run(
                [*base_command, "-f", input_format, url],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        if result.returncode != 0:
            logger.warning(
                "[DOWNLOADS] ffprobe failed: %s",
                _redact_media_error(result.stderr)[:500],
            )
            return None
        probe_data = json.loads(result.stdout)
        if input_format:
            probe_data["_vodstrm_input_format"] = input_format
        return probe_data
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
    input_format: str | None = None,
    output_container: str = "mkv",
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
        "-user_agent", _HTTP_HEADERS["User-Agent"],
        "-copy_unknown",
    ]
    if input_format:
        cmd.extend(["-f", input_format])
    cmd.extend([
        "-i", url,
        "-map", "0",
        "-map_metadata", "0",
        "-map_chapters", "0",
        "-c", "copy",
        "-f", _FFMPEG_OUTPUT_FORMATS.get(output_container, output_container),
        dest_path,
    ])
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
        logger.warning(
            "[DOWNLOADS] ffmpeg failed (exit %d): %s",
            process.returncode,
            _redact_media_error(stderr)[-800:],
        )
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
    range_chunk_size = 4 * 1024 * 1024
    expected_size = None
    last_reported_bytes = 0
    last_reported_at = time.monotonic()
    while expected_size is None or os.path.getsize(dest_path) < expected_size:
        range_completed = False
        for attempt in range(1, 5):
            try:
                offset = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
                range_end = offset + range_chunk_size - 1
                if expected_size is not None:
                    range_end = min(range_end, expected_size - 1)
                headers = {
                    **_HTTP_HEADERS,
                    "Range": f"bytes={offset}-{range_end}",
                }
                with requests.get(
                    url,
                    headers=headers,
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

                    content_range = response.headers.get("content-range") or ""
                    range_match = re.fullmatch(
                        r"bytes (\d+)-(\d+)/(\d+)", content_range
                    )
                    if response.status_code == 206:
                        if not range_match or int(range_match.group(1)) != offset:
                            logger.warning(
                                "[DOWNLOADS] Direct download returned an invalid byte range"
                            )
                            return False
                        response_end = int(range_match.group(2))
                        expected_size = int(range_match.group(3))
                        if response_end > range_end or response_end >= expected_size:
                            logger.warning(
                                "[DOWNLOADS] Direct download returned an invalid range boundary"
                            )
                            return False
                        response_size = response_end - offset + 1
                    elif offset:
                        logger.warning(
                            "[DOWNLOADS] Source ignored byte-range resume"
                        )
                        return False
                    else:
                        content_length = response.headers.get("content-length")
                        try:
                            expected_size = int(content_length) if content_length else None
                        except ValueError:
                            expected_size = None
                        response_size = expected_size

                    bytes_written = offset
                    if progress_callback:
                        progress_callback(bytes_written, expected_size)
                    with open(dest_path, "ab" if offset else "wb") as destination:
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
                    if response_size is not None and bytes_written - offset != response_size:
                        raise requests.exceptions.ChunkedEncodingError(
                            "incomplete byte range"
                        )
                    if progress_callback and bytes_written != last_reported_bytes:
                        progress_callback(bytes_written, expected_size)
                    range_completed = True
                    break
            except requests.RequestException as exc:
                logger.warning(
                    "[DOWNLOADS] Direct range interrupted (%s), attempt %d/4",
                    type(exc).__name__,
                    attempt,
                )
            except OSError as exc:
                logger.warning(
                    "[DOWNLOADS] Direct lossless download failed (%s)",
                    type(exc).__name__,
                )
                return False
            if cancel_event and cancel_event.is_set():
                return False
            if attempt < 4:
                time.sleep(attempt)
        if not range_completed:
            return False
        if expected_size is None:
            return os.path.getsize(dest_path) > 0
    return os.path.getsize(dest_path) == expected_size


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
        if cancel_event is not None and cancel_event.is_set():
            return True
        current = conn.execute(
            "SELECT status FROM downloads WHERE entry_id=?", (entry_id,)
        ).fetchone()
        return current is None or current["status"] == "cancelled"

    # Get winning stream
    stream_info = _winning_stream(conn, entry_id)
    if not stream_info:
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason='no_eligible_stream', "
            "failed_at=?, updated_at=? WHERE entry_id=? AND status='pending'",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    stream_url, provider, filtered_title = stream_info
    if is_cancelled():
        return False

    # Transition to probing
    cursor = conn.execute(
        "UPDATE downloads SET status='probing', stream_url=?, provider=?, "
        "probing_at=?, updated_at=? WHERE entry_id=? AND status='pending'",
        (stream_url, provider, now, now, entry_id),
    )
    if cursor.rowcount != 1:
        conn.rollback()
        return False
    conn.commit()

    # Probe
    probe_data = _ffprobe_stream(stream_url)
    if not probe_data:
        try:
            from app.ingestion.xtream_native import refresh_episode_stream
            refreshed_url = refresh_episode_stream(conn, entry_id, provider)
        except Exception as exc:
            logger.warning(
                "[DOWNLOADS] Xtream episode refresh failed (%s)",
                type(exc).__name__,
            )
            refreshed_url = None
        if refreshed_url and refreshed_url != stream_url:
            logger.info(
                "[DOWNLOADS] Retrying probe with refreshed Xtream episode stream"
            )
            stream_url = refreshed_url
            probe_data = _ffprobe_stream(stream_url)
            conn.execute(
                "UPDATE downloads SET stream_url=?, updated_at=? "
                "WHERE entry_id=? AND status='probing'",
                (stream_url, local_now_iso(), entry_id),
            )
            conn.commit()
    if is_cancelled():
        return False
    if not probe_data:
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason='probe_failed', "
            "failed_at=?, updated_at=?, retry_count=retry_count+1 "
            "WHERE entry_id=? AND status='probing'",
            (now, now, entry_id),
        )
        conn.commit()
        return False

    input_format = probe_data.pop("_vodstrm_input_format", None)
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
    staging = f"{offline_path}.part"
    os.makedirs(os.path.dirname(staging), exist_ok=True)

    # Transition to downloading
    cursor = conn.execute(
        "UPDATE downloads SET status='downloading', probe_data=?, container=?, "
        "staging_path=?, offline_path=?, expected_size=?, downloaded_bytes=0, "
        "downloading_at=?, updated_at=? WHERE entry_id=? AND status='probing'",
        (json.dumps(probe_data), container, staging, offline_path, expected_size, now, now, entry_id),
    )
    if cursor.rowcount != 1:
        conn.rollback()
        _remove_download_file(staging)
        return False
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
            input_format=input_format,
            output_container=container,
        )
        failure_reason = "ffmpeg_failed"
    if not success:
        if not source_container:
            _remove_download_file(staging)
        if is_cancelled():
            return False
        conn.execute(
            "UPDATE downloads SET status='failed', fail_reason=?, "
            "failed_at=?, updated_at=?, retry_count=retry_count+1 "
            "WHERE entry_id=? AND status='downloading'",
            (failure_reason, now, now, entry_id),
        )
        conn.commit()
        return False

    if is_cancelled():
        _remove_download_file(staging)
        return False

    # Finalization and cancellation signaling share a lock so the last-chunk
    # race either cancels staging or cleanly deletes an already-completed file.
    with _cancel_lock:
        if is_cancelled():
            _remove_download_file(staging)
            return False
        try:
            os.replace(staging, offline_path)
        except OSError as exc:
            if is_cancelled():
                _remove_download_file(staging)
                _remove_download_file(offline_path)
                return False
            conn.execute(
                "UPDATE downloads SET status='failed', fail_reason='finalize_failed', "
                "offline_path=?, failed_at=?, updated_at=?, "
                "retry_count=retry_count+1 "
                "WHERE entry_id=? AND status='downloading'",
                (offline_path, now, now, entry_id),
            )
            conn.commit()
            logger.error(
                "[DOWNLOADS] Could not finalize media into VOD tree (%s)",
                exc,
            )
            return False

        try:
            file_size = os.path.getsize(offline_path)
        except OSError:
            if is_cancelled():
                _remove_download_file(offline_path)
                return False
            raise
        cursor = conn.execute(
            "UPDATE downloads SET status='completed', local_path=?, offline_path=?, container=?, "
            "file_size=?, expected_size=COALESCE(expected_size, ?), "
            "downloaded_bytes=?, staging_path=NULL, completed_at=?, updated_at=? "
            "WHERE entry_id=? AND status='downloading'",
            (offline_path, offline_path, container, file_size, file_size, file_size, now, now, entry_id),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            _remove_download_file(offline_path)
            return False
        conn.commit()
    logger.info(
        "[DOWNLOADS] Completed — entry=%s offline=%s size=%d",
        entry_id[:12], offline_path, file_size,
    )
    return True


@task("process_downloads")
def process_downloads(force: bool = False) -> None:
    """Wake up and drain pending downloads until the queue is empty.

    When force=True, skip the enabled check (manual trigger from the UI).
    """
    global _active_count
    restart_if_pending = False

    with _lock:
        if _active_count > 0:
            logger.debug("[DOWNLOADS] Already processing, skipping this cycle")
            return
        _active_count = 1

    try:
        reset_stuck_downloads()

        with get_db() as conn:
            settings = _load_settings(conn)
            max_concurrent = settings["max_concurrent"]

            if max_concurrent < 1:
                logger.warning("[DOWNLOADS] max_concurrent is %d, skipping", max_concurrent)
                return

            if not force and not settings.get("enabled", False):
                logger.debug("[DOWNLOADS] Integration is disabled, skipping")
                return
            restart_if_pending = True

        os.makedirs(_offline_root(), exist_ok=True)

        while True:
            with get_db() as conn:
                pending = conn.execute(
                    "SELECT entry_id FROM downloads WHERE status='pending' "
                    "ORDER BY queued_at LIMIT ?",
                    (max_concurrent,),
                ).fetchall()

            if not pending:
                logger.debug("[DOWNLOADS] Pending queue drained")
                break

            logger.info("[DOWNLOADS] Processing %d pending download(s)", len(pending))
            for row in pending:
                entry_id = row["entry_id"]
                cancel_event = threading.Event()
                with _cancel_lock:
                    _cancel_events[entry_id] = cancel_event
                try:
                    with get_db() as conn:
                        dl_row = conn.execute(
                            "SELECT * FROM downloads "
                            "WHERE entry_id=? AND status='pending'",
                            (entry_id,),
                        ).fetchone()
                        if not dl_row:
                            continue
                        settings = _load_settings(conn)
                        _process_one(
                            conn, dl_row, settings, cancel_event=cancel_event
                        )
                except Exception as exc:
                    logger.error(
                        "[DOWNLOADS] Error processing entry=%s: %s",
                        entry_id[:12],
                        exc,
                        exc_info=True,
                    )
                    try:
                        with get_db() as conn:
                            if not cancel_event.is_set():
                                conn.execute(
                                    "UPDATE downloads SET status='failed', fail_reason=?, "
                                    "failed_at=?, updated_at=? WHERE entry_id=? "
                                    "AND status IN ('pending','probing','downloading')",
                                    (
                                        str(exc)[:200],
                                        local_now_iso(),
                                        local_now_iso(),
                                        entry_id,
                                    ),
                                )
                                conn.commit()
                    except Exception:
                        pass
                finally:
                    with _cancel_lock:
                        if _cancel_events.get(entry_id) is cancel_event:
                            _cancel_events.pop(entry_id, None)

        # Trigger STRM regeneration after the queue is drained.
        try:
            from app.tasks.strm import generate_strm
            generate_strm()
        except Exception as exc:
            logger.error(
                "[DOWNLOADS] generate_strm after processing failed: %s",
                exc,
                exc_info=True,
            )
    finally:
        with _lock:
            _active_count = 0
        if restart_if_pending:
            try:
                with get_db() as conn:
                    has_pending = conn.execute(
                        "SELECT 1 FROM downloads WHERE status='pending' LIMIT 1"
                    ).fetchone()
                if has_pending:
                    logger.debug(
                        "[DOWNLOADS] New work arrived while processor was exiting"
                    )
                    _kick_processor()
            except Exception:
                logger.exception(
                    "[DOWNLOADS] Could not check for pending work after processor exit"
                )


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
            paths = {
                path
                for path in (
                    row["local_path"], row["offline_path"], row["staging_path"]
                )
                if path
            }
            for path in paths:
                _remove_download_file(path)

        if row["status"] in ("pending", "probing", "downloading"):
            conn.execute(
                "UPDATE downloads SET status='cancelled', fail_reason='cancelled_by_user', "
                "local_path=NULL, offline_path=NULL, staging_path=NULL, "
                "file_size=NULL, completed_at=NULL, cancelled_at=?, updated_at=? "
                "WHERE entry_id=?",
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


def reset_stuck_downloads(max_age_seconds: int | None = 300) -> int:
    """Reset downloads whose worker is gone or whose progress lease expired."""
    now = local_now_iso()
    with get_db() as conn:
        if max_age_seconds is None:
            cursor = conn.execute(
                "UPDATE downloads SET status='pending', updated_at=? "
                "WHERE status IN ('probing','downloading')",
                (now,),
            )
        else:
            stale_modifier = f"-{max(1, max_age_seconds)} seconds"
            cursor = conn.execute(
                "UPDATE downloads SET status='pending', updated_at=? "
                "WHERE status IN ('probing','downloading') "
                "AND (updated_at IS NULL OR datetime(updated_at) <= datetime('now', ?))",
                (now, stale_modifier),
            )
        count = cursor.rowcount
        if count:
            conn.commit()
            logger.info("[DOWNLOADS] Reset %d stuck download(s) to pending", count)
        return count


def resume_downloads_on_startup() -> None:
    """Reclaim work from the previous web process and restart the queue."""
    reset_stuck_downloads(max_age_seconds=None)
    _kick_processor()

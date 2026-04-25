"""
Live TV M3U writer.

Generates per-provider .m3u files and a combined all_providers.m3u in
data/vod/livetv/ for all live (type='live') streams.

Per-provider files
──────────────────
generate_all providers:
    All non-excluded live streams from that provider are written to
    <livetv_dir>/<provider_slug>.m3u unconditionally.

import_selected providers:
    Only streams where imported=1 are written to
    <livetv_dir>/<provider_slug>.m3u.

Combined file
─────────────
all_providers.m3u is the union of all eligible live streams across every
active provider (both modes, respecting imported for import_selected).
Streams from multiple providers for the same channel are all included —
deduplication is left to the user's IPTV player.

Each EXTINF line is reconstructed from the raw attributes stored in
streams.metadata_json at ingest time, preserving tvg-id, tvg-logo,
group-title, etc. exactly as the provider supplied them.

deactivate_provider_live_m3u(provider_slug)
────────────────────────────────────────────
Deletes <livetv_dir>/<provider_slug>.m3u when a provider is deactivated
or deleted, then rewrites all_providers.m3u to reflect the change.
"""
import json
import logging
import os

from app.database import get_db
from app.tasks.base import task
from app.utils.env import resolve_path

logger = logging.getLogger("app.tasks.live_m3u")

_LIVETV_DIR_RELATIVE = os.path.join(os.getenv("VOD_DIR", "data/vod"), "livetv")

_ALL_PROVIDERS_FILENAME = "all_providers.m3u"

# Keys from metadata_json that we don't want to put back in EXTINF attributes
_SKIP_ATTR_KEYS = {
    "duration", "name", "stream_url", "raw_title", "cleaned_title",
    "type", "season", "episode", "air_date", "year", "series_type",
    "entry_id", "extgrp",
}


def _livetv_dir() -> str:
    return resolve_path(_LIVETV_DIR_RELATIVE)


def _provider_m3u_path(provider_slug: str) -> str:
    return os.path.join(_livetv_dir(), f"{provider_slug}.m3u")


def _all_providers_path() -> str:
    return os.path.join(_livetv_dir(), _ALL_PROVIDERS_FILENAME)


def _build_extinf(stream_url: str, metadata_json: str | None, raw_title: str) -> str:
    """
    Reconstruct a pair of M3U lines (#EXTINF + URL) from stored metadata.
    Attributes are restored from metadata_json; the display name is the
    raw_title stored in metadata_json if available, otherwise raw_title arg.
    """
    meta: dict = {}
    if metadata_json:
        try:
            meta = json.loads(metadata_json)
        except (ValueError, TypeError):
            pass

    # Use original name from metadata if present
    name = meta.get("name") or raw_title

    # Reconstruct attribute string from all non-internal keys
    attrs = {k: v for k, v in meta.items() if k not in _SKIP_ATTR_KEYS and isinstance(v, str)}
    attr_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())

    extinf = f"#EXTINF:-1 {attr_str},{name}" if attr_str else f"#EXTINF:-1 {name}"
    return f"{extinf}\n{stream_url}"


def _write_m3u(path: str, lines: list[str], provider_slug: str) -> None:
    """Write lines to a .m3u file, creating directories as needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("#EXTM3U\n")
        for line in lines:
            fh.write(line + "\n")
    logger.info("[LIVE_M3U] Wrote %d entries → %s (%s)", len(lines) // 2, path, provider_slug)


def _delete_m3u(path: str, provider_slug: str) -> None:
    try:
        os.remove(path)
        logger.info("[LIVE_M3U] Deleted %s (%s)", path, provider_slug)
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning("[LIVE_M3U] Could not delete %s: %s", path, exc)


def _generate_provider_m3u(conn, provider_slug: str, strm_mode: str) -> list[str]:
    """
    Build the line list for a single provider's m3u.
    Returns a flat list of alternating #EXTINF and URL lines (no #EXTM3U header).
    """
    if strm_mode == "generate_all":
        rows = conn.execute(
            """
            SELECT e.raw_title, s.stream_url, s.metadata_json
            FROM streams s
            JOIN entries e ON e.entry_id = s.entry_id
            WHERE e.type = 'live'
              AND s.provider = ?
              AND s.exclude = 0
            ORDER BY e.cleaned_title
            """,
            (provider_slug,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT e.raw_title, s.stream_url, s.metadata_json
            FROM streams s
            JOIN entries e ON e.entry_id = s.entry_id
            WHERE e.type = 'live'
              AND s.provider = ?
              AND s.imported = 1
              AND s.exclude = 0
            ORDER BY e.cleaned_title
            """,
            (provider_slug,),
        ).fetchall()

    lines: list[str] = []
    for row in rows:
        lines.append(_build_extinf(row["stream_url"], row["metadata_json"], row["raw_title"] or ""))
    return lines


def _generate_all_providers_m3u(conn) -> list[str]:
    """
    Build the combined m3u line list across all active providers.
    generate_all providers contribute all non-excluded live streams.
    import_selected providers contribute only imported=1 live streams.
    """
    rows = conn.execute(
        """
        SELECT e.raw_title, s.stream_url, s.metadata_json
        FROM streams s
        JOIN entries e ON e.entry_id = s.entry_id
        JOIN providers p ON p.slug = s.provider
        WHERE e.type = 'live'
          AND p.is_active = 1
          AND s.exclude = 0
          AND (
              p.strm_mode = 'generate_all'
              OR (p.strm_mode = 'import_selected' AND s.imported = 1)
          )
        ORDER BY p.priority, p.slug, e.cleaned_title
        """
    ).fetchall()

    lines: list[str] = []
    for row in rows:
        lines.append(_build_extinf(row["stream_url"], row["metadata_json"], row["raw_title"] or ""))
    return lines


@task("generate_live_m3u")
def generate_live_m3u() -> None:
    """
    Rewrite all per-provider and the combined all_providers.m3u in livetv/.
    Only active providers are processed; inactive providers' files are left
    as-is (use deactivate_provider_live_m3u to clean them up on deactivation).
    """
    with get_db() as conn:
        providers = conn.execute(
            "SELECT slug, strm_mode FROM providers WHERE is_active = 1"
        ).fetchall()

        for p in providers:
            lines = _generate_provider_m3u(conn, p["slug"], p["strm_mode"])
            path = _provider_m3u_path(p["slug"])
            if lines:
                _write_m3u(path, lines, p["slug"])
            else:
                # No eligible live streams — remove stale file if present
                _delete_m3u(path, p["slug"])

        all_lines = _generate_all_providers_m3u(conn)

    all_path = _all_providers_path()
    if all_lines:
        _write_m3u(all_path, all_lines, "all_providers")
    else:
        _delete_m3u(all_path, "all_providers")


def deactivate_provider_live_m3u(provider_slug: str) -> None:
    """
    Remove the provider's m3u file and rewrite all_providers.m3u.
    Called when a provider is deactivated or deleted.
    """
    _delete_m3u(_provider_m3u_path(provider_slug), provider_slug)
    # Rewrite combined file without this provider's streams
    try:
        with get_db() as conn:
            all_lines = _generate_all_providers_m3u(conn)
        all_path = _all_providers_path()
        if all_lines:
            _write_m3u(all_path, all_lines, "all_providers")
        else:
            _delete_m3u(all_path, "all_providers")
    except Exception as exc:
        logger.error("[LIVE_M3U] Failed to rewrite all_providers after deactivation: %s", exc, exc_info=True)


def deactivate_provider_live_m3u_async(provider_slug: str) -> None:
    """Thread-safe wrapper for use in async toggle handlers."""
    import threading
    threading.Thread(
        target=deactivate_provider_live_m3u, args=(provider_slug,), daemon=True
    ).start()

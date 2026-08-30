"""Native Xtream Player API ingestion for providers without M3U export."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from typing import Any, Callable, Mapping
from urllib.parse import quote, urlsplit

import requests

from app.database import get_db
from app.ingestion.parser import _build_metadata_json, _clean_name, _make_entry_id
from app.ingestion.sync import persist_entries, run_sync
from app.utils.env import local_now, local_now_iso

logger = logging.getLogger("app.ingestion.xtream_native")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}
_YEAR_RE = re.compile(r"\b(?:19\d{2}|20\d{2})\b")
_EPISODE_POSITION_RE = re.compile(r"\bS(\d{1,3})\s*E(\d{1,4})\b", re.IGNORECASE)
_EXTENSION_RE = re.compile(r"^[a-zA-Z0-9]{1,8}$")
_PATH_SEGMENT_SAFE = "!$&'()*+,;=:@"


class XtreamAPIError(RuntimeError):
    """A redacted Player API failure suitable for application logs."""


def _emit(
    callback: Callable[..., None] | None,
    *,
    phase: str,
    message: str,
    current: int | None = None,
    total: int | None = None,
    **details: Any,
) -> None:
    if callback:
        callback(
            phase=phase,
            message=message,
            current=current,
            total=total,
            **details,
        )


def _provider_base(provider: Mapping[str, Any]) -> str:
    server = str(provider.get("url") or "").rstrip("/")
    if not server:
        raise XtreamAPIError("provider server URL is empty")

    port = str(provider.get("port") or "").strip()
    try:
        parsed = urlsplit(server)
        has_port = parsed.port is not None
    except ValueError as exc:
        raise XtreamAPIError("provider server URL is invalid") from exc

    return f"{server}:{port}" if port and not has_port else server


def _safe_extension(value: Any, fallback: str) -> str:
    extension = str(value or "").strip().lstrip(".")
    return extension if _EXTENSION_RE.fullmatch(extension) else fallback


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _quote_path_segment(value: Any) -> str:
    """Encode only characters that are not valid inside an RFC 3986 segment."""
    return quote(str(value), safe=_PATH_SEGMENT_SAFE)


class XtreamClient:
    def __init__(self, provider: Mapping[str, Any]) -> None:
        self.base = _provider_base(provider)
        self.username = str(provider.get("username") or "")
        self.password = str(provider.get("password") or "")
        if not self.username or not self.password:
            raise XtreamAPIError("provider credentials are incomplete")

    def get_json(self, action: str | None = None, **extra: Any) -> Any:
        params: dict[str, Any] = {
            "username": self.username,
            "password": self.password,
        }
        if action:
            params["action"] = action
        params.update(extra)

        try:
            response = requests.get(
                f"{self.base}/player_api.php",
                params=params,
                headers=_HEADERS,
                timeout=60,
            )
        except requests.RequestException as exc:
            raise XtreamAPIError(
                f"Player API request failed ({type(exc).__name__})"
            ) from exc

        if response.status_code != 200:
            raise XtreamAPIError(f"Player API returned HTTP {response.status_code}")
        try:
            return response.json()
        except ValueError as exc:
            raise XtreamAPIError("Player API returned invalid JSON") from exc

    def get_list(self, action: str) -> list[dict[str, Any]]:
        payload = self.get_json(action)
        if not isinstance(payload, list):
            raise XtreamAPIError(f"Player API action {action} returned an invalid payload")
        return [item for item in payload if isinstance(item, dict)]

    def authenticate(self) -> None:
        payload = self.get_json()
        if not isinstance(payload, dict):
            raise XtreamAPIError("Player API authentication returned an invalid payload")
        user_info = payload.get("user_info")
        if not isinstance(user_info, dict) or str(user_info.get("auth")) != "1":
            raise XtreamAPIError("Player API rejected the provider credentials")


def _category_map(client: XtreamClient, action: str) -> dict[str, str]:
    try:
        categories = client.get_list(action)
    except XtreamAPIError as exc:
        logger.warning("[XTREAM API] Optional category request failed: %s", exc)
        return {}
    return {
        str(item.get("category_id")): str(item.get("category_name") or "")
        for item in categories
        if item.get("category_id") is not None
    }


def _load_cache(conn: sqlite3.Connection, provider_slug: str) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM xtream_series_cache WHERE provider_slug = ?",
        (provider_slug,),
    ).fetchall()
    return {str(row["series_id"]): dict(row) for row in rows}


def _replace_series_catalog(
    conn: sqlite3.Connection,
    provider_slug: str,
    series_list: list[dict[str, Any]],
    updated_at: str,
) -> int:
    """Replace one provider's summaries after a successful get_series call."""
    rows: list[
        tuple[str, str, str, str, str, str, str | None, str, str]
    ] = []
    for series in series_list:
        series_id = str(series.get("series_id") or "").strip()
        series_name = str(series.get("name") or "").strip()
        if not series_id or not series_name:
            continue
        metadata = {
            key: value
            for key, value in series.items()
            if key not in {"series_id", "name", "cover", "category_id", "last_modified"}
        }
        rows.append(
            (
                provider_slug,
                series_id,
                series_name,
                series_name.casefold(),
                str(series.get("cover") or ""),
                str(series.get("category_id") or ""),
                str(series.get("last_modified"))
                if series.get("last_modified") is not None
                else None,
                json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
                updated_at,
            )
        )

    conn.execute(
        "DELETE FROM xtream_series_catalog WHERE provider_slug = ?",
        (provider_slug,),
    )
    conn.executemany(
        """
        INSERT INTO xtream_series_catalog (
            provider_slug, series_id, series_name, title_key, cover,
            category_id, last_modified, metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def refresh_series_catalog(provider: Mapping[str, Any]) -> int:
    """Fetch and persist all series summaries without loading any episodes."""
    provider_data = dict(provider)
    provider_slug = str(
        provider_data.get("slug") or provider_data.get("id") or "xtream"
    )
    client = XtreamClient(provider_data)
    client.authenticate()
    series_list = client.get_list("get_series")
    with get_db() as conn:
        count = _replace_series_catalog(
            conn, provider_slug, series_list, local_now_iso()
        )
    return count


def _normalize_episode_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("episodes"), dict):
        raise XtreamAPIError("series detail returned an invalid episode payload")

    normalized: list[dict[str, Any]] = []
    seen_positions: set[tuple[int, int]] = set()
    for season_key, episode_list in payload["episodes"].items():
        if not isinstance(episode_list, list):
            continue
        for episode in episode_list:
            if not isinstance(episode, dict):
                continue
            stream_id = str(episode.get("id") or "")
            season = _to_int(episode.get("season")) or _to_int(season_key)
            episode_number = _to_int(episode.get("episode_num"))
            title = str(episode.get("title") or "")
            title_position = _EPISODE_POSITION_RE.search(title)
            if title_position:
                season = int(title_position.group(1))
                episode_number = int(title_position.group(2))
            if not stream_id or season is None or episode_number is None:
                continue
            position = (season, episode_number)
            if position in seen_positions:
                continue
            seen_positions.add(position)
            info = episode.get("info") if isinstance(episode.get("info"), dict) else {}
            normalized.append(
                {
                    "id": stream_id,
                    "season": season,
                    "episode": episode_number,
                    "title": title,
                    "container_extension": _safe_extension(
                        episode.get("container_extension"), "mkv"
                    ),
                    "direct_source": str(episode.get("direct_source") or ""),
                    "cover": str(
                        info.get("movie_image") or info.get("cover_big") or ""
                    ),
                }
            )
    return normalized


def _upsert_cache_success(
    conn: sqlite3.Connection,
    provider_slug: str,
    series: Mapping[str, Any],
    episodes: list[dict[str, Any]],
    fetched_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO xtream_series_cache (
            provider_slug, series_id, series_name, last_modified,
            episodes_json, fetch_status, fetched_at
        ) VALUES (?, ?, ?, ?, ?, 'ok', ?)
        ON CONFLICT(provider_slug, series_id) DO UPDATE SET
            series_name = excluded.series_name,
            last_modified = excluded.last_modified,
            episodes_json = excluded.episodes_json,
            fetch_status = 'ok',
            fetched_at = excluded.fetched_at
        """,
        (
            provider_slug,
            str(series.get("series_id")),
            str(series.get("name") or "Untitled Series"),
            str(series.get("last_modified")) if series.get("last_modified") is not None else None,
            json.dumps(episodes, ensure_ascii=False, separators=(",", ":")),
            fetched_at,
        ),
    )


def _playback_url(
    client: XtreamClient,
    media_type: str,
    stream_id: Any,
    extension: str,
    direct_source: Any = None,
) -> str:
    source = str(direct_source or "").strip()
    if source:
        parsed = urlsplit(source)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            return source
    username = _quote_path_segment(client.username)
    password = _quote_path_segment(client.password)
    identifier = _quote_path_segment(stream_id)
    return f"{client.base}/{media_type}/{username}/{password}/{identifier}.{extension}"


def refresh_episode_stream(
    conn: sqlite3.Connection,
    entry_id: str,
    provider_slug: str,
) -> str | None:
    """Refresh one materialized episode URL when an Xtream stream ID rotates."""
    row = conn.execute(
        """
        SELECT e.season, e.episode, s.metadata_json, p.*
        FROM entries e
        JOIN streams s ON s.entry_id=e.entry_id AND s.provider=?
        JOIN providers p ON p.slug=s.provider
        WHERE e.entry_id=? AND e.type='series' AND p.type='xtream'
        """,
        (provider_slug, entry_id),
    ).fetchone()
    if not row:
        return None
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except (TypeError, ValueError):
        return None
    series_id = str(metadata.get("xtream-series-id") or "").strip()
    if not series_id:
        return None

    client = XtreamClient(dict(row))
    episodes = _normalize_episode_payload(
        client.get_json("get_series_info", series_id=series_id)
    )
    episode = next(
        (
            item for item in episodes
            if item["season"] == row["season"]
            and item["episode"] == row["episode"]
        ),
        None,
    )
    if not episode:
        return None

    extension = _safe_extension(episode.get("container_extension"), "mkv")
    stream_url = _playback_url(
        client,
        "series",
        episode["id"],
        extension,
        episode.get("direct_source"),
    )
    metadata.update({
        "xtream-id": str(episode["id"]),
        "container-extension": extension,
        "episode-title": str(episode.get("title") or ""),
    })
    conn.execute(
        "UPDATE streams SET stream_url=?, metadata_json=? "
        "WHERE entry_id=? AND provider=?",
        (stream_url, json.dumps(metadata, ensure_ascii=False), entry_id, provider_slug),
    )
    conn.commit()
    return stream_url


def _entry(
    *,
    entry_type: str,
    raw_title: str,
    cleaned_title: str,
    stream_url: str,
    provider_slug: str,
    batch_id: str,
    ingested_at: str,
    metadata: Mapping[str, Any],
    year: int | None = None,
    season: int | None = None,
    episode: int | None = None,
    series_type: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "type": entry_type,
        "raw_title": raw_title,
        "cleaned_title": cleaned_title,
        "stream_url": stream_url,
        "source_file": "xtream-player-api",
        "provider": provider_slug,
        "ingested_at": ingested_at,
        "batch_id": batch_id,
        "year": year,
        "season": season,
        "episode": episode,
        "series_type": series_type,
    }
    value.update(metadata)
    value["entry_id"] = _make_entry_id(value)
    value["metadata_json"] = _build_metadata_json(value)
    return value


def _movie_identity(item: Mapping[str, Any]) -> tuple[str, int | None]:
    name = str(item.get("name") or "Untitled Movie").strip()
    matches = list(_YEAR_RE.finditer(name))
    if not matches:
        return _clean_name(name), _to_int(item.get("year"))
    match = matches[-1]
    title = name[: match.start()].rstrip(" ([._-")
    return _clean_name(title), int(match.group(0))


def _build_series_entries(
    client: XtreamClient,
    provider_slug: str,
    series_catalog: list[dict[str, Any]],
    cache: dict[str, dict[str, Any]],
    series_categories: Mapping[str, str],
    batch_id: str,
    ingested_at: str,
) -> tuple[list[dict[str, Any]], int, set[str]]:
    entries: list[dict[str, Any]] = []
    active_series_ids: set[str] = set()
    ready_series = 0
    for series in series_catalog:
        series_id = str(series.get("series_id") or "")
        if not series_id:
            continue
        active_series_ids.add(series_id)
        cached = cache.get(series_id)
        if not cached:
            continue
        series_name = str(
            series.get("name") or cached.get("series_name") or ""
        ).strip()
        if not series_name:
            continue
        try:
            episodes = json.loads(cached.get("episodes_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(episodes, list):
            continue
        if cached.get("fetch_status") == "ok":
            ready_series += 1

        category_id = str(series.get("category_id") or "")
        series_cover = str(series.get("cover") or "")
        for episode_data in episodes:
            if not isinstance(episode_data, dict):
                continue
            stream_id = episode_data.get("id")
            season = _to_int(episode_data.get("season"))
            episode_number = _to_int(episode_data.get("episode"))
            if stream_id is None or season is None or episode_number is None:
                continue
            raw_title = f"{series_name} S{season:02d}E{episode_number:02d}"
            extension = _safe_extension(
                episode_data.get("container_extension"), "mkv"
            )
            entries.append(
                _entry(
                    entry_type="series",
                    raw_title=raw_title,
                    cleaned_title=_clean_name(series_name),
                    season=season,
                    episode=episode_number,
                    series_type="season_episode",
                    stream_url=_playback_url(
                        client,
                        "series",
                        stream_id,
                        extension,
                        episode_data.get("direct_source"),
                    ),
                    provider_slug=provider_slug,
                    batch_id=batch_id,
                    ingested_at=ingested_at,
                    metadata={
                        "duration": "0",
                        "tvg-name": raw_title,
                        "tvg-logo": str(episode_data.get("cover") or series_cover),
                        "group-title": series_categories.get(category_id, ""),
                        "xtream-id": str(stream_id),
                        "xtream-series-id": series_id,
                        "xtream-category-id": category_id,
                        "episode-title": str(episode_data.get("title") or ""),
                        "container-extension": extension,
                    },
                )
            )
    return entries, ready_series, active_series_ids


def ensure_series_loaded(title: str) -> int:
    """Fetch and materialize episode details for one known series title."""
    with get_db() as conn:
        already_loaded = conn.execute(
            """
            SELECT COUNT(*) FROM entries
            WHERE type='series' AND lower(cleaned_title)=lower(?)
            """,
            (title,),
        ).fetchone()[0]
        rows = conn.execute(
            """
            SELECT c.*, p.*
            FROM xtream_series_catalog c
            JOIN providers p ON p.slug=c.provider_slug
                        LEFT JOIN xtream_series_cache x
                            ON x.provider_slug=c.provider_slug AND x.series_id=c.series_id
            WHERE p.is_active=1 AND p.type='xtream'
              AND lower(c.series_name)=lower(?)
                                                        AND (
                                                                x.series_id IS NULL
                                                                OR x.fetch_status != 'ok'
                                                                OR datetime(x.fetched_at) <= datetime('now', '-1 hour')
                                                        )
            ORDER BY p.priority, p.slug, c.series_id
            """,
            (title,),
        ).fetchall()
        if not rows:
                        return already_loaded

        by_provider: dict[str, list[dict[str, Any]]] = {}
        providers: dict[str, dict[str, Any]] = {}
        for row in rows:
            value = dict(row)
            slug = value["provider_slug"]
            providers[slug] = value
            metadata = json.loads(value.get("metadata_json") or "{}")
            metadata.update(
                {
                    "series_id": value["series_id"],
                    "name": value["series_name"],
                    "cover": value.get("cover") or "",
                    "category_id": value.get("category_id") or "",
                    "last_modified": value.get("last_modified"),
                }
            )
            by_provider.setdefault(slug, []).append(metadata)

    total_entries = 0
    for provider_slug, summaries in by_provider.items():
        provider = providers[provider_slug]
        client = XtreamClient(provider)
        client.authenticate()
        fetched_at = local_now_iso()
        with get_db() as conn:
            for summary in summaries:
                details = client.get_json(
                    "get_series_info", series_id=str(summary["series_id"])
                )
                episodes = _normalize_episode_payload(details)
                _upsert_cache_success(
                    conn, provider_slug, summary, episodes, fetched_at
                )
            cache = _load_cache(conn, provider_slug)
            batch_id = hashlib.sha256(
                f"xtream-series-lazy:{provider_slug}:{title}:{fetched_at}".encode()
            ).hexdigest()
            entries, _, _ = _build_series_entries(
                client,
                provider_slug,
                summaries,
                cache,
                {},
                batch_id,
                fetched_at,
            )
            try:
                quality_terms = json.loads(provider.get("quality_terms") or "[]")
            except (json.JSONDecodeError, TypeError):
                quality_terms = []
            persist_entries(conn, entries, quality_terms=quality_terms)
            from app.filters.engine import load_filters, run_filters_for_provider

            run_filters_for_provider(conn, load_filters(conn), provider=provider_slug)
            if provider.get("strm_mode") == "import_selected":
                from app.ingestion.sync import apply_follow_rules

                apply_follow_rules(conn, provider_slug)
            total_entries += len(entries)

    if total_entries:
        from app.tasks.strm import generate_strm

        generate_strm()
    return total_entries


def build_parsed_result(
    provider: Mapping[str, Any],
    conn: sqlite3.Connection,
    client: XtreamClient | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    provider_slug = str(provider.get("slug") or provider.get("id") or "xtream")
    client = client or XtreamClient(provider)
    _emit(
        progress_callback,
        phase="authenticating",
        message="Authenticating with Xtream Player API",
    )
    client.authenticate()

    _emit(progress_callback, phase="catalog", message="Fetching live channels")
    live_catalog = client.get_list("get_live_streams")
    _emit(
        progress_callback,
        phase="catalog",
        message=f"Fetched {len(live_catalog):,} live channels; fetching movies",
    )
    vod_catalog = client.get_list("get_vod_streams")
    _emit(
        progress_callback,
        phase="catalog",
        message=f"Fetched {len(vod_catalog):,} movies; fetching series catalogue",
    )
    series_catalog = client.get_list("get_series")
    live_categories = _category_map(client, "get_live_categories")
    vod_categories = _category_map(client, "get_vod_categories")
    series_categories = _category_map(client, "get_series_categories")

    now = local_now()
    ingested_at = now.isoformat()
    batch_id = hashlib.sha256(
        f"xtream-player-api:{provider_slug}:{ingested_at}".encode()
    ).hexdigest()

    catalog_count = _replace_series_catalog(
        conn, provider_slug, series_catalog, ingested_at
    )
    logger.info(
        "[XTREAM API] Series summaries stored — provider=%s count=%d",
        provider_slug,
        catalog_count,
    )

    cache = _load_cache(conn, provider_slug)

    _emit(
        progress_callback,
        phase="normalizing",
        message="Preparing catalogue entries for synchronization",
        series_cached=sum(
            1 for row in cache.values() if row.get("fetch_status") == "ok"
        ),
        series_catalog=len(series_catalog),
    )

    live_entries: list[dict[str, Any]] = []
    movie_entries: list[dict[str, Any]] = []
    series_entries: list[dict[str, Any]] = []

    live_extension = "m3u8" if str(provider.get("stream_format") or "ts") == "hls" else "ts"
    for item in live_catalog:
        stream_id = item.get("stream_id")
        name = str(item.get("name") or "").strip()
        if stream_id is None or not name:
            continue
        category_id = str(item.get("category_id") or "")
        live_entries.append(
            _entry(
                entry_type="live",
                raw_title=name,
                cleaned_title=_clean_name(name),
                stream_url=_playback_url(
                    client,
                    "live",
                    stream_id,
                    live_extension,
                    item.get("direct_source"),
                ),
                provider_slug=provider_slug,
                batch_id=batch_id,
                ingested_at=ingested_at,
                metadata={
                    "duration": "-1",
                    "tvg-name": name,
                    "tvg-id": str(item.get("epg_channel_id") or ""),
                    "tvg-logo": str(item.get("stream_icon") or ""),
                    "group-title": live_categories.get(category_id, ""),
                    "xtream-id": str(stream_id),
                    "xtream-category-id": category_id,
                },
            )
        )

    for item in vod_catalog:
        stream_id = item.get("stream_id")
        name = str(item.get("name") or "").strip()
        if stream_id is None or not name:
            continue
        title, year = _movie_identity(item)
        extension = _safe_extension(item.get("container_extension"), "mkv")
        category_id = str(item.get("category_id") or "")
        movie_entries.append(
            _entry(
                entry_type="movie",
                raw_title=name,
                cleaned_title=title,
                year=year,
                stream_url=_playback_url(
                    client,
                    "movie",
                    stream_id,
                    extension,
                    item.get("direct_source"),
                ),
                provider_slug=provider_slug,
                batch_id=batch_id,
                ingested_at=ingested_at,
                metadata={
                    "duration": "0",
                    "tvg-name": name,
                    "tvg-logo": str(item.get("stream_icon") or ""),
                    "group-title": vod_categories.get(category_id, ""),
                    "xtream-id": str(stream_id),
                    "xtream-category-id": category_id,
                    "container-extension": extension,
                },
            )
        )

    series_entries, ready_series, active_series_ids = _build_series_entries(
        client,
        provider_slug,
        series_catalog,
        cache,
        series_categories,
        batch_id,
        ingested_at,
    )

    total_entries = len(live_entries) + len(movie_entries) + len(series_entries)
    pending_series = max(len(active_series_ids) - ready_series, 0)
    logger.info(
        "[XTREAM API] Catalog normalized — provider=%s live=%d movies=%d episodes=%d series_loaded_on_demand=%d/%d pending=%d",
        provider_slug,
        len(live_entries),
        len(movie_entries),
        len(series_entries),
        ready_series,
        len(active_series_ids),
        pending_series,
    )

    return {
        "provider": provider_slug,
        "movies": movie_entries,
        "series": series_entries,
        "live_tv": live_entries,
        "tv_vod": [],
        "unsorted": [],
        "batch_id": batch_id,
        "summary": {
            "stats": {
                "entries_completed": total_entries,
                "movie": len(movie_entries),
                "series": len(series_entries),
                "live": len(live_entries),
                "tv_vod": 0,
                "unsorted": 0,
                "errors": 0,
                "series_catalog": len(active_series_ids),
                "series_ready": ready_series,
                "series_pending": pending_series,
            },
            "error_buckets": {},
        },
    }


def ingest_native_provider(
    provider: Mapping[str, Any],
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Fetch a Player API catalogue and run it through the normal sync pipeline."""
    provider_data = dict(provider)
    with get_db() as cache_conn:
        parsed = build_parsed_result(
            provider_data,
            cache_conn,
            progress_callback=progress_callback,
        )
    with get_db() as sync_conn:
        sync_summary = run_sync(
            sync_conn,
            parsed,
            progress_callback=progress_callback,
        )
    return {
        "parse": parsed["summary"],
        "sync": sync_summary,
    }
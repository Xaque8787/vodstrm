"""
TMDB enrichment task.

Enriches entries of type 'series' and 'movie' with metadata from The Movie
Database API. Runs asynchronously in a daemon thread after each ingest
completes and can also be triggered manually from the Integrations page.

Concurrency: a module-level flag prevents overlapping runs. If a trigger
arrives while enrichment is already running, it is dropped and logged — the
next scheduled ingest will call trigger_tmdb_enrichment() again and pick up
any entries that were missed.
"""
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import json

from app.database import get_db
from app.utils.env import local_now_iso

logger = logging.getLogger("app.tasks.tmdb")

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

_tmdb_running = False
_tmdb_running_lock = threading.Lock()

# ── Token-bucket rate limiter ─────────────────────────────────────────────


class _TokenBucket:
    def __init__(self, rate: float):
        self._rate = rate          # tokens per second
        self._tokens = rate
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def consume(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            if self._tokens >= 1:
                self._tokens -= 1
            else:
                wait = (1 - self._tokens) / self._rate
                time.sleep(wait)
                self._tokens = 0


_bucket = _TokenBucket(rate=3.0)  # 3 req/s — well within TMDB free tier


# ── Settings helpers ──────────────────────────────────────────────────────


def _get_tmdb_settings() -> dict:
    with get_db() as conn:
        row = conn.execute(
            "SELECT settings FROM integrations WHERE slug = 'tmdb'"
        ).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row["settings"] or "{}")
    except (ValueError, TypeError):
        return {}


def _tmdb_enabled() -> bool:
    return bool(_get_tmdb_settings().get("enabled", False))


def _tmdb_api_key() -> str:
    return (_get_tmdb_settings().get("api_key") or "").strip()


def _tmdb_language() -> str:
    return (_get_tmdb_settings().get("language") or "en-US").strip()


# ── TMDB HTTP helpers ─────────────────────────────────────────────────────


def _tmdb_get(path: str, params: dict) -> dict:
    """Single TMDB API GET with rate limiting and basic retry on 5xx."""
    api_key = _tmdb_api_key()
    params = {"api_key": api_key, "language": _tmdb_language(), **params}
    url = "https://api.themoviedb.org/3" + path + "?" + urllib.parse.urlencode(params)

    for attempt in range(3):
        _bucket.consume()
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise RuntimeError(f"TMDB auth error {exc.code} — check TMDB_API_KEY") from exc
            if exc.code == 429:
                retry_after = int(exc.headers.get("Retry-After", "10"))
                logger.warning("[TMDB] Rate limited — waiting %ds", retry_after)
                time.sleep(retry_after)
                continue
            if exc.code >= 500 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception as exc:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise

    raise RuntimeError(f"TMDB request failed after retries: {path}")


# ── Cleanup ───────────────────────────────────────────────────────────────


def cleanup_tmdb_orphans(conn) -> None:
    """Remove TMDB cache rows whose entries no longer exist."""
    conn.execute("""
        DELETE FROM tmdb_seasons
        WHERE tmdb_id NOT IN (
            SELECT DISTINCT tmdb_id FROM entries WHERE tmdb_type = 'show'
        )
    """)
    conn.execute("""
        DELETE FROM tmdb_shows
        WHERE tmdb_id NOT IN (SELECT DISTINCT tmdb_id FROM tmdb_seasons)
    """)
    conn.execute("""
        DELETE FROM tmdb_movies
        WHERE tmdb_id NOT IN (
            SELECT DISTINCT tmdb_id FROM entries WHERE tmdb_type = 'movie'
        )
    """)
    # Reset tmdb_id/tmdb_type on entries that lost all streams (orphans)
    conn.execute("""
        UPDATE entries SET tmdb_id = NULL, tmdb_type = NULL
        WHERE entry_id NOT IN (SELECT DISTINCT entry_id FROM streams)
          AND tmdb_id IS NOT NULL
    """)


def clear_tmdb_metadata(conn) -> None:
    """Wipe all TMDB data — entries revert to tvg-logo / placeholder."""
    conn.execute("UPDATE entries SET tmdb_id = NULL, tmdb_type = NULL")
    conn.execute("DELETE FROM tmdb_movies")
    conn.execute("DELETE FROM tmdb_shows")
    conn.execute("DELETE FROM tmdb_seasons")
    conn.execute("DELETE FROM tmdb_run_log")


# ── Cover art cascade helper ──────────────────────────────────────────────


def resolve_cover_art(conn, entry_id: str, entry_tmdb_id, entry_tmdb_type: str) -> str | None:
    """
    Resolve cover art for an entry using the three-level cascade:
      1. tvg-logo from any stream's metadata_json
      2. TMDB poster (via tmdb_id / tmdb_type)
      3. None  →  caller renders placeholder
    """
    # Level 1 — provider-supplied tvg-logo
    streams = conn.execute(
        "SELECT metadata_json FROM streams WHERE entry_id = ?", (entry_id,)
    ).fetchall()
    for s in streams:
        try:
            meta = json.loads(s["metadata_json"] or "{}")
        except (ValueError, TypeError):
            meta = {}
        logo = (meta.get("tvg-logo") or "").strip()
        if logo:
            return logo

    # Level 2 — TMDB poster
    if entry_tmdb_id:
        if entry_tmdb_type == "show":
            row = conn.execute(
                "SELECT poster_path FROM tmdb_shows WHERE tmdb_id = ?", (entry_tmdb_id,)
            ).fetchone()
        elif entry_tmdb_type == "movie":
            row = conn.execute(
                "SELECT poster_path FROM tmdb_movies WHERE tmdb_id = ?", (entry_tmdb_id,)
            ).fetchone()
        else:
            row = None
        if row and row["poster_path"]:
            return f"{TMDB_IMAGE_BASE}{row['poster_path']}"

    return None


# ── Enrichment ────────────────────────────────────────────────────────────


_TRAILING_YEAR_RE = re.compile(r"\s+((?:19|20)\d{2})$")
_COLON_RE = re.compile(r"\s*:\s*")


def _normalize_query(title: str) -> str:
    """Replace colons with a space and collapse whitespace for TMDB queries."""
    return _COLON_RE.sub(" ", title).strip()


def _search_show(title: str, year: int | None) -> dict | None:
    query = _normalize_query(title)

    # Attempt (a): if cleaned_title has a trailing year suffix, strip it and
    # pass as first_air_date_year so TMDB receives "Castle" + year=2009 rather
    # than the literal query "Castle 2009" which matches nothing.
    m = _TRAILING_YEAR_RE.search(query)
    if m:
        stripped = query[: m.start()]
        extracted_year = int(m.group(1))
        data = _tmdb_get("/search/tv", {"query": stripped, "first_air_date_year": extracted_year})
        results = data.get("results") or []
        if results:
            return results[0]
        # Attempt (b): same stripped title, no year filter
        data = _tmdb_get("/search/tv", {"query": stripped})
        results = data.get("results") or []
        if results:
            return results[0]
        # Attempt (c): normalized full title, no year filter
        data = _tmdb_get("/search/tv", {"query": query})
        results = data.get("results") or []
        return results[0] if results else None

    # No trailing year — single call
    params: dict = {"query": query}
    if year:
        params["first_air_date_year"] = year
    data = _tmdb_get("/search/tv", params)
    results = data.get("results") or []
    return results[0] if results else None


def _search_movie(title: str, year: int | None) -> dict | None:
    params: dict = {"query": _normalize_query(title)}
    if year:
        params["year"] = year
    data = _tmdb_get("/search/movie", params)
    results = data.get("results") or []
    return results[0] if results else None


def _fetch_show_seasons(tmdb_id: int) -> list[dict]:
    data = _tmdb_get(f"/tv/{tmdb_id}", {})
    return data.get("seasons") or []


def _upsert_show(conn, result: dict, season_data: list[dict]) -> None:
    now = local_now_iso()
    conn.execute("""
        INSERT INTO tmdb_shows (tmdb_id, tmdb_title, poster_path, first_air_date, overview, cached_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_id) DO UPDATE SET
            tmdb_title     = excluded.tmdb_title,
            poster_path    = excluded.poster_path,
            first_air_date = excluded.first_air_date,
            overview       = excluded.overview,
            cached_at      = excluded.cached_at
    """, (
        result["id"],
        result.get("name") or result.get("original_name"),
        result.get("poster_path"),
        result.get("first_air_date"),
        result.get("overview"),
        now,
    ))
    for s in season_data:
        snum = s.get("season_number")
        if snum is None:
            continue
        conn.execute("""
            INSERT INTO tmdb_seasons (tmdb_id, season_number, episode_count, poster_path)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tmdb_id, season_number) DO UPDATE SET
                episode_count = excluded.episode_count,
                poster_path   = excluded.poster_path
        """, (result["id"], snum, s.get("episode_count"), s.get("poster_path")))


def _upsert_movie(conn, result: dict) -> None:
    now = local_now_iso()
    conn.execute("""
        INSERT INTO tmdb_movies (tmdb_id, tmdb_title, poster_path, release_date, overview, cached_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_id) DO UPDATE SET
            tmdb_title   = excluded.tmdb_title,
            poster_path  = excluded.poster_path,
            release_date = excluded.release_date,
            overview     = excluded.overview,
            cached_at    = excluded.cached_at
    """, (
        result["id"],
        result.get("title") or result.get("original_title"),
        result.get("poster_path"),
        result.get("release_date"),
        result.get("overview"),
        now,
    ))


def _run_enrichment(triggered_by: str) -> None:
    global _tmdb_running
    started = time.monotonic()
    run_at = local_now_iso()

    entries_checked = 0
    api_calls_made = 0
    enriched = 0
    cache_hits = 0
    errors = 0
    error_detail = None

    try:
        with get_db() as conn:
            cleanup_tmdb_orphans(conn)

        # Fetch unenriched entries that haven't been marked as not-found
        with get_db() as conn:
            pending = conn.execute("""
                SELECT entry_id, cleaned_title, season, type, year
                FROM entries
                WHERE type IN ('series', 'movie')
                  AND tmdb_id IS NULL
                  AND tmdb_skipped_at IS NULL
            """).fetchall()

        entries_checked = len(pending)
        if not entries_checked:
            logger.info("[TMDB] No unenriched entries — done")
            return

        logger.info("[TMDB] Enriching %d entries (triggered_by=%s)", entries_checked, triggered_by)

        # Group series by (cleaned_title, year) to minimise API calls
        series_groups: dict[tuple, list] = {}
        movie_rows: list = []
        for row in pending:
            if row["type"] == "series":
                key = (row["cleaned_title"], row["year"])
                series_groups.setdefault(key, []).append(row)
            else:
                movie_rows.append(row)

        # ── Series ────────────────────────────────────────────────────────
        for (title, year), group_rows in series_groups.items():
            try:
                # Check cache by tmdb_id already stored for same title
                with get_db() as conn:
                    cached = conn.execute("""
                        SELECT tmdb_id FROM entries
                        WHERE cleaned_title = ? AND tmdb_type = 'show' AND tmdb_id IS NOT NULL
                        LIMIT 1
                    """, (title,)).fetchone()

                if cached:
                    tmdb_id = cached["tmdb_id"]
                    cache_hits += 1
                else:
                    result = _search_show(title, year)
                    api_calls_made += 1
                    if not result:
                        logger.debug("[TMDB] No show result for %r", title)
                        entry_ids = [r["entry_id"] for r in group_rows]
                        skipped_at = local_now_iso()
                        with get_db() as conn:
                            conn.executemany(
                                "UPDATE entries SET tmdb_skipped_at = ? WHERE entry_id = ?",
                                [(skipped_at, eid) for eid in entry_ids],
                            )
                        continue
                    tmdb_id = result["id"]
                    season_data = _fetch_show_seasons(tmdb_id)
                    api_calls_made += 1
                    with get_db() as conn:
                        _upsert_show(conn, result, season_data)

                entry_ids = [r["entry_id"] for r in group_rows]
                with get_db() as conn:
                    conn.executemany(
                        "UPDATE entries SET tmdb_id = ?, tmdb_type = 'show' WHERE entry_id = ?",
                        [(tmdb_id, eid) for eid in entry_ids],
                    )
                enriched += len(entry_ids)

            except RuntimeError as exc:
                # Auth errors — abort entirely
                error_detail = str(exc)
                logger.error("[TMDB] Fatal: %s", error_detail)
                errors += 1
                return
            except Exception as exc:
                error_detail = str(exc)
                logger.warning("[TMDB] Failed to enrich show %r: %s", title, exc)
                errors += 1

        # ── Movies ────────────────────────────────────────────────────────
        for row in movie_rows:
            title = row["cleaned_title"]
            year = row["year"]
            try:
                with get_db() as conn:
                    cached = conn.execute("""
                        SELECT tmdb_id FROM entries
                        WHERE cleaned_title = ? AND tmdb_type = 'movie' AND tmdb_id IS NOT NULL
                        LIMIT 1
                    """, (title,)).fetchone()

                if cached:
                    tmdb_id = cached["tmdb_id"]
                    cache_hits += 1
                else:
                    result = _search_movie(title, year)
                    api_calls_made += 1
                    if not result:
                        logger.debug("[TMDB] No movie result for %r", title)
                        with get_db() as conn:
                            conn.execute(
                                "UPDATE entries SET tmdb_skipped_at = ? WHERE entry_id = ?",
                                (local_now_iso(), row["entry_id"]),
                            )
                        continue
                    tmdb_id = result["id"]
                    with get_db() as conn:
                        _upsert_movie(conn, result)

                with get_db() as conn:
                    conn.execute(
                        "UPDATE entries SET tmdb_id = ?, tmdb_type = 'movie' WHERE entry_id = ?",
                        (tmdb_id, row["entry_id"]),
                    )
                enriched += 1

            except RuntimeError as exc:
                error_detail = str(exc)
                logger.error("[TMDB] Fatal: %s", error_detail)
                errors += 1
                return
            except Exception as exc:
                error_detail = str(exc)
                logger.warning("[TMDB] Failed to enrich movie %r: %s", title, exc)
                errors += 1

        logger.info(
            "[TMDB] Enrichment complete — checked=%d enriched=%d cache_hits=%d api_calls=%d errors=%d",
            entries_checked, enriched, cache_hits, api_calls_made, errors,
        )

    except Exception as exc:
        error_detail = str(exc)
        logger.error("[TMDB] Enrichment run failed: %s", exc, exc_info=True)
        errors += 1

    finally:
        duration = time.monotonic() - started
        try:
            with get_db() as conn:
                conn.execute("""
                    INSERT INTO tmdb_run_log
                        (run_at, triggered_by, entries_checked, api_calls_made,
                         enriched, cache_hits, errors, error_detail, duration_seconds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (run_at, triggered_by, entries_checked, api_calls_made,
                      enriched, cache_hits, errors, error_detail, round(duration, 2)))
                # Keep last 10 rows only
                conn.execute("""
                    DELETE FROM tmdb_run_log
                    WHERE id NOT IN (
                        SELECT id FROM tmdb_run_log ORDER BY id DESC LIMIT 10
                    )
                """)
        except Exception as log_exc:
            logger.warning("[TMDB] Could not write run log: %s", log_exc)

        with _tmdb_running_lock:
            _tmdb_running = False


def trigger_tmdb_enrichment(triggered_by: str = "manual") -> bool:
    """
    Spawn the enrichment thread if TMDB is enabled and not already running.
    Returns True if a new run was started, False if skipped.
    """
    global _tmdb_running

    if not _tmdb_enabled():
        return False
    if not _tmdb_api_key():
        logger.warning("[TMDB] trigger ignored — API key not configured in Integrations settings")
        return False

    with _tmdb_running_lock:
        if _tmdb_running:
            logger.info("[TMDB] Already running — trigger ignored (triggered_by=%s)", triggered_by)
            return False
        _tmdb_running = True

    thread = threading.Thread(
        target=_run_enrichment,
        args=(triggered_by,),
        daemon=True,
        name="tmdb-enrichment",
    )
    thread.start()
    logger.info("[TMDB] Enrichment started (triggered_by=%s)", triggered_by)
    return True


def is_running() -> bool:
    with _tmdb_running_lock:
        return _tmdb_running

"""Library management routes — browse content, manage selections, manage follow rules."""
import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db
from app.ingestion.sync import _delete_strm_file

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/library")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _import_selected_providers() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, slug FROM providers WHERE strm_mode = 'import_selected' AND is_active = 1 ORDER BY priority, name"
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

@router.get("", response_class=HTMLResponse)
async def library_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "library/index.html",
        {
            "request": request,
            "current_user": current_user,
            "import_selected_providers": _import_selected_providers(),
        },
    )


# ---------------------------------------------------------------------------
# Content browse endpoints (JSON)
# ---------------------------------------------------------------------------

@router.get("/entries", response_class=JSONResponse)
async def list_entries(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=48, ge=1, le=200),
    type: str = Query(default=""),
    search: str = Query(default=""),
    owned: str = Query(default=""),
    current_user: TokenData = Depends(get_current_user),
):
    """
    Return movies and series title-groups (not individual episodes).

    Series are grouped by cleaned_title so one card represents the entire
    series. Live / tv_vod / unsorted entries are returned individually.
    """
    offset = (page - 1) * per_page
    conditions = []
    params: list = []

    # For browse purposes exclude series episodes from top level — they are
    # surfaced through the /series/<title>/seasons/<n>/episodes endpoint.
    # When type filter is 'series' we return grouped rows; otherwise individual.
    type_filter = type.strip()

    if type_filter == "series":
        base_condition = "e.type = 'series'"
    elif type_filter:
        base_condition = "e.type = ?"
        params.append(type_filter)
    else:
        # All types — but series entries are grouped, others are individual
        base_condition = "1=1"

    if search:
        conditions.append("lower(e.cleaned_title) LIKE lower(?)")
        params.append(f"%{search}%")

    if owned == "true":
        conditions.append(
            "EXISTS (SELECT 1 FROM streams s2 WHERE s2.entry_id = e.entry_id AND s2.strm_path IS NOT NULL)"
        )
    elif owned == "false":
        conditions.append(
            "NOT EXISTS (SELECT 1 FROM streams s2 WHERE s2.entry_id = e.entry_id AND s2.strm_path IS NOT NULL)"
        )

    extra_where = (" AND " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn:
        if type_filter in ("series", ""):
            # For series: group by cleaned_title to get one card per show
            series_query = f"""
                SELECT
                    e.cleaned_title,
                    'series' AS type,
                    MIN(e.year) AS year,
                    COUNT(DISTINCT e.season) AS season_count,
                    COUNT(e.entry_id) AS episode_count,
                    MAX(e.cover_art) AS cover_art,
                    SUM(CASE WHEN s2.strm_path IS NOT NULL THEN 1 ELSE 0 END) AS owned_count,
                    COUNT(DISTINCT CASE
                        WHEN p2.strm_mode = 'import_selected' AND p2.is_active = 1
                             AND s2.exclude = 0 AND s2.imported = 0
                        THEN s2.stream_id END) AS can_add_count
                FROM entries e
                LEFT JOIN streams s2 ON s2.entry_id = e.entry_id
                LEFT JOIN providers p2 ON p2.slug = s2.provider
                WHERE e.type = 'series' {extra_where}
                GROUP BY e.cleaned_title
                ORDER BY e.cleaned_title
            """
            if type_filter == "series":
                total = conn.execute(
                    f"SELECT COUNT(*) FROM ({series_query})", params
                ).fetchone()[0]
                rows = conn.execute(
                    series_query + " LIMIT ? OFFSET ?", params + [per_page, offset]
                ).fetchall()
                entries = [_format_series_group(r) for r in rows]
            else:
                # Mixed: series groups + individual non-series
                individual_query = f"""
                    SELECT
                        e.entry_id, e.type, e.cleaned_title, e.year,
                        e.season, e.episode, e.cover_art,
                        (SELECT s3.provider FROM streams s3 WHERE s3.entry_id = e.entry_id AND s3.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                        (SELECT COUNT(*) FROM streams s3 WHERE s3.entry_id = e.entry_id) AS stream_count,
                        (SELECT COUNT(*) FROM streams s3
                         JOIN providers p3 ON p3.slug = s3.provider
                         WHERE s3.entry_id = e.entry_id
                           AND p3.strm_mode = 'import_selected' AND p3.is_active = 1
                           AND s3.exclude = 0 AND s3.imported = 0
                        ) AS can_add_count
                    FROM entries e
                    WHERE e.type NOT IN ('series') {extra_where}
                    ORDER BY e.cleaned_title
                """
                # Count: series groups + individual non-series
                series_count = conn.execute(
                    f"SELECT COUNT(*) FROM ({series_query})", params
                ).fetchone()[0]
                individual_count = conn.execute(
                    f"SELECT COUNT(*) FROM ({individual_query})",
                    params,
                ).fetchone()[0]
                total = series_count + individual_count

                # Fetch both with pagination applied naively (series first)
                series_rows = conn.execute(
                    series_query + " LIMIT ? OFFSET ?",
                    params + [per_page, offset],
                ).fetchall()
                remaining = per_page - len(series_rows)
                indiv_offset = max(0, offset - series_count)
                indiv_rows = []
                if remaining > 0:
                    indiv_rows = conn.execute(
                        individual_query + " LIMIT ? OFFSET ?",
                        params + [remaining, indiv_offset],
                    ).fetchall()

                entries = [_format_series_group(r) for r in series_rows] + \
                          [_format_individual(r) for r in indiv_rows]
        else:
            # Specific non-series type
            q = f"""
                SELECT
                    e.entry_id, e.type, e.cleaned_title, e.year,
                    e.season, e.episode, e.cover_art,
                    (SELECT s3.provider FROM streams s3 WHERE s3.entry_id = e.entry_id AND s3.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                    (SELECT COUNT(*) FROM streams s3 WHERE s3.entry_id = e.entry_id) AS stream_count,
                    (SELECT COUNT(*) FROM streams s3
                     JOIN providers p3 ON p3.slug = s3.provider
                     WHERE s3.entry_id = e.entry_id
                       AND p3.strm_mode = 'import_selected' AND p3.is_active = 1
                       AND s3.exclude = 0 AND s3.imported = 0
                    ) AS can_add_count
                FROM entries e
                WHERE {base_condition} {extra_where}
                ORDER BY e.cleaned_title
            """
            total = conn.execute(f"SELECT COUNT(*) FROM ({q})", params).fetchone()[0]
            rows = conn.execute(q + " LIMIT ? OFFSET ?", params + [per_page, offset]).fetchall()
            entries = [_format_individual(r) for r in rows]

    return JSONResponse({
        "entries": entries,
        "total": total,
        "page": page,
        "per_page": per_page,
    })


def _format_series_group(r) -> dict:
    return {
        "entry_id": None,
        "type": "series",
        "cleaned_title": r["cleaned_title"],
        "year": r["year"],
        "season_count": r["season_count"],
        "episode_count": r["episode_count"],
        "cover_art": r["cover_art"],
        "is_owned": (r["owned_count"] or 0) > 0,
        "owned_count": r["owned_count"] or 0,
        "can_add": (r["can_add_count"] or 0) > 0,
        "is_series_group": True,
    }


def _format_individual(r) -> dict:
    return {
        "entry_id": r["entry_id"],
        "type": r["type"],
        "cleaned_title": r["cleaned_title"],
        "year": r["year"],
        "season": r["season"],
        "episode": r["episode"],
        "cover_art": r["cover_art"],
        "is_owned": r["owner_slug"] is not None,
        "owner_slug": r["owner_slug"],
        "stream_count": r["stream_count"],
        "can_add": (r["can_add_count"] or 0) > 0,
        "is_series_group": False,
    }


@router.get("/series/{title}/seasons", response_class=JSONResponse)
async def list_seasons(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Return seasons for a series title with per-season ownership + follow info."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                e.season,
                COUNT(e.entry_id) AS episode_count,
                MAX(e.cover_art) AS cover_art,
                SUM(CASE WHEN s.strm_path IS NOT NULL THEN 1 ELSE 0 END) AS owned_count,
                COUNT(DISTINCT CASE
                    WHEN p.strm_mode = 'import_selected' AND p.is_active = 1
                         AND s.exclude = 0 AND s.imported = 0
                    THEN s.stream_id END) AS can_add_count
            FROM entries e
            LEFT JOIN streams s ON s.entry_id = e.entry_id
            LEFT JOIN providers p ON p.slug = s.provider
            WHERE e.type = 'series' AND lower(e.cleaned_title) = lower(?)
            GROUP BY e.season
            ORDER BY e.season
            """,
            (title,),
        ).fetchall()

        # Existing follow rules for this title across all import_selected providers
        follows = conn.execute(
            """
            SELECT f.id, f.season, f.provider_id, p.name AS provider_name
            FROM follows f
            JOIN providers p ON p.id = f.provider_id
            WHERE lower(f.entry_title) = lower(?) AND f.entry_type = 'series'
            """,
            (title,),
        ).fetchall()

    # Build a map: season_num -> [follow_ids] (NULL season = all-seasons follow)
    all_season_follows = [dict(f) for f in follows if f["season"] is None]
    season_follows: dict[int, list] = {}
    for f in follows:
        if f["season"] is not None:
            season_follows.setdefault(f["season"], []).append(dict(f))

    seasons = []
    for r in rows:
        s = r["season"]
        seasons.append({
            "season": s,
            "episode_count": r["episode_count"],
            "cover_art": r["cover_art"],
            "owned_count": r["owned_count"] or 0,
            "is_owned": (r["owned_count"] or 0) > 0,
            "can_add": (r["can_add_count"] or 0) > 0,
            "season_follows": season_follows.get(s, []),
        })

    return JSONResponse({
        "title": title,
        "seasons": seasons,
        "all_season_follows": all_season_follows,
    })


@router.get("/series/{title}/seasons/{season}/episodes", response_class=JSONResponse)
async def list_episodes(
    title: str,
    season: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Return individual episodes for a given series title + season."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                e.entry_id, e.episode, e.cover_art,
                (SELECT s2.provider FROM streams s2 WHERE s2.entry_id = e.entry_id AND s2.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                (SELECT COUNT(*) FROM streams s2 WHERE s2.entry_id = e.entry_id) AS stream_count,
                (SELECT COUNT(*) FROM streams s2
                 JOIN providers p2 ON p2.slug = s2.provider
                 WHERE s2.entry_id = e.entry_id
                   AND p2.strm_mode = 'import_selected' AND p2.is_active = 1
                   AND s2.exclude = 0 AND s2.imported = 0
                ) AS can_add_count
            FROM entries e
            WHERE e.type = 'series'
              AND lower(e.cleaned_title) = lower(?)
              AND e.season = ?
            ORDER BY e.episode
            """,
            (title, season),
        ).fetchall()

    episodes = [{
        "entry_id": r["entry_id"],
        "episode": r["episode"],
        "cover_art": r["cover_art"],
        "is_owned": r["owner_slug"] is not None,
        "owner_slug": r["owner_slug"],
        "stream_count": r["stream_count"],
        "can_add": (r["can_add_count"] or 0) > 0,
    } for r in rows]

    return JSONResponse({"title": title, "season": season, "episodes": episodes})


# ---------------------------------------------------------------------------
# Add / Remove — episode level (by entry_id)
# ---------------------------------------------------------------------------

@router.post("/entries/{entry_id}/add")
async def add_entry(
    entry_id: str,
    provider_id: int = Form(default=0),
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        if provider_id:
            stream = conn.execute(
                """
                SELECT s.stream_id FROM streams s
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ? AND p.id = ?
                  AND p.strm_mode = 'import_selected' AND p.is_active = 1
                LIMIT 1
                """,
                (entry_id, provider_id),
            ).fetchone()
        else:
            stream = conn.execute(
                """
                SELECT s.stream_id FROM streams s
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ?
                  AND p.strm_mode = 'import_selected' AND p.is_active = 1
                  AND s.exclude = 0 AND s.imported = 0
                ORDER BY p.priority, p.slug
                LIMIT 1
                """,
                (entry_id,),
            ).fetchone()
        if stream:
            conn.execute("UPDATE streams SET imported = 1 WHERE stream_id = ?", (stream["stream_id"],))
            logger.info("[LIBRARY] Add entry=%s stream=%s by=%s", entry_id[:12], stream["stream_id"], current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after Add failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True})


@router.post("/entries/{entry_id}/remove")
async def remove_entry(
    entry_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        owned = conn.execute(
            """
            SELECT s.stream_id, s.strm_path FROM streams s
            JOIN providers p ON p.slug = s.provider
            WHERE s.entry_id = ? AND p.strm_mode = 'import_selected' AND s.imported = 1
            """,
            (entry_id,),
        ).fetchall()
        for row in owned:
            if row["strm_path"]:
                _delete_strm_file(row["strm_path"])
            conn.execute(
                "UPDATE streams SET imported = 0, strm_path = NULL, last_written_url = NULL WHERE stream_id = ?",
                (row["stream_id"],),
            )
        logger.info("[LIBRARY] Remove entry=%s cleared=%d by=%s", entry_id[:12], len(owned), current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after Remove failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Add / Remove — season level (all episodes in a season)
# ---------------------------------------------------------------------------

@router.post("/series/{title}/seasons/{season}/add")
async def add_season(
    title: str,
    season: int,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='series' AND lower(cleaned_title)=lower(?) AND season=?",
                (title, season),
            ).fetchall()
        ]
        marked = 0
        for eid in entry_ids:
            stream = conn.execute(
                """
                SELECT s.stream_id FROM streams s
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ?
                  AND p.strm_mode = 'import_selected' AND p.is_active = 1
                  AND s.exclude = 0 AND s.imported = 0
                ORDER BY p.priority, p.slug LIMIT 1
                """,
                (eid,),
            ).fetchone()
            if stream:
                conn.execute("UPDATE streams SET imported = 1 WHERE stream_id = ?", (stream["stream_id"],))
                marked += 1
        logger.info("[LIBRARY] Add season title=%r S%02d episodes=%d by=%s", title, season, marked, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after season Add failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "marked": marked})


@router.post("/series/{title}/seasons/{season}/remove")
async def remove_season(
    title: str,
    season: int,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='series' AND lower(cleaned_title)=lower(?) AND season=?",
                (title, season),
            ).fetchall()
        ]
        cleared = 0
        for eid in entry_ids:
            owned = conn.execute(
                """
                SELECT s.stream_id, s.strm_path FROM streams s
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ? AND p.strm_mode = 'import_selected' AND s.imported = 1
                """,
                (eid,),
            ).fetchall()
            for row in owned:
                if row["strm_path"]:
                    _delete_strm_file(row["strm_path"])
                conn.execute(
                    "UPDATE streams SET imported = 0, strm_path = NULL, last_written_url = NULL WHERE stream_id = ?",
                    (row["stream_id"],),
                )
                cleared += 1
        logger.info("[LIBRARY] Remove season title=%r S%02d cleared=%d by=%s", title, season, cleared, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after season Remove failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "cleared": cleared})


# ---------------------------------------------------------------------------
# Add / Remove — series title level (all episodes across all seasons)
# ---------------------------------------------------------------------------

@router.post("/series/{title}/add")
async def add_series(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='series' AND lower(cleaned_title)=lower(?)",
                (title,),
            ).fetchall()
        ]
        marked = 0
        for eid in entry_ids:
            stream = conn.execute(
                """
                SELECT s.stream_id FROM streams s
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ?
                  AND p.strm_mode = 'import_selected' AND p.is_active = 1
                  AND s.exclude = 0 AND s.imported = 0
                ORDER BY p.priority, p.slug LIMIT 1
                """,
                (eid,),
            ).fetchone()
            if stream:
                conn.execute("UPDATE streams SET imported = 1 WHERE stream_id = ?", (stream["stream_id"],))
                marked += 1
        logger.info("[LIBRARY] Add series title=%r episodes=%d by=%s", title, marked, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after series Add failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "marked": marked})


@router.post("/series/{title}/remove")
async def remove_series(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='series' AND lower(cleaned_title)=lower(?)",
                (title,),
            ).fetchall()
        ]
        cleared = 0
        for eid in entry_ids:
            owned = conn.execute(
                """
                SELECT s.stream_id, s.strm_path FROM streams s
                JOIN providers p ON p.slug = s.provider
                WHERE s.entry_id = ? AND p.strm_mode = 'import_selected' AND s.imported = 1
                """,
                (eid,),
            ).fetchall()
            for row in owned:
                if row["strm_path"]:
                    _delete_strm_file(row["strm_path"])
                conn.execute(
                    "UPDATE streams SET imported = 0, strm_path = NULL, last_written_url = NULL WHERE stream_id = ?",
                    (row["stream_id"],),
                )
                cleared += 1
        logger.info("[LIBRARY] Remove series title=%r cleared=%d by=%s", title, cleared, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after series Remove failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "cleared": cleared})


# ---------------------------------------------------------------------------
# Follow rules — CRUD
# ---------------------------------------------------------------------------

@router.get("/follows", response_class=JSONResponse)
async def list_follows(current_user: TokenData = Depends(get_current_user)):
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT f.id, f.entry_type, f.entry_title, f.season,
                   p.name AS provider_name, p.slug AS provider_slug
            FROM follows f
            JOIN providers p ON p.id = f.provider_id
            ORDER BY p.name, f.entry_type, f.entry_title
            """
        ).fetchall()
    return JSONResponse([dict(r) for r in rows])


@router.post("/follows", response_class=JSONResponse)
async def add_follow(
    provider_id: int = Form(...),
    entry_type: str = Form(...),
    entry_title: str = Form(...),
    season: str = Form(default=""),
    current_user: TokenData = Depends(get_current_user),
):
    if entry_type not in ("movie", "series"):
        return JSONResponse({"ok": False, "error": "Invalid entry_type"}, status_code=400)

    season_int: Optional[int] = None
    if season.strip():
        try:
            season_int = int(season.strip())
        except ValueError:
            pass

    with get_db() as conn:
        provider = conn.execute(
            "SELECT id FROM providers WHERE id = ? AND strm_mode = 'import_selected' AND is_active = 1",
            (provider_id,),
        ).fetchone()
        if not provider:
            return JSONResponse({"ok": False, "error": "Provider not found"}, status_code=404)

        conn.execute(
            "INSERT INTO follows (provider_id, entry_type, entry_title, season) VALUES (?, ?, ?, ?)",
            (provider_id, entry_type, entry_title.strip(), season_int),
        )
    logger.info(
        "[LIBRARY] Follow added provider_id=%d type=%s title=%r season=%s by=%s",
        provider_id, entry_type, entry_title.strip(), season_int, current_user.username,
    )
    return JSONResponse({"ok": True})


@router.post("/follows/{follow_id}/delete", response_class=JSONResponse)
async def delete_follow(
    follow_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        conn.execute("DELETE FROM follows WHERE id = ?", (follow_id,))
    logger.info("[LIBRARY] Follow deleted id=%d by=%s", follow_id, current_user.username)
    return JSONResponse({"ok": True})

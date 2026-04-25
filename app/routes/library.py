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
        },
    )


# ---------------------------------------------------------------------------
# Content browse endpoints (JSON)
# ---------------------------------------------------------------------------

# Subquery: entry has at least one stream from an active import_selected provider
_HAS_IMPORT_SELECTED = """
    EXISTS (
        SELECT 1 FROM streams _s
        JOIN providers _p ON _p.slug = _s.provider
        WHERE _s.entry_id = e.entry_id
          AND _p.strm_mode = 'import_selected'
          AND _p.is_active = 1
    )
"""

# Subquery: can_add — entry has an unimported, non-excluded stream from an
# active import_selected provider
_CAN_ADD_SUBQUERY = """
    (SELECT COUNT(*) FROM streams _s2
     JOIN providers _p2 ON _p2.slug = _s2.provider
     WHERE _s2.entry_id = e.entry_id
       AND _p2.strm_mode = 'import_selected' AND _p2.is_active = 1
       AND _s2.exclude = 0 AND _s2.imported = 0)
"""

# Subquery: filtered_title from the highest-priority eligible import_selected stream.
# This mirrors the STRM engine's priority resolution so the displayed title matches
# what will be written to disk.
_FILTERED_TITLE_SUBQUERY = """
    (SELECT _sf.filtered_title FROM streams _sf
     JOIN providers _pf ON _pf.slug = _sf.provider
     WHERE _sf.entry_id = e.entry_id
       AND _pf.strm_mode = 'import_selected' AND _pf.is_active = 1
       AND _sf.exclude = 0
       AND _sf.filtered_title IS NOT NULL AND _sf.filtered_title != ''
     ORDER BY _pf.priority, _pf.slug
     LIMIT 1)
"""


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
    Return content from import_selected providers only.

    Series are grouped by cleaned_title so one card represents the entire show.
    """
    offset = (page - 1) * per_page
    conditions = []
    params: list = []

    type_filter = type.strip()

    if type_filter == "series":
        base_condition = "e.type = 'series'"
    elif type_filter and type_filter != "tv_vod":
        base_condition = "e.type = ?"
        params.append(type_filter)
    else:
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

    # Always restrict to import_selected provider content
    conditions.append(_HAS_IMPORT_SELECTED)

    extra_where = (" AND " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn:
        if type_filter == "series":
            series_query = _series_group_query(extra_where)
            total = conn.execute(f"SELECT COUNT(*) FROM ({series_query})", params).fetchone()[0]
            rows = conn.execute(series_query + " LIMIT ? OFFSET ?", params + [per_page, offset]).fetchall()
            entries = [_format_series_group(r) for r in rows]

        elif type_filter == "tv_vod":
            tv_query = _tv_vod_group_query(extra_where)
            total = conn.execute(f"SELECT COUNT(*) FROM ({tv_query})", params).fetchone()[0]
            rows = conn.execute(tv_query + " LIMIT ? OFFSET ?", params + [per_page, offset]).fetchall()
            entries = [_format_tv_vod_group(r) for r in rows]

        elif type_filter == "":
            # All types: series groups + tv_vod groups + individual items
            series_query = _series_group_query(extra_where)
            tv_query = _tv_vod_group_query(extra_where)
            individual_query = f"""
                SELECT
                    e.entry_id, e.type, e.cleaned_title, e.year,
                    e.season, e.episode, e.cover_art,
                    {_FILTERED_TITLE_SUBQUERY} AS filtered_title,
                    (SELECT s3.provider FROM streams s3 WHERE s3.entry_id = e.entry_id AND s3.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                    (SELECT COUNT(*) FROM streams s3 WHERE s3.entry_id = e.entry_id) AS stream_count,
                    {_CAN_ADD_SUBQUERY} AS can_add_count
                FROM entries e
                WHERE e.type NOT IN ('series', 'tv_vod') {extra_where}
                ORDER BY e.cleaned_title
            """
            series_count = conn.execute(f"SELECT COUNT(*) FROM ({series_query})", params).fetchone()[0]
            tv_count = conn.execute(f"SELECT COUNT(*) FROM ({tv_query})", params).fetchone()[0]
            individual_count = conn.execute(f"SELECT COUNT(*) FROM ({individual_query})", params).fetchone()[0]
            total = series_count + tv_count + individual_count

            # Paginate across the three result sets in order
            series_rows = conn.execute(series_query + " LIMIT ? OFFSET ?", params + [per_page, offset]).fetchall()
            remaining = per_page - len(series_rows)
            tv_offset = max(0, offset - series_count)
            tv_rows = []
            if remaining > 0:
                tv_rows = conn.execute(tv_query + " LIMIT ? OFFSET ?", params + [remaining, tv_offset]).fetchall()
            remaining -= len(tv_rows)
            indiv_offset = max(0, offset - series_count - tv_count)
            indiv_rows = []
            if remaining > 0:
                indiv_rows = conn.execute(individual_query + " LIMIT ? OFFSET ?", params + [remaining, indiv_offset]).fetchall()

            entries = (
                [_format_series_group(r) for r in series_rows]
                + [_format_tv_vod_group(r) for r in tv_rows]
                + [_format_individual(r) for r in indiv_rows]
            )

        else:
            q = f"""
                SELECT
                    e.entry_id, e.type, e.cleaned_title, e.year,
                    e.season, e.episode, e.cover_art,
                    {_FILTERED_TITLE_SUBQUERY} AS filtered_title,
                    (SELECT s3.provider FROM streams s3 WHERE s3.entry_id = e.entry_id AND s3.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                    (SELECT COUNT(*) FROM streams s3 WHERE s3.entry_id = e.entry_id) AS stream_count,
                    {_CAN_ADD_SUBQUERY} AS can_add_count
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


def _series_group_query(extra_where: str) -> str:
    return f"""
        SELECT
            e.cleaned_title,
            MIN({_FILTERED_TITLE_SUBQUERY}) AS filtered_title,
            'series' AS type,
            MIN(e.year) AS year,
            COUNT(DISTINCT e.season) AS season_count,
            COUNT(e.entry_id) AS episode_count,
            MAX(e.cover_art) AS cover_art,
            SUM(CASE WHEN s2.strm_path IS NOT NULL THEN 1 ELSE 0 END) AS owned_count,
            SUM(CASE WHEN _p2.strm_mode = 'import_selected' AND _p2.is_active = 1
                          AND s2.exclude = 0 AND s2.imported = 0
                     THEN 1 ELSE 0 END) AS can_add_count
        FROM entries e
        LEFT JOIN streams s2 ON s2.entry_id = e.entry_id
        LEFT JOIN providers _p2 ON _p2.slug = s2.provider
        WHERE e.type = 'series' {extra_where}
        GROUP BY e.cleaned_title
        ORDER BY e.cleaned_title
    """


def _tv_vod_group_query(extra_where: str) -> str:
    return f"""
        SELECT
            e.cleaned_title,
            MIN({_FILTERED_TITLE_SUBQUERY}) AS filtered_title,
            'tv_vod' AS type,
            COUNT(DISTINCT substr(e.air_date, 1, 4)) AS year_count,
            COUNT(e.entry_id) AS episode_count,
            MAX(e.cover_art) AS cover_art,
            SUM(CASE WHEN s2.strm_path IS NOT NULL THEN 1 ELSE 0 END) AS owned_count,
            SUM(CASE WHEN _p2.strm_mode = 'import_selected' AND _p2.is_active = 1
                          AND s2.exclude = 0 AND s2.imported = 0
                     THEN 1 ELSE 0 END) AS can_add_count
        FROM entries e
        LEFT JOIN streams s2 ON s2.entry_id = e.entry_id
        LEFT JOIN providers _p2 ON _p2.slug = s2.provider
        WHERE e.type = 'tv_vod' {extra_where}
        GROUP BY e.cleaned_title
        ORDER BY e.cleaned_title
    """


def _display_title(r) -> str:
    try:
        ft = r["filtered_title"]
    except IndexError:
        ft = None
    return (ft or r["cleaned_title"] or "").strip() or (r["cleaned_title"] or "")


def _format_series_group(r) -> dict:
    return {
        "entry_id": None,
        "type": "series",
        "cleaned_title": r["cleaned_title"],   # kept for API lookups (URLs use this)
        "display_title": _display_title(r),
        "year": r["year"],
        "season_count": r["season_count"],
        "episode_count": r["episode_count"],
        "cover_art": r["cover_art"],
        "is_owned": (r["owned_count"] or 0) > 0,
        "owned_count": r["owned_count"] or 0,
        "can_add": (r["can_add_count"] or 0) > 0,
        "is_series_group": True,
    }


def _format_tv_vod_group(r) -> dict:
    return {
        "entry_id": None,
        "type": "tv_vod",
        "cleaned_title": r["cleaned_title"],
        "display_title": _display_title(r),
        "year_count": r["year_count"],
        "episode_count": r["episode_count"],
        "cover_art": r["cover_art"],
        "is_owned": (r["owned_count"] or 0) > 0,
        "owned_count": r["owned_count"] or 0,
        "can_add": (r["can_add_count"] or 0) > 0,
        "is_tv_vod_group": True,
        "is_series_group": False,
    }


def _format_individual(r) -> dict:
    return {
        "entry_id": r["entry_id"],
        "type": r["type"],
        "cleaned_title": r["cleaned_title"],   # kept for API lookups
        "display_title": _display_title(r),
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
    """Return seasons for a series title with per-season ownership info."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                e.season,
                COUNT(e.entry_id) AS episode_count,
                MAX(e.cover_art) AS cover_art,
                SUM(CASE WHEN s.strm_path IS NOT NULL THEN 1 ELSE 0 END) AS owned_count,
                SUM(CASE WHEN p.strm_mode = 'import_selected' AND p.is_active = 1
                              AND s.exclude = 0 AND s.imported = 0
                         THEN 1 ELSE 0 END) AS can_add_count
            FROM entries e
            LEFT JOIN streams s ON s.entry_id = e.entry_id
            LEFT JOIN providers p ON p.slug = s.provider
            WHERE e.type = 'series' AND lower(e.cleaned_title) = lower(?)
            GROUP BY e.season
            ORDER BY e.season
            """,
            (title,),
        ).fetchall()

        # Follow state: all-seasons rule and per-season rules
        follows = conn.execute(
            """
            SELECT f.season FROM follows f
            WHERE lower(f.entry_title) = lower(?) AND f.entry_type = 'series'
            """,
            (title,),
        ).fetchall()

    followed_all = any(f["season"] is None for f in follows)
    followed_seasons = {f["season"] for f in follows if f["season"] is not None}

    seasons = []
    for r in rows:
        snum = r["season"]
        seasons.append({
            "season": snum,
            "episode_count": r["episode_count"],
            "cover_art": r["cover_art"],
            "owned_count": r["owned_count"] or 0,
            "is_owned": (r["owned_count"] or 0) > 0,
            "can_add": (r["can_add_count"] or 0) > 0,
            "is_following": followed_all or (snum in followed_seasons),
        })

    return JSONResponse({
        "title": title,
        "seasons": seasons,
        "is_following_all": followed_all,
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
                ) AS can_add_count,
                (SELECT s2.filtered_title FROM streams s2
                 JOIN providers p2 ON p2.slug = s2.provider
                 WHERE s2.entry_id = e.entry_id
                   AND p2.strm_mode = 'import_selected' AND p2.is_active = 1
                   AND s2.exclude = 0
                   AND s2.filtered_title IS NOT NULL AND s2.filtered_title != ''
                 ORDER BY p2.priority, p2.slug
                 LIMIT 1
                ) AS filtered_title
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
        "display_title": (r["filtered_title"] or "").strip() or None,
        "is_owned": r["owner_slug"] is not None,
        "owner_slug": r["owner_slug"],
        "stream_count": r["stream_count"],
        "can_add": (r["can_add_count"] or 0) > 0,
    } for r in rows]

    return JSONResponse({"title": title, "season": season, "episodes": episodes})


@router.get("/tv_vod/{title}/years", response_class=JSONResponse)
async def list_tv_vod_years(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Return years for a tv_vod show title with per-year ownership info."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                substr(e.air_date, 1, 4) AS year,
                COUNT(e.entry_id) AS episode_count,
                MAX(e.cover_art) AS cover_art,
                SUM(CASE WHEN s.strm_path IS NOT NULL THEN 1 ELSE 0 END) AS owned_count,
                SUM(CASE WHEN p.strm_mode = 'import_selected' AND p.is_active = 1
                              AND s.exclude = 0 AND s.imported = 0
                         THEN 1 ELSE 0 END) AS can_add_count
            FROM entries e
            LEFT JOIN streams s ON s.entry_id = e.entry_id
            LEFT JOIN providers p ON p.slug = s.provider
            WHERE e.type = 'tv_vod' AND lower(e.cleaned_title) = lower(?)
            GROUP BY substr(e.air_date, 1, 4)
            ORDER BY substr(e.air_date, 1, 4) DESC
            """,
            (title,),
        ).fetchall()

    years = [{
        "year": r["year"] or "Unknown",
        "episode_count": r["episode_count"],
        "cover_art": r["cover_art"],
        "owned_count": r["owned_count"] or 0,
        "is_owned": (r["owned_count"] or 0) > 0,
        "can_add": (r["can_add_count"] or 0) > 0,
    } for r in rows]

    return JSONResponse({"title": title, "years": years})


@router.get("/tv_vod/{title}/years/{year}/episodes", response_class=JSONResponse)
async def list_tv_vod_episodes(
    title: str,
    year: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Return individual episodes for a tv_vod show title + year."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                e.entry_id, e.air_date, e.cover_art,
                (SELECT s2.provider FROM streams s2 WHERE s2.entry_id = e.entry_id AND s2.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                (SELECT COUNT(*) FROM streams s2 WHERE s2.entry_id = e.entry_id) AS stream_count,
                (SELECT COUNT(*) FROM streams s2
                 JOIN providers p2 ON p2.slug = s2.provider
                 WHERE s2.entry_id = e.entry_id
                   AND p2.strm_mode = 'import_selected' AND p2.is_active = 1
                   AND s2.exclude = 0 AND s2.imported = 0
                ) AS can_add_count
            FROM entries e
            WHERE e.type = 'tv_vod'
              AND lower(e.cleaned_title) = lower(?)
              AND substr(e.air_date, 1, 4) = ?
            ORDER BY e.air_date DESC
            """,
            (title, year),
        ).fetchall()

    episodes = [{
        "entry_id": r["entry_id"],
        "air_date": r["air_date"],
        "cover_art": r["cover_art"],
        "is_owned": r["owner_slug"] is not None,
        "owner_slug": r["owner_slug"],
        "stream_count": r["stream_count"],
        "can_add": (r["can_add_count"] or 0) > 0,
    } for r in rows]

    return JSONResponse({"title": title, "year": year, "episodes": episodes})


@router.post("/tv_vod/{title}/years/{year}/add")
async def add_tv_vod_year(
    title: str,
    year: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='tv_vod' AND lower(cleaned_title)=lower(?) AND substr(air_date,1,4)=?",
                (title, year),
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
        logger.info("[LIBRARY] Add tv_vod title=%r year=%s episodes=%d by=%s", title, year, marked, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after tv_vod year Add failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "marked": marked})


@router.post("/tv_vod/{title}/years/{year}/remove")
async def remove_tv_vod_year(
    title: str,
    year: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='tv_vod' AND lower(cleaned_title)=lower(?) AND substr(air_date,1,4)=?",
                (title, year),
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
        logger.info("[LIBRARY] Remove tv_vod title=%r year=%s cleared=%d by=%s", title, year, cleared, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after tv_vod year Remove failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "cleared": cleared})


@router.post("/tv_vod/{title}/add")
async def add_tv_vod_all(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='tv_vod' AND lower(cleaned_title)=lower(?)",
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
        logger.info("[LIBRARY] Add tv_vod all title=%r episodes=%d by=%s", title, marked, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after tv_vod Add All failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "marked": marked})


@router.post("/tv_vod/{title}/remove")
async def remove_tv_vod_all(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        entry_ids = [
            r["entry_id"] for r in conn.execute(
                "SELECT entry_id FROM entries WHERE type='tv_vod' AND lower(cleaned_title)=lower(?)",
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
        logger.info("[LIBRARY] Remove tv_vod all title=%r cleared=%d by=%s", title, cleared, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after tv_vod Remove All failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "cleared": cleared})


# ---------------------------------------------------------------------------
# Add / Remove — episode level (by entry_id)
# ---------------------------------------------------------------------------

@router.post("/entries/{entry_id}/add")
async def add_entry(
    entry_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm
    with get_db() as conn:
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
# Internal helper: mark existing episodes as imported
# ---------------------------------------------------------------------------

def _import_entries_for_title(conn, entry_type: str, title: str, season) -> int:
    """Mark all existing unimported streams for a title (optionally a specific season) as imported=1.
    Returns count of streams marked."""
    if entry_type == "series":
        if season is not None:
            rows = conn.execute(
                "SELECT entry_id FROM entries WHERE type='series' AND lower(cleaned_title)=lower(?) AND season=?",
                (title, season),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT entry_id FROM entries WHERE type='series' AND lower(cleaned_title)=lower(?)",
                (title,),
            ).fetchall()
    else:
        rows = conn.execute(
            "SELECT entry_id FROM entries WHERE type=? AND lower(cleaned_title)=lower(?)",
            (entry_type, title),
        ).fetchall()

    marked = 0
    for r in rows:
        stream = conn.execute(
            """
            SELECT s.stream_id FROM streams s
            JOIN providers p ON p.slug = s.provider
            WHERE s.entry_id = ?
              AND p.strm_mode = 'import_selected' AND p.is_active = 1
              AND s.exclude = 0 AND s.imported = 0
            ORDER BY p.priority, p.slug LIMIT 1
            """,
            (r["entry_id"],),
        ).fetchone()
        if stream:
            conn.execute("UPDATE streams SET imported = 1 WHERE stream_id = ?", (stream["stream_id"],))
            marked += 1
    return marked


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
    entry_type: str = Form(...),
    entry_title: str = Form(...),
    current_user: TokenData = Depends(get_current_user),
):
    """Create a follow rule for an entire series and immediately import all existing episodes."""
    from app.tasks.strm import generate_strm
    if entry_type not in ("movie", "series"):
        return JSONResponse({"ok": False, "error": "Invalid entry_type"}, status_code=400)

    title = entry_title.strip()
    with get_db() as conn:
        providers = conn.execute(
            "SELECT id FROM providers WHERE strm_mode = 'import_selected' AND is_active = 1"
        ).fetchall()

        if not providers:
            return JSONResponse({"ok": False, "error": "No active import_selected providers"}, status_code=400)

        inserted = 0
        for p in providers:
            exists = conn.execute(
                "SELECT 1 FROM follows WHERE provider_id = ? AND entry_type = ? AND lower(entry_title) = lower(?) AND season IS NULL",
                (p["id"], entry_type, title),
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO follows (provider_id, entry_type, entry_title, season) VALUES (?, ?, ?, NULL)",
                    (p["id"], entry_type, title),
                )
                inserted += 1

        # Immediately import all existing matching episodes
        marked = _import_entries_for_title(conn, entry_type, title, season=None)

    logger.info(
        "[LIBRARY] Follow added type=%s title=%r providers=%d marked=%d by=%s",
        entry_type, title, inserted, marked, current_user.username,
    )
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after follow failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "inserted": inserted, "marked": marked})


@router.post("/follows/{follow_id}/delete", response_class=JSONResponse)
async def delete_follow(
    follow_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        conn.execute("DELETE FROM follows WHERE id = ?", (follow_id,))
    logger.info("[LIBRARY] Follow deleted id=%d by=%s", follow_id, current_user.username)
    return JSONResponse({"ok": True})


@router.post("/series/{title}/seasons/{season}/follow", response_class=JSONResponse)
async def follow_season(
    title: str,
    season: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Create a season-specific follow rule and immediately import all existing episodes."""
    from app.tasks.strm import generate_strm
    with get_db() as conn:
        providers = conn.execute(
            "SELECT id FROM providers WHERE strm_mode = 'import_selected' AND is_active = 1"
        ).fetchall()

        if not providers:
            return JSONResponse({"ok": False, "error": "No active import_selected providers"}, status_code=400)

        inserted = 0
        for p in providers:
            exists = conn.execute(
                "SELECT 1 FROM follows WHERE provider_id = ? AND entry_type = 'series' AND lower(entry_title) = lower(?) AND season = ?",
                (p["id"], title, season),
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO follows (provider_id, entry_type, entry_title, season) VALUES (?, 'series', ?, ?)",
                    (p["id"], title, season),
                )
                inserted += 1

        marked = _import_entries_for_title(conn, "series", title, season=season)

    logger.info("[LIBRARY] Follow season title=%r S%02d inserted=%d marked=%d by=%s", title, season, inserted, marked, current_user.username)
    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after season follow failed: %s", exc, exc_info=True)
    return JSONResponse({"ok": True, "inserted": inserted, "marked": marked})


@router.post("/series/{title}/seasons/{season}/unfollow", response_class=JSONResponse)
async def unfollow_season(
    title: str,
    season: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Remove season-specific follow rules for a series title + season."""
    with get_db() as conn:
        result = conn.execute(
            "DELETE FROM follows WHERE lower(entry_title) = lower(?) AND entry_type = 'series' AND season = ?",
            (title, season),
        )
        deleted = result.rowcount
    logger.info("[LIBRARY] Unfollow season title=%r S%02d deleted=%d by=%s", title, season, deleted, current_user.username)
    return JSONResponse({"ok": True, "deleted": deleted})


@router.post("/series/{title}/unfollow", response_class=JSONResponse)
async def unfollow_series(
    title: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Remove all follow rules for a series title."""
    with get_db() as conn:
        result = conn.execute(
            "DELETE FROM follows WHERE lower(entry_title) = lower(?) AND entry_type = 'series'",
            (title,),
        )
        deleted = result.rowcount
    logger.info("[LIBRARY] Unfollow series title=%r deleted=%d by=%s", title, deleted, current_user.username)
    return JSONResponse({"ok": True, "deleted": deleted})

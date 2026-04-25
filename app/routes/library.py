"""Library management routes — browse content, manage selections, manage follow rules."""
import logging
import os

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


@router.get("", response_class=HTMLResponse)
async def library_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    import_selected_providers = _import_selected_providers()
    with get_db() as conn:
        follows_rows = conn.execute(
            """
            SELECT f.id, f.entry_type, f.entry_title, f.season,
                   p.name AS provider_name, p.slug AS provider_slug
            FROM follows f
            JOIN providers p ON p.id = f.provider_id
            ORDER BY p.name, f.entry_type, f.entry_title
            """
        ).fetchall()
    follows = [dict(r) for r in follows_rows]
    return templates.TemplateResponse(
        "library/index.html",
        {
            "request": request,
            "current_user": current_user,
            "import_selected_providers": import_selected_providers,
            "follows": follows,
        },
    )


@router.get("/entries", response_class=JSONResponse)
async def list_entries(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=200),
    type: str = Query(default=""),
    search: str = Query(default=""),
    owned: str = Query(default=""),
    current_user: TokenData = Depends(get_current_user),
):
    offset = (page - 1) * per_page

    conditions = []
    params: list = []

    if type:
        conditions.append("e.type = ?")
        params.append(type)

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

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM entries e {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""
            SELECT e.entry_id, e.type, e.cleaned_title, e.year, e.season, e.episode,
                   (SELECT s.provider FROM streams s WHERE s.entry_id = e.entry_id AND s.strm_path IS NOT NULL LIMIT 1) AS owner_slug,
                   (SELECT COUNT(*) FROM streams s WHERE s.entry_id = e.entry_id) AS stream_count,
                   (SELECT COUNT(*) FROM streams s
                    JOIN providers p ON p.slug = s.provider
                    WHERE s.entry_id = e.entry_id
                      AND p.strm_mode = 'import_selected'
                      AND p.is_active = 1
                      AND s.exclude = 0
                      AND s.imported = 0
                   ) AS can_add_count
            FROM entries e
            {where}
            ORDER BY e.cleaned_title, e.season, e.episode
            LIMIT ? OFFSET ?
            """,
            params + [per_page, offset],
        ).fetchall()

    entries = []
    for row in rows:
        entries.append({
            "entry_id": row["entry_id"],
            "type": row["type"],
            "cleaned_title": row["cleaned_title"],
            "year": row["year"],
            "season": row["season"],
            "episode": row["episode"],
            "is_owned": row["owner_slug"] is not None,
            "owner_slug": row["owner_slug"],
            "stream_count": row["stream_count"],
            "can_add": row["can_add_count"] > 0,
        })

    return JSONResponse({
        "entries": entries,
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@router.post("/entries/{entry_id}/add", response_class=RedirectResponse)
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
            conn.execute(
                "UPDATE streams SET imported = 1 WHERE stream_id = ?",
                (stream["stream_id"],),
            )
            logger.info(
                "[LIBRARY] Add — entry=%s  stream_id=%s  by=%s",
                entry_id[:12], stream["stream_id"], current_user.username,
            )

    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after Add failed: %s", exc, exc_info=True)

    return RedirectResponse("/library", status_code=302)


@router.post("/entries/{entry_id}/remove", response_class=RedirectResponse)
async def remove_entry(
    entry_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import generate_strm

    with get_db() as conn:
        owned_streams = conn.execute(
            """
            SELECT s.stream_id, s.strm_path FROM streams s
            JOIN providers p ON p.slug = s.provider
            WHERE s.entry_id = ? AND p.strm_mode = 'import_selected'
              AND s.imported = 1
            """,
            (entry_id,),
        ).fetchall()

        for row in owned_streams:
            if row["strm_path"]:
                _delete_strm_file(row["strm_path"])
            conn.execute(
                "UPDATE streams SET imported = 0, strm_path = NULL, last_written_url = NULL WHERE stream_id = ?",
                (row["stream_id"],),
            )
        logger.info(
            "[LIBRARY] Remove — entry=%s  streams_cleared=%d  by=%s",
            entry_id[:12], len(owned_streams), current_user.username,
        )

    try:
        generate_strm()
    except Exception as exc:
        logger.error("[LIBRARY] generate_strm after Remove failed: %s", exc, exc_info=True)

    return RedirectResponse("/library", status_code=302)


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


@router.post("/follows", response_class=RedirectResponse)
async def add_follow(
    provider_id: int = Form(...),
    entry_type: str = Form(...),
    entry_title: str = Form(...),
    season: str = Form(default=""),
    current_user: TokenData = Depends(get_current_user),
):
    if entry_type not in ("movie", "series"):
        return RedirectResponse("/library", status_code=302)

    season_int: int | None = None
    if season.strip():
        try:
            season_int = int(season.strip())
        except ValueError:
            season_int = None

    with get_db() as conn:
        provider = conn.execute(
            "SELECT id FROM providers WHERE id = ? AND strm_mode = 'import_selected' AND is_active = 1",
            (provider_id,),
        ).fetchone()
        if not provider:
            return RedirectResponse("/library", status_code=302)

        conn.execute(
            "INSERT INTO follows (provider_id, entry_type, entry_title, season) VALUES (?, ?, ?, ?)",
            (provider_id, entry_type, entry_title.strip(), season_int),
        )
    logger.info(
        "[LIBRARY] Follow added — provider_id=%d  type=%s  title=%r  season=%s  by=%s",
        provider_id, entry_type, entry_title.strip(), season_int, current_user.username,
    )
    return RedirectResponse("/library", status_code=302)


@router.post("/follows/{follow_id}/delete", response_class=RedirectResponse)
async def delete_follow(
    follow_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        conn.execute("DELETE FROM follows WHERE id = ?", (follow_id,))
    logger.info("[LIBRARY] Follow deleted — id=%d  by=%s", follow_id, current_user.username)
    return RedirectResponse("/library", status_code=302)

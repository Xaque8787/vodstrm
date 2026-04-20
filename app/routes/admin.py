import hashlib
import logging
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_admin
from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

_LIBRARY_PAGE_SIZE = 100


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


@router.get("/users", response_class=HTMLResponse)
async def list_users(request: Request, current_user: TokenData = Depends(get_current_admin)):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, username, email, is_admin, created_at FROM users ORDER BY id"
        ).fetchall()
    users = [dict(r) for r in rows]
    return templates.TemplateResponse(
        "admin/users.html",
        {"request": request, "users": users, "current_user": current_user},
    )


@router.post("/users/{user_id}/delete")
async def delete_user(user_id: int, current_user: TokenData = Depends(get_current_admin)):
    if user_id == current_user.user_id:
        return RedirectResponse("/admin/users?error=cannot_delete_self", status_code=302)
    with get_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    logger.info("Admin '%s' deleted user id=%d", current_user.username, user_id)
    return RedirectResponse("/admin/users", status_code=302)


# ---------------------------------------------------------------------------
# LIBRARY INSPECTION / DEBUG
# ---------------------------------------------------------------------------

@router.get("/library", response_class=HTMLResponse)
async def library_page(
    request: Request,
    tab: str = "entries",
    page: int = 1,
    current_user: TokenData = Depends(get_current_admin),
):
    tab = tab if tab in ("entries", "streams") else "entries"
    page = max(1, page)
    offset = (page - 1) * _LIBRARY_PAGE_SIZE

    with get_db() as conn:
        entry_count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        stream_count = conn.execute("SELECT COUNT(*) FROM streams").fetchone()[0]

        if tab == "entries":
            rows = conn.execute(
                """
                SELECT entry_id, type, cleaned_title, raw_title,
                       year, season, episode, air_date, series_type,
                       created_at, updated_at
                FROM entries
                ORDER BY type, cleaned_title
                LIMIT ? OFFSET ?
                """,
                (_LIBRARY_PAGE_SIZE, offset),
            ).fetchall()
            total = entry_count
        else:
            rows = conn.execute(
                """
                SELECT stream_id, entry_id, provider, stream_url,
                       source_file, ingested_at, batch_id
                FROM streams
                ORDER BY provider, entry_id
                LIMIT ? OFFSET ?
                """,
                (_LIBRARY_PAGE_SIZE, offset),
            ).fetchall()
            total = stream_count

    total_pages = max(1, (total + _LIBRARY_PAGE_SIZE - 1) // _LIBRARY_PAGE_SIZE)
    data = [dict(r) for r in rows]

    flash = request.query_params.get("flash")

    return templates.TemplateResponse(
        "admin/library.html",
        {
            "request": request,
            "current_user": current_user,
            "tab": tab,
            "rows": data,
            "entry_count": entry_count,
            "stream_count": stream_count,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "flash": flash,
        },
    )


@router.post("/library/clear/entries")
async def clear_entries(current_user: TokenData = Depends(get_current_admin)):
    with get_db() as conn:
        conn.execute("DELETE FROM streams")
        conn.execute("DELETE FROM entries")
    logger.warning(
        "[ADMIN] entries + streams tables cleared by '%s'", current_user.username
    )
    return RedirectResponse("/admin/library?flash=cleared&tab=entries", status_code=302)


@router.post("/library/clear/streams")
async def clear_streams(current_user: TokenData = Depends(get_current_admin)):
    with get_db() as conn:
        conn.execute("DELETE FROM streams")
    logger.warning(
        "[ADMIN] streams table cleared by '%s'", current_user.username
    )
    return RedirectResponse("/admin/library?flash=streams_cleared&tab=streams", status_code=302)

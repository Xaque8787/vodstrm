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

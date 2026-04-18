import hashlib
import logging
import os
from datetime import timedelta

from fastapi import APIRouter, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import (
    COOKIE_NAME,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    create_access_token,
    decode_access_token,
)
from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _get_user_by_username(username: str):
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, username, email, password_hash, is_admin FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    return row


def _admin_exists() -> bool:
    with get_db() as conn:
        row = conn.execute("SELECT 1 FROM users WHERE is_admin = 1 LIMIT 1").fetchone()
    return row is not None


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if not _admin_exists():
        return RedirectResponse("/setup", status_code=302)
    token = request.cookies.get(COOKIE_NAME)
    if token and decode_access_token(token):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    response: Response,
    username: str = Form(...),
    password: str = Form(...),
):
    user = _get_user_by_username(username)
    if not user or user["password_hash"] != _hash_password(password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid username or password"},
            status_code=401,
        )

    token = create_access_token(
        {"sub": user["username"], "user_id": user["id"], "is_admin": bool(user["is_admin"])},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )

    redirect = RedirectResponse("/", status_code=302)
    redirect.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=os.getenv("SECURE_COOKIES", "false").lower() == "true",
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )
    logger.info("User '%s' logged in", username)
    return redirect


@router.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie(COOKIE_NAME)
    return response


@router.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request):
    if _admin_exists():
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("setup.html", {"request": request, "error": None})


@router.post("/setup", response_class=HTMLResponse)
async def setup_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    email: str = Form(""),
):
    if _admin_exists():
        return RedirectResponse("/login", status_code=302)

    if len(username.strip()) < 3:
        return templates.TemplateResponse(
            "setup.html",
            {"request": request, "error": "Username must be at least 3 characters"},
            status_code=400,
        )
    if len(password) < 8:
        return templates.TemplateResponse(
            "setup.html",
            {"request": request, "error": "Password must be at least 8 characters"},
            status_code=400,
        )

    with get_db() as conn:
        conn.execute(
            "INSERT INTO users (username, email, password_hash, is_admin) VALUES (?, ?, ?, 1)",
            (username.strip(), email.strip() or None, _hash_password(password)),
        )

    logger.info("Initial admin account created: %s", username)
    return RedirectResponse("/login", status_code=302)

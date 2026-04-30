"""Integrations page — settings storage and TMDB enrichment controls."""
import json
import logging
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db
from app.utils.env import local_now_iso

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/integrations")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _load_tmdb_settings(conn) -> dict:
    row = conn.execute(
        "SELECT settings FROM integrations WHERE slug = 'tmdb'"
    ).fetchone()
    if not row:
        return {"enabled": False, "api_key": "", "language": "en-US"}
    try:
        return json.loads(row["settings"] or "{}")
    except (ValueError, TypeError):
        return {"enabled": False, "api_key": "", "language": "en-US"}


def _save_tmdb_settings(conn, settings: dict) -> None:
    conn.execute("""
        INSERT INTO integrations (slug, settings, updated_at)
        VALUES ('tmdb', ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            settings   = excluded.settings,
            updated_at = excluded.updated_at
    """, (json.dumps(settings), local_now_iso()))


def _page_ctx(conn, request, current_user):
    from app.tasks.tmdb import is_running

    cfg = _load_tmdb_settings(conn)
    counts = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) AS enriched
        FROM entries
        WHERE type IN ('series', 'movie')
    """).fetchone()
    last_run = conn.execute(
        "SELECT * FROM tmdb_run_log ORDER BY id DESC LIMIT 1"
    ).fetchone()

    return {
        "request": request,
        "current_user": current_user,
        "tmdb_enabled": cfg.get("enabled", False),
        "tmdb_api_key": cfg.get("api_key", ""),
        "tmdb_language": cfg.get("language", "en-US"),
        "tmdb_running": is_running(),
        "total": (counts["total"] or 0) if counts else 0,
        "enriched": (counts["enriched"] or 0) if counts else 0,
        "last_run": dict(last_run) if last_run else None,
        "flash": None,
        "error": None,
    }


@router.get("", response_class=HTMLResponse)
async def integrations_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        ctx = _page_ctx(conn, request, current_user)
    return templates.TemplateResponse("integrations/index.html", ctx)


@router.post("/tmdb/settings", response_class=HTMLResponse)
async def save_tmdb_settings(
    request: Request,
    enabled: str = Form(default=""),
    api_key: str = Form(default=""),
    language: str = Form(default="en-US"),
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        existing = _load_tmdb_settings(conn)
        new_key = api_key.strip()
        settings = {
            "enabled": enabled == "1",
            "api_key": new_key if new_key else existing.get("api_key", ""),
            "language": language.strip() or "en-US",
        }
        _save_tmdb_settings(conn, settings)

    with get_db() as conn:
        ctx = _page_ctx(conn, request, current_user)

    ctx["flash"] = "TMDB settings saved."
    logger.info("[INTEGRATIONS] TMDB settings updated by %s", current_user.username)
    return templates.TemplateResponse("integrations/index.html", ctx)


@router.get("/tmdb/status", response_class=JSONResponse)
async def tmdb_status(current_user: TokenData = Depends(get_current_user)):
    from app.tasks.tmdb import is_running

    with get_db() as conn:
        counts = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) AS enriched
            FROM entries
            WHERE type IN ('series', 'movie')
        """).fetchone()
        last_run = conn.execute(
            "SELECT * FROM tmdb_run_log ORDER BY id DESC LIMIT 1"
        ).fetchone()

    return JSONResponse({
        "running": is_running(),
        "total": (counts["total"] or 0) if counts else 0,
        "enriched": (counts["enriched"] or 0) if counts else 0,
        "last_run": dict(last_run) if last_run else None,
    })


@router.post("/tmdb/trigger", response_class=JSONResponse)
async def tmdb_trigger(current_user: TokenData = Depends(get_current_user)):
    from app.tasks.tmdb import trigger_tmdb_enrichment, is_running

    if is_running():
        return JSONResponse({"ok": False, "reason": "already_running"})

    started = trigger_tmdb_enrichment(triggered_by="manual")
    return JSONResponse({"ok": started, "reason": None if started else "disabled_or_no_key"})


@router.post("/tmdb/clear", response_class=JSONResponse)
async def tmdb_clear(current_user: TokenData = Depends(get_current_user)):
    from app.tasks.tmdb import clear_tmdb_metadata, is_running

    if is_running():
        return JSONResponse({"ok": False, "reason": "enrichment_running"})

    with get_db() as conn:
        clear_tmdb_metadata(conn)

    logger.info("[INTEGRATIONS] TMDB metadata cleared by %s", current_user.username)
    return JSONResponse({"ok": True})

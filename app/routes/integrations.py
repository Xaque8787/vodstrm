"""Integrations page — currently exposes TMDB enrichment controls."""
import logging
import os

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/integrations")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


@router.get("", response_class=HTMLResponse)
async def integrations_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.tmdb import _tmdb_enabled, _tmdb_api_key, _tmdb_language, is_running

    with get_db() as conn:
        counts = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) AS enriched
            FROM entries
            WHERE type IN ('series', 'movie')
        """).fetchone()
        last_run = conn.execute("""
            SELECT * FROM tmdb_run_log ORDER BY id DESC LIMIT 1
        """).fetchone()

    return templates.TemplateResponse(
        "integrations/index.html",
        {
            "request": request,
            "current_user": current_user,
            "tmdb_enabled": _tmdb_enabled(),
            "tmdb_key_set": bool(_tmdb_api_key()),
            "tmdb_language": _tmdb_language(),
            "tmdb_running": is_running(),
            "total": (counts["total"] or 0) if counts else 0,
            "enriched": (counts["enriched"] or 0) if counts else 0,
            "last_run": dict(last_run) if last_run else None,
        },
    )


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
        "total": counts["total"] if counts else 0,
        "enriched": counts["enriched"] if counts else 0,
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

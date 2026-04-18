import logging
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db
from app.models import ProviderM3UCreate, ProviderXtreamCreate

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/providers")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _provider_name_taken(name: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM providers WHERE name = ?", (name.strip(),)
        ).fetchone()
    return row is not None


def _list_providers() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, type, url, username, port, created_at FROM providers ORDER BY name"
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("", response_class=HTMLResponse)
async def providers_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    providers = _list_providers()
    return templates.TemplateResponse(
        "providers/index.html",
        {"request": request, "current_user": current_user, "providers": providers, "error": None},
    )


@router.post("/add/m3u", response_class=HTMLResponse)
async def add_m3u_provider(
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    current_user: TokenData = Depends(get_current_user),
):
    try:
        data = ProviderM3UCreate(name=name, url=url)
    except ValidationError as exc:
        error = exc.errors()[0]["msg"]
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),
                "error": error,
                "open_type": "m3u",
                "form_name": name,
                "form_url": url,
            },
            status_code=422,
        )

    if _provider_name_taken(data.name):
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),
                "error": f'A provider named "{data.name}" already exists.',
                "open_type": "m3u",
                "form_name": name,
                "form_url": url,
            },
            status_code=409,
        )

    with get_db() as conn:
        conn.execute(
            "INSERT INTO providers (name, type, url) VALUES (?, 'm3u', ?)",
            (data.name, data.url),
        )
    logger.info("Provider added (m3u): %s", data.name)
    return RedirectResponse("/providers", status_code=302)


@router.post("/add/xtream", response_class=HTMLResponse)
async def add_xtream_provider(
    request: Request,
    name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    port: str = Form(""),
    current_user: TokenData = Depends(get_current_user),
):
    try:
        data = ProviderXtreamCreate(name=name, username=username, password=password, port=port or None)
    except ValidationError as exc:
        error = exc.errors()[0]["msg"]
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),
                "error": error,
                "open_type": "xtream",
                "form_name": name,
                "form_username": username,
                "form_port": port,
            },
            status_code=422,
        )

    if _provider_name_taken(data.name):
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),
                "error": f'A provider named "{data.name}" already exists.',
                "open_type": "xtream",
                "form_name": name,
                "form_username": username,
                "form_port": port,
            },
            status_code=409,
        )

    with get_db() as conn:
        conn.execute(
            "INSERT INTO providers (name, type, username, password, port) VALUES (?, 'xtream', ?, ?, ?)",
            (data.name, data.username, data.password, data.port),
        )
    logger.info("Provider added (xtream): %s", data.name)
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_id}/delete")
async def delete_provider(
    provider_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        conn.execute("DELETE FROM providers WHERE id = ?", (provider_id,))
    logger.info("Provider deleted: id=%d by %s", provider_id, current_user.username)
    return RedirectResponse("/providers", status_code=302)

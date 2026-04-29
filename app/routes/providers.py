import logging
import os

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db
from app.models import (
    ProviderLocalFileCreate, ProviderLocalFileUpdate,
    ProviderM3UCreate, ProviderM3UUpdate,
    ProviderXtreamCreate, ProviderXtreamUpdate,
)
from app.utils.slugify import slugify

logger = logging.getLogger(__name__)

_M3U_DIR = os.getenv("M3U_DIR", "data/m3u")


def _default_browse_root() -> str:
    from app.utils.env import resolve_path
    return resolve_path(_M3U_DIR)

router = APIRouter(prefix="/providers")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


@router.get("/browse", response_class=JSONResponse)
async def browse_directory(
    path: str = Query(default=""),
    current_user: TokenData = Depends(get_current_user),
):
    root = _default_browse_root()
    target = os.path.realpath(path) if path else os.path.realpath(root)

    if not os.path.isdir(target):
        return JSONResponse({"error": "Not a directory"}, status_code=400)

    dirs = []
    files = []
    try:
        for entry in sorted(os.scandir(target), key=lambda e: (not e.is_dir(), e.name.lower())):
            if entry.is_dir(follow_symlinks=False):
                dirs.append({"name": entry.name, "path": entry.path})
            elif entry.is_file() and entry.name.lower().endswith(".m3u"):
                files.append({"name": entry.name, "path": entry.path})
    except PermissionError:
        return JSONResponse({"error": "Permission denied"}, status_code=403)

    parent = str(os.path.dirname(target)) if target != os.path.dirname(target) else None

    return JSONResponse({
        "current": target,
        "parent": parent,
        "dirs": dirs,
        "files": files,
    })


def _provider_name_taken(name: str, exclude_slug: str | None = None) -> bool:
    with get_db() as conn:
        if exclude_slug is not None:
            row = conn.execute(
                "SELECT 1 FROM providers WHERE name = ? AND slug != ?",
                (name.strip(), exclude_slug),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT 1 FROM providers WHERE name = ?", (name.strip(),)
            ).fetchone()
    return row is not None


def _list_providers() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, slug, type, url, username, port, stream_format, is_active, priority, local_file_path, created_at FROM providers ORDER BY priority, name"
        ).fetchall()
    return [dict(r) for r in rows]


def _get_provider_by_slug(slug: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, name, slug, type, url, username, password, port, stream_format, is_active, created_at FROM providers WHERE slug = ?",
            (slug,),
        ).fetchone()
    return dict(row) if row else None


@router.get("", response_class=HTMLResponse)
async def providers_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    providers = _list_providers()
    return templates.TemplateResponse(
        "providers/index.html",
        {
            "request": request,
            "current_user": current_user,
            "providers": providers,
            "error": None,
        },
    )


@router.post("/add/m3u", response_class=HTMLResponse)
async def add_m3u_provider(
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    priority: int = Form(10),
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

    slug = slugify(data.name)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO providers (name, slug, type, url, priority) VALUES (?, ?, 'm3u', ?, ?)",
            (data.name, slug, data.url, max(1, priority)),
        )
    logger.info("Provider added (m3u): %s", data.name)
    return RedirectResponse("/providers", status_code=302)


@router.post("/add/xtream", response_class=HTMLResponse)
async def add_xtream_provider(
    request: Request,
    name: str = Form(...),
    server_scheme: str = Form("https://"),
    server_url: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    port: str = Form(""),
    stream_format: str = Form("ts"),
    priority: int = Form(10),
    current_user: TokenData = Depends(get_current_user),
):
    try:
        data = ProviderXtreamCreate(
            name=name, server_scheme=server_scheme, server_url=server_url,
            username=username, password=password,
            port=port or None, stream_format=stream_format,
        )
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
                "form_server_scheme": server_scheme,
                "form_server_url": server_url,
                "form_username": username,
                "form_port": port,
                "form_stream_format": stream_format,
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
                "form_server_scheme": server_scheme,
                "form_server_url": server_url,
                "form_username": username,
                "form_port": port,
                "form_stream_format": stream_format,
            },
            status_code=409,
        )

    slug = slugify(data.name)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO providers (name, slug, type, url, username, password, port, stream_format, priority) VALUES (?, ?, 'xtream', ?, ?, ?, ?, ?, ?)",
            (data.name, slug, data.full_server_url(), data.username, data.password, data.port, data.stream_format, max(1, priority)),
        )
    logger.info("Provider added (xtream): %s", data.name)
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_slug}/edit/m3u", response_class=HTMLResponse)
async def edit_m3u_provider(
    provider_slug: str,
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    priority: int = Form(10),
    current_user: TokenData = Depends(get_current_user),
):
    provider = _get_provider_by_slug(provider_slug)
    if not provider or provider["type"] != "m3u":
        return RedirectResponse("/providers", status_code=302)

    try:
        data = ProviderM3UUpdate(name=name, url=url)
    except ValidationError as exc:
        error = exc.errors()[0]["msg"]
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),

                "error": error,
                "edit_provider_slug": provider_slug,
            },
            status_code=422,
        )

    if _provider_name_taken(data.name, exclude_slug=provider_slug):
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),

                "error": f'A provider named "{data.name}" already exists.',
                "edit_provider_slug": provider_slug,
            },
            status_code=409,
        )

    new_slug = slugify(data.name)
    with get_db() as conn:
        conn.execute(
            "UPDATE providers SET name = ?, slug = ?, url = ?, priority = ? WHERE slug = ?",
            (data.name, new_slug, data.url, max(1, priority), provider_slug),
        )
    logger.info("Provider updated (m3u): %s by %s", provider_slug, current_user.username)
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_slug}/edit/xtream", response_class=HTMLResponse)
async def edit_xtream_provider(
    provider_slug: str,
    request: Request,
    name: str = Form(...),
    server_scheme: str = Form("https://"),
    server_url: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    port: str = Form(""),
    stream_format: str = Form("ts"),
    priority: int = Form(10),
    current_user: TokenData = Depends(get_current_user),
):
    provider = _get_provider_by_slug(provider_slug)
    if not provider or provider["type"] != "xtream":
        return RedirectResponse("/providers", status_code=302)

    try:
        data = ProviderXtreamUpdate(
            name=name, server_scheme=server_scheme, server_url=server_url,
            username=username, password=password,
            port=port or None, stream_format=stream_format,
        )
    except ValidationError as exc:
        error = exc.errors()[0]["msg"]
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),
                "error": error,
                "edit_provider_slug": provider_slug,
            },
            status_code=422,
        )

    if _provider_name_taken(data.name, exclude_slug=provider_slug):
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),
                "error": f'A provider named "{data.name}" already exists.',
                "edit_provider_slug": provider_slug,
            },
            status_code=409,
        )

    new_slug = slugify(data.name)
    with get_db() as conn:
        conn.execute(
            "UPDATE providers SET name = ?, slug = ?, url = ?, username = ?, password = ?, port = ?, stream_format = ?, priority = ? WHERE slug = ?",
            (data.name, new_slug, data.full_server_url(), data.username, data.password, data.port, data.stream_format, max(1, priority), provider_slug),
        )
    logger.info("Provider updated (xtream): %s by %s", provider_slug, current_user.username)
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_slug}/toggle")
async def toggle_provider(
    provider_slug: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Re-enable a previously disabled provider (is_active 0 → 1 only)."""
    with get_db() as conn:
        before = conn.execute(
            "SELECT is_active FROM providers WHERE slug = ?", (provider_slug,)
        ).fetchone()
        if before and not before["is_active"]:
            conn.execute(
                "UPDATE providers SET is_active = 1 WHERE slug = ?", (provider_slug,)
            )
    logger.info("Provider re-enabled: %s by %s", provider_slug, current_user.username)
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_slug}/disable")
async def disable_provider(
    provider_slug: str,
    current_user: TokenData = Depends(get_current_user),
):
    """
    Disable a provider and immediately remove all its streams and orphaned
    entries from the database. .strm files owned by this provider are
    handed over to the next eligible provider or deleted.
    """
    from app.tasks.strm import deactivate_provider_strm
    from app.tasks.live_m3u import deactivate_provider_live_m3u
    from app.ingestion.sync import purge_provider_data

    # Mark inactive first so the STRM handover excludes this provider from
    # replacement candidates, then purge the DB rows.
    with get_db() as conn:
        row = conn.execute(
            "SELECT is_active FROM providers WHERE slug = ?", (provider_slug,)
        ).fetchone()
        if not row:
            return RedirectResponse("/providers", status_code=302)
        conn.execute(
            "UPDATE providers SET is_active = 0 WHERE slug = ?", (provider_slug,)
        )

    # STRM handover: hand files to next eligible provider or delete them.
    deactivate_provider_strm(provider_slug)
    deactivate_provider_live_m3u(provider_slug)

    # Purge DB streams + orphaned entries for this provider.
    with get_db() as conn:
        streams_deleted, entries_deleted = purge_provider_data(conn, provider_slug)

    logger.info(
        "Provider disabled+purged: %s  streams=%d  entries=%d  by %s",
        provider_slug, streams_deleted, entries_deleted, current_user.username,
    )
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_slug}/delete")
async def delete_provider(
    provider_slug: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.strm import deactivate_provider_strm
    from app.tasks.live_m3u import deactivate_provider_live_m3u

    # Run handover/cleanup before touching the DB so replacements can still be
    # found and files are dealt with while stream rows still exist.
    deactivate_provider_strm(provider_slug)
    deactivate_provider_live_m3u(provider_slug)

    with get_db() as conn:
        conn.execute("DELETE FROM providers WHERE slug = ?", (provider_slug,))
        conn.execute("DELETE FROM streams WHERE provider = ?", (provider_slug,))
        conn.execute(
            "DELETE FROM entries WHERE entry_id NOT IN (SELECT DISTINCT entry_id FROM streams)"
        )
    logger.info("Provider deleted: %s by %s", provider_slug, current_user.username)
    return RedirectResponse("/providers", status_code=302)


@router.post("/add/local_file", response_class=HTMLResponse)
async def add_local_file_provider(
    request: Request,
    name: str = Form(...),
    local_file_path: str = Form(...),
    priority: int = Form(10),
    current_user: TokenData = Depends(get_current_user),
):
    try:
        data = ProviderLocalFileCreate(name=name, local_file_path=local_file_path)
    except ValidationError as exc:
        error = exc.errors()[0]["msg"]
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),

                "error": error,
                "open_type": "local_file",
                "form_name": name,
                "form_local_file_path": local_file_path,
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
                "open_type": "local_file",
                "form_name": name,
                "form_local_file_path": local_file_path,
            },
            status_code=409,
        )

    slug = slugify(data.name)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO providers (name, slug, type, local_file_path, priority) VALUES (?, ?, 'local_file', ?, ?)",
            (data.name, slug, data.local_file_path, max(1, priority)),
        )
    logger.info("Provider added (local_file): %s → %s", data.name, data.local_file_path)
    return RedirectResponse("/providers", status_code=302)


@router.post("/{provider_slug}/edit/local_file", response_class=HTMLResponse)
async def edit_local_file_provider(
    provider_slug: str,
    request: Request,
    name: str = Form(...),
    local_file_path: str = Form(...),
    priority: int = Form(10),
    current_user: TokenData = Depends(get_current_user),
):
    provider = _get_provider_by_slug(provider_slug)
    if not provider or provider["type"] != "local_file":
        return RedirectResponse("/providers", status_code=302)

    try:
        data = ProviderLocalFileUpdate(name=name, local_file_path=local_file_path)
    except ValidationError as exc:
        error = exc.errors()[0]["msg"]
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),

                "error": error,
                "edit_provider_slug": provider_slug,
            },
            status_code=422,
        )

    if _provider_name_taken(data.name, exclude_slug=provider_slug):
        return templates.TemplateResponse(
            "providers/index.html",
            {
                "request": request,
                "current_user": current_user,
                "providers": _list_providers(),

                "error": f'A provider named "{data.name}" already exists.',
                "edit_provider_slug": provider_slug,
            },
            status_code=409,
        )

    new_slug = slugify(data.name)
    with get_db() as conn:
        conn.execute(
            "UPDATE providers SET name = ?, slug = ?, local_file_path = ?, priority = ? WHERE slug = ?",
            (data.name, new_slug, data.local_file_path, max(1, priority), provider_slug),
        )
    logger.info("Provider updated (local_file): %s by %s", provider_slug, current_user.username)
    return RedirectResponse("/providers", status_code=302)

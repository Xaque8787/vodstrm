"""Filter management routes — CRUD + reapply action."""
import logging
import os

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db
from app.filters.query import (
    create_filter, delete_filter, get_filter,
    list_filters, list_provider_slugs, toggle_filter, update_filter,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/filters")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_FILTER_TYPES = ["remove", "exclude", "include_only", "replace"]
_ENTRY_TYPES = ["movie", "series", "live", "tv_vod", "unsorted"]
_FILTER_TYPE_LABELS = {
    "remove": "Remove Terms",
    "exclude": "Exclude Terms",
    "include_only": "Include Only",
    "replace": "Replace Terms",
}


def _page_ctx(conn, *, error=None, open_id=None, flash=None):
    return {
        "filters": list_filters(conn),
        "provider_slugs": list_provider_slugs(conn),
        "filter_types": _FILTER_TYPES,
        "entry_types": _ENTRY_TYPES,
        "filter_type_labels": _FILTER_TYPE_LABELS,
        "error": error,
        "open_id": open_id,
        "flash": flash,
    }


def _multivalue(items: list[tuple], key: str) -> list[str]:
    return [v for k, v in items if k == key and v]


def _parse_patterns(items: list[tuple], filter_type: str) -> list[dict]:
    by_idx: dict[int, dict] = {}
    for k, v in items:
        if k.startswith("pattern_"):
            try:
                idx = int(k.split("_", 1)[1])
            except ValueError:
                continue
            by_idx.setdefault(idx, {})["pattern"] = v.strip()
        elif k.startswith("replacement_") and filter_type == "replace":
            try:
                idx = int(k.split("_", 1)[1])
            except ValueError:
                continue
            by_idx.setdefault(idx, {})["replacement"] = v.strip()

    patterns = []
    for idx in sorted(by_idx):
        pat = by_idx[idx].get("pattern", "").strip()
        if pat:
            entry = {"pattern": pat}
            if filter_type == "replace":
                entry["replacement"] = by_idx[idx].get("replacement", "")
            patterns.append(entry)
    return patterns


@router.get("", response_class=HTMLResponse)
async def filters_page(request: Request, flash: str = "", current_user: TokenData = Depends(get_current_user)):
    with get_db() as conn:
        ctx = _page_ctx(conn, flash=flash or None)
    return templates.TemplateResponse("filters/index.html", {"request": request, "current_user": current_user, **ctx})


@router.post("/add", response_class=HTMLResponse)
async def add_filter(request: Request, current_user: TokenData = Depends(get_current_user)):
    form = await request.form()
    items = form.multi_items()
    form_dict = dict(items)

    filter_type = (form_dict.get("filter_type") or "").strip()
    label = (form_dict.get("label") or "").strip()
    try:
        order_index = int(form_dict.get("order_index") or 0)
    except ValueError:
        order_index = 0

    if filter_type not in _FILTER_TYPES:
        with get_db() as conn:
            ctx = _page_ctx(conn, error="Invalid filter type.")
        return templates.TemplateResponse("filters/index.html",
            {"request": request, "current_user": current_user, **ctx}, status_code=422)

    providers = _multivalue(items, "providers") or ["*"]
    entry_types = _multivalue(items, "entry_types") or ["*"]
    patterns = _parse_patterns(items, filter_type)

    if not patterns:
        with get_db() as conn:
            ctx = _page_ctx(conn, error="At least one pattern is required.")
        return templates.TemplateResponse("filters/index.html",
            {"request": request, "current_user": current_user, **ctx}, status_code=422)

    with get_db() as conn:
        create_filter(conn, filter_type, label, order_index, providers, entry_types, patterns)
    logger.info("[FILTERS] Created type=%s label=%r by %s", filter_type, label, current_user.username)
    return RedirectResponse("/filters?flash=created", status_code=302)


@router.post("/{filter_id}/edit", response_class=HTMLResponse)
async def edit_filter(filter_id: int, request: Request, current_user: TokenData = Depends(get_current_user)):
    form = await request.form()
    items = form.multi_items()
    form_dict = dict(items)

    label = (form_dict.get("label") or "").strip()
    try:
        order_index = int(form_dict.get("order_index") or 0)
    except ValueError:
        order_index = 0

    with get_db() as conn:
        f = get_filter(conn, filter_id)
    if not f:
        return RedirectResponse("/filters", status_code=302)

    filter_type = f["filter_type"]
    providers = _multivalue(items, "providers") or ["*"]
    entry_types = _multivalue(items, "entry_types") or ["*"]
    patterns = _parse_patterns(items, filter_type)

    if not patterns:
        with get_db() as conn:
            ctx = _page_ctx(conn, error="At least one pattern is required.", open_id=filter_id)
        return templates.TemplateResponse("filters/index.html",
            {"request": request, "current_user": current_user, **ctx}, status_code=422)

    with get_db() as conn:
        update_filter(conn, filter_id, label, order_index, providers, entry_types, patterns)
    logger.info("[FILTERS] Updated filter %d by %s", filter_id, current_user.username)
    return RedirectResponse("/filters?flash=updated", status_code=302)


@router.post("/{filter_id}/toggle")
async def toggle_filter_route(filter_id: int, current_user: TokenData = Depends(get_current_user)):
    with get_db() as conn:
        toggle_filter(conn, filter_id)
    return RedirectResponse("/filters", status_code=302)


@router.post("/{filter_id}/delete")
async def delete_filter_route(filter_id: int, current_user: TokenData = Depends(get_current_user)):
    with get_db() as conn:
        delete_filter(conn, filter_id)
    logger.info("[FILTERS] Deleted filter %d by %s", filter_id, current_user.username)
    return RedirectResponse("/filters?flash=deleted", status_code=302)


@router.post("/reapply")
async def reapply_filters(request: Request, current_user: TokenData = Depends(get_current_user)):
    form = await request.form()
    provider_slug = (form.get("provider") or "").strip() or None

    from app.filters.engine import load_filters, run_filters_for_provider
    with get_db() as conn:
        filters = load_filters(conn)
        updated = run_filters_for_provider(conn, filters, provider=provider_slug)

    logger.info("[FILTERS] Reapply — provider=%s streams=%d by=%s", provider_slug or "*", updated, current_user.username)
    return RedirectResponse(f"/filters?flash=reapplied&count={updated}", status_code=302)

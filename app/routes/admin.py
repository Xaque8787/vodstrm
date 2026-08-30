import hashlib
import logging
import os

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from app.auth.jwt_handler import TokenData, get_current_admin
from app.database import get_db
from app.template_engine import create_templates
from app.tasks.strm import _remove_empty_dirs, _vod_root
from app.utils.log_reader import (
    LOG_LEVELS,
    available_log_files,
    cleanup_log_file,
    read_log_entries,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin")
templates = create_templates()

_LIBRARY_PAGE_SIZE = 100

_ENTRIES_SORT_COLS = {
    "type":         "e.type",
    "cleaned_title":"e.cleaned_title",
    "year":         "e.year",
    "season":       "e.season",
    "episode":      "e.episode",
    "air_date":     "e.air_date",
    "created_at":   "e.created_at",
}

_STREAMS_SORT_COLS = {
    "stream_id":      "s.stream_id",
    "provider":       "s.provider",
    "cleaned_title":  "e.cleaned_title",
    "exclude":        "s.exclude",
    "include_only":   "s.include_only",
    "ingested_at":    "s.ingested_at",
}


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
# LOG VIEWER
# ---------------------------------------------------------------------------

@router.get("/logs", response_class=HTMLResponse)
async def logs_page(
    request: Request,
    current_user: TokenData = Depends(get_current_admin),
):
    return templates.TemplateResponse(
        "admin/logs.html",
        {
            "request": request,
            "current_user": current_user,
            "log_files": available_log_files(),
            "log_levels": LOG_LEVELS,
        },
    )


@router.get("/logs/data", response_class=JSONResponse)
async def logs_data(
    file: str = Query(default="app.log"),
    level: str = Query(default="ALL"),
    search: str = Query(default=""),
    limit: int = Query(default=250, ge=50, le=2000),
    current_user: TokenData = Depends(get_current_admin),
):
    try:
        entries = read_log_entries(file, level, search, limit)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(
        {
            "entries": entries,
            "files": available_log_files(),
            "selected_file": file,
            "level": level.upper(),
            "limit": limit,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/logs/cleanup", response_class=JSONResponse)
async def cleanup_logs(
    file: str = Form(...),
    current_user: TokenData = Depends(get_current_admin),
):
    try:
        result = cleanup_log_file(file)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except OSError:
        logger.exception("[ADMIN] Could not clean up log file=%s", file)
        return JSONResponse({"error": "Could not clean up log file"}, status_code=500)
    logger.warning(
        "[ADMIN] Log %s by=%s file=%s bytes_removed=%d",
        result["action"],
        current_user.username,
        result["name"],
        result["bytes_removed"],
    )
    return JSONResponse({"ok": True, **result, "files": available_log_files()})


# ---------------------------------------------------------------------------
# LIBRARY INSPECTION / DEBUG
# ---------------------------------------------------------------------------

@router.get("/database", response_class=HTMLResponse)
async def database_page(
    request: Request,
    tab: str = "entries",
    page: int = 1,
    q: str = "",
    sort: str = "",
    order: str = "asc",
    current_user: TokenData = Depends(get_current_admin),
):
    tab   = tab if tab in ("entries", "streams") else "entries"
    page  = max(1, page)
    order = "asc" if order not in ("asc", "desc") else order
    offset = (page - 1) * _LIBRARY_PAGE_SIZE
    search = q.strip()

    with get_db() as conn:
        entry_count  = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        stream_count = conn.execute("SELECT COUNT(*) FROM streams").fetchone()[0]

        streams_by_entry: dict[str, list] = {}

        if tab == "entries":
            sort_col = _ENTRIES_SORT_COLS.get(sort, "e.type")
            order_sql = "DESC" if order == "desc" else "ASC"
            where_sql  = ""
            params: list = []
            if search:
                where_sql = "WHERE (e.cleaned_title LIKE ? OR e.type LIKE ? OR CAST(e.year AS TEXT) LIKE ?)"
                like = f"%{search}%"
                params = [like, like, like]

            count_sql = f"SELECT COUNT(*) FROM entries e {where_sql}"
            total = conn.execute(count_sql, params).fetchone()[0]

            rows = conn.execute(
                f"""
                SELECT e.entry_id, e.type, e.cleaned_title, e.raw_title,
                       e.year, e.season, e.episode, e.air_date,
                       e.created_at
                FROM entries e
                {where_sql}
                ORDER BY {sort_col} {order_sql}
                LIMIT ? OFFSET ?
                """,
                params + [_LIBRARY_PAGE_SIZE, offset],
            ).fetchall()

            entry_ids = [r["entry_id"] for r in rows]
            if entry_ids:
                placeholders = ",".join("?" * len(entry_ids))
                stream_rows = conn.execute(
                    f"""
                    SELECT entry_id, stream_id, provider, stream_url,
                           filtered_title, exclude, include_only, ingested_at
                    FROM streams
                    WHERE entry_id IN ({placeholders})
                    ORDER BY provider ASC
                    """,
                    entry_ids,
                ).fetchall()
                for sr in stream_rows:
                    streams_by_entry.setdefault(sr["entry_id"], []).append(dict(sr))

        else:
            sort_col  = _STREAMS_SORT_COLS.get(sort, "e.cleaned_title")
            order_sql = "DESC" if order == "desc" else "ASC"
            where_sql  = ""
            params = []
            if search:
                where_sql = "WHERE (e.cleaned_title LIKE ? OR s.provider LIKE ? OR s.stream_url LIKE ?)"
                like = f"%{search}%"
                params = [like, like, like]

            count_sql = f"""
                SELECT COUNT(*) FROM streams s
                JOIN entries e ON e.entry_id = s.entry_id
                {where_sql}
            """
            total = conn.execute(count_sql, params).fetchone()[0]

            rows = conn.execute(
                f"""
                SELECT s.stream_id, s.entry_id, s.provider, s.stream_url,
                       s.ingested_at, s.metadata_json,
                       e.cleaned_title,
                       s.filtered_title, s.filter_hits, s.exclude, s.include_only
                FROM streams s
                JOIN entries e ON e.entry_id = s.entry_id
                {where_sql}
                ORDER BY {sort_col} {order_sql}
                LIMIT ? OFFSET ?
                """,
                params + [_LIBRARY_PAGE_SIZE, offset],
            ).fetchall()

    total_pages = max(1, (total + _LIBRARY_PAGE_SIZE - 1) // _LIBRARY_PAGE_SIZE)
    data = [dict(r) for r in rows]

    flash = request.query_params.get("flash")

    return templates.TemplateResponse(
        "admin/library.html",
        {
            "request":          request,
            "current_user":     current_user,
            "tab":              tab,
            "rows":             data,
            "streams_by_entry": streams_by_entry if tab == "entries" else {},
            "entry_count":      entry_count,
            "stream_count":     stream_count,
            "page":             page,
            "total_pages":      total_pages,
            "total":            total,
            "flash":            flash,
            "q":                search,
            "sort":             sort,
            "order":            order,
        },
    )


@router.get("/library", include_in_schema=False)
async def legacy_library_page(
    request: Request,
    current_user: TokenData = Depends(get_current_admin),
):
    target = "/admin/database"
    if request.url.query:
        target = f"{target}?{request.url.query}"
    return RedirectResponse(target, status_code=308)


def _delete_strm_files(conn) -> int:
    """Delete all .strm files referenced in streams.strm_path. Returns count deleted."""
    paths = conn.execute(
        "SELECT strm_path FROM streams WHERE strm_path IS NOT NULL"
    ).fetchall()
    deleted = 0
    seen_dirs: set[str] = set()
    for (path,) in paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
                deleted += 1
                seen_dirs.add(os.path.dirname(path))
            except OSError as exc:
                logger.warning("[ADMIN] Could not delete strm file %s: %s", path, exc)
    for d in seen_dirs:
        _remove_empty_dirs(d)
    return deleted


def _clear_xtream_series_data(conn) -> tuple[int, int]:
    """Clear summary and episode caches that independently feed Library TV cards."""
    catalog = conn.execute("DELETE FROM xtream_series_catalog").rowcount
    cache = conn.execute("DELETE FROM xtream_series_cache").rowcount
    return catalog, cache


@router.post("/library/clear/entries", include_in_schema=False)
@router.post("/database/clear/entries")
async def clear_entries(current_user: TokenData = Depends(get_current_admin)):
    with get_db() as conn:
        deleted = _delete_strm_files(conn)
        catalog, cache = _clear_xtream_series_data(conn)
        conn.execute("DELETE FROM streams")
        conn.execute("DELETE FROM entries")
    logger.warning(
        "[ADMIN] library data cleared by '%s' "
        "(strm files=%d series_catalog=%d series_cache=%d)",
        current_user.username, deleted, catalog, cache,
    )
    return RedirectResponse("/admin/database?flash=cleared&tab=entries", status_code=302)


@router.post("/library/clear/streams", include_in_schema=False)
@router.post("/database/clear/streams")
async def clear_streams(current_user: TokenData = Depends(get_current_admin)):
    with get_db() as conn:
        deleted = _delete_strm_files(conn)
        catalog, cache = _clear_xtream_series_data(conn)
        conn.execute("DELETE FROM streams")
    logger.warning(
        "[ADMIN] streams and native series data cleared by '%s' "
        "(strm files=%d series_catalog=%d series_cache=%d)",
        current_user.username, deleted, catalog, cache,
    )
    return RedirectResponse("/admin/database?flash=streams_cleared&tab=streams", status_code=302)

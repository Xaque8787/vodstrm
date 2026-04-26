import logging
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_user
from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/schedules")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _list_providers() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, slug, type, is_active, strm_mode FROM providers ORDER BY name"
        ).fetchall()
    return [dict(r) for r in rows]


def _list_schedules() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM task_schedules ORDER BY label").fetchall()
    return [dict(r) for r in rows]


def _get_schedule(task_id: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM task_schedules WHERE task_id = ?", (task_id,)
        ).fetchone()
    return dict(row) if row else None


def _upsert_schedule(
    task_id: str,
    provider_slug: str | None,
    task_type: str,
    label: str,
    trigger_type: str,
    cron_expression: str | None,
    interval_seconds: int | None,
    enabled: bool,
) -> None:
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM task_schedules WHERE task_id = ?", (task_id,)
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE task_schedules
                SET provider_slug = ?, task_type = ?, label = ?,
                    trigger_type = ?, cron_expression = ?,
                    interval_seconds = ?, enabled = ?,
                    updated_at = datetime('now')
                WHERE task_id = ?
                """,
                (provider_slug, task_type, label, trigger_type,
                 cron_expression, interval_seconds, 1 if enabled else 0, task_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO task_schedules
                    (task_id, provider_slug, task_type, label, trigger_type,
                     cron_expression, interval_seconds, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (task_id, provider_slug, task_type, label, trigger_type,
                 cron_expression, interval_seconds, 1 if enabled else 0),
            )


def _apply_schedule_to_scheduler(schedule: dict) -> None:
    from app.scheduler import get_scheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = get_scheduler()
    task_id = schedule["task_id"]

    if not schedule["enabled"]:
        try:
            scheduler.remove_job(task_id)
            logger.info("[SCHEDULER] Job removed from job store: '%s'", task_id)
        except Exception:
            logger.info("[SCHEDULER] Job '%s' was not in job store (nothing to remove)", task_id)
        return

    fn = _resolve_task_fn(schedule["task_type"], schedule.get("provider_slug"))
    if fn is None:
        logger.warning("[SCHEDULER] No task function found for task_type='%s' — job not registered", schedule["task_type"])
        return

    if schedule["trigger_type"] == "cron":
        trigger = CronTrigger.from_crontab(schedule["cron_expression"])
        trigger_desc = f"cron '{schedule['cron_expression']}'"
    else:
        trigger = IntervalTrigger(seconds=schedule["interval_seconds"])
        trigger_desc = f"interval {schedule['interval_seconds']}s"

    job = scheduler.add_job(fn, trigger=trigger, id=task_id, replace_existing=True)
    next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S %Z") if job.next_run_time else "unknown"
    logger.info(
        "[SCHEDULER] Job registered: '%s' trigger=%s next_run=%s",
        task_id, trigger_desc, next_run,
    )


def _resolve_task_fn(task_type: str, provider_slug: str | None):
    from app.tasks.downloader import download_all_providers
    from app.tasks.strm import clean_strm_orphans, generate_strm

    if task_type == "download_all_providers":
        return download_all_providers

    if task_type == "clean_strm_orphans":
        return clean_strm_orphans

    if task_type == "generate_strm":
        return generate_strm

    return None


# ---------------------------------------------------------------------------
# ROUTES — page
# ---------------------------------------------------------------------------

@router.get("", response_class=HTMLResponse)
async def schedules_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    providers = _list_providers()

    from app.scheduler import get_scheduler
    scheduler = get_scheduler()
    scheduler_jobs = {job.id: job for job in scheduler.get_jobs()}

    _GLOBAL_TASKS = [
        {"task_type": "download_all_providers", "label": "Download All Active Providers"},
        {"task_type": "clean_strm_orphans",     "label": "Clean Orphaned STRM Files"},
    ]

    all_schedules = _list_schedules()
    schedules_by_task_id: dict[str, dict] = {}
    for s in all_schedules:
        job = scheduler_jobs.get(s["task_id"])
        s["next_run"] = (
            job.next_run_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            if job and job.next_run_time
            else None
        )
        schedules_by_task_id[s["task_id"]] = s

    global_tasks = []
    for gt in _GLOBAL_TASKS:
        task_id = f"global:{gt['task_type']}"
        global_tasks.append({
            "task_type": gt["task_type"],
            "label": gt["label"],
            "task_id": task_id,
            "sched": schedules_by_task_id.get(task_id),
        })

    return templates.TemplateResponse(
        "schedules/index.html",
        {
            "request": request,
            "current_user": current_user,
            "providers": providers,
            "global_tasks": global_tasks,
            "tz": os.getenv("TZ", "America/Los_Angeles"),
        },
    )


# ---------------------------------------------------------------------------
# ROUTES — global schedule
# ---------------------------------------------------------------------------

@router.post("/global/{task_type}/save")
async def save_global_schedule(
    task_type: str,
    trigger_type: str = Form("cron"),
    cron_expression: str = Form(""),
    interval_seconds: str = Form(""),
    enabled: str = Form("off"),
    current_user: TokenData = Depends(get_current_user),
):
    _GLOBAL_LABELS = {
        "download_all_providers": "Download All Active Providers",
        "clean_strm_orphans":     "Clean Orphaned STRM Files",
    }
    if task_type not in _GLOBAL_LABELS:
        return RedirectResponse("/schedules", status_code=302)

    task_id = f"global:{task_type}"
    is_enabled = enabled.lower() in ("on", "true", "1", "yes")
    cron_val = cron_expression.strip() or None
    interval_val = int(interval_seconds.strip()) if interval_seconds.strip().isdigit() else None

    trigger_desc = cron_val if trigger_type == "cron" else f"{interval_val}s"
    logger.info(
        "[SCHEDULES] Saving global schedule: task_type='%s' enabled=%s trigger=%s %s (user=%s)",
        task_type, is_enabled, trigger_type, trigger_desc, current_user.username,
    )

    _upsert_schedule(
        task_id=task_id,
        provider_slug=None,
        task_type=task_type,
        label=_GLOBAL_LABELS[task_type],
        trigger_type=trigger_type,
        cron_expression=cron_val,
        interval_seconds=interval_val,
        enabled=is_enabled,
    )

    schedule = _get_schedule(task_id)
    if schedule:
        _apply_schedule_to_scheduler(schedule)

    logger.info("[SCHEDULES] Global schedule saved: '%s' enabled=%s (user=%s)", task_type, is_enabled, current_user.username)
    return RedirectResponse("/schedules", status_code=302)


@router.post("/global/{task_type}/run-now")
async def run_global_now(
    task_type: str,
    current_user: TokenData = Depends(get_current_user),
):
    fn = _resolve_task_fn(task_type, None)
    if fn is not None:
        import threading
        threading.Thread(target=fn, daemon=True).start()
        logger.info("Manual global trigger: %s by %s", task_type, current_user.username)
    return RedirectResponse("/schedules", status_code=302)


# ---------------------------------------------------------------------------
# ROUTES — provider actions
# ---------------------------------------------------------------------------

@router.post("/provider/{provider_slug}/run-now")
async def run_provider_now(
    provider_slug: str,
    current_user: TokenData = Depends(get_current_user),
):
    from app.tasks.downloader import download_provider
    import threading
    threading.Thread(target=download_provider, args=(provider_slug,), daemon=True).start()
    logger.info("Manual download trigger: provider=%s by %s", provider_slug, current_user.username)
    return RedirectResponse("/schedules", status_code=302)


@router.post("/provider/{provider_slug}/toggle")
async def toggle_provider(
    provider_slug: str,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        before = conn.execute(
            "SELECT is_active FROM providers WHERE slug = ?", (provider_slug,)
        ).fetchone()
        conn.execute(
            "UPDATE providers SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE slug = ?",
            (provider_slug,),
        )

    now_inactive = before and bool(before["is_active"])
    if now_inactive:
        from app.tasks.strm import deactivate_provider_strm_async
        import threading
        threading.Thread(
            target=deactivate_provider_strm_async, args=(provider_slug,), daemon=True
        ).start()
        logger.info(
            "STRM handover triggered for provider '%s' (deactivated) by %s",
            provider_slug, current_user.username,
        )

    logger.info("Provider toggled: %s by %s", provider_slug, current_user.username)
    return RedirectResponse("/schedules", status_code=302)


@router.post("/provider/{provider_slug}/strm-mode")
async def set_strm_mode(
    provider_slug: str,
    strm_mode: str = Form(...),
    current_user: TokenData = Depends(get_current_user),
):
    if strm_mode not in ("generate_all", "import_selected"):
        return RedirectResponse("/schedules", status_code=302)

    with get_db() as conn:
        current = conn.execute(
            "SELECT strm_mode FROM providers WHERE slug = ?", (provider_slug,)
        ).fetchone()

    if not current:
        return RedirectResponse("/schedules", status_code=302)

    switching_to_selected = (
        current["strm_mode"] != "import_selected"
        and strm_mode == "import_selected"
    )

    with get_db() as conn:
        conn.execute(
            "UPDATE providers SET strm_mode = ? WHERE slug = ?",
            (strm_mode, provider_slug),
        )

    if switching_to_selected:
        from app.tasks.strm import deactivate_provider_strm_async
        import threading
        threading.Thread(
            target=deactivate_provider_strm_async,
            args=(provider_slug,),
            daemon=True,
        ).start()
        logger.info(
            "STRM handover triggered for provider '%s' (switched to import_selected) by %s",
            provider_slug, current_user.username,
        )

    logger.info(
        "Provider strm_mode set to '%s': %s by %s",
        strm_mode, provider_slug, current_user.username,
    )
    return RedirectResponse("/schedules", status_code=302)

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


def _list_providers() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, slug, type, is_active FROM providers ORDER BY name"
        ).fetchall()
    return [dict(r) for r in rows]


def _list_schedules() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM task_schedules ORDER BY label"
        ).fetchall()
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
            logger.info("Removed scheduler job: %s", task_id)
        except Exception:
            pass
        return

    fn = _resolve_task_fn(schedule["task_type"], schedule["provider_slug"])
    if fn is None:
        logger.warning("No task function found for task_type=%s", schedule["task_type"])
        return

    if schedule["trigger_type"] == "cron":
        trigger = CronTrigger.from_crontab(schedule["cron_expression"])
    else:
        trigger = IntervalTrigger(seconds=schedule["interval_seconds"])

    scheduler.add_job(
        fn,
        trigger=trigger,
        id=task_id,
        replace_existing=True,
    )
    logger.info("Scheduled job applied: %s", task_id)


def _resolve_task_fn(task_type: str, provider_slug: str | None):
    from app.tasks.downloader import download_provider, download_all_providers

    if task_type in ("m3u_download", "xtream_download") and provider_slug:
        import functools
        return functools.partial(download_provider, provider_slug)

    if task_type == "download_all_providers":
        return download_all_providers

    return None


@router.get("", response_class=HTMLResponse)
async def schedules_page(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    schedules = _list_schedules()
    providers = _list_providers()

    from app.scheduler import get_scheduler
    scheduler = get_scheduler()
    scheduler_jobs = {job.id: job for job in scheduler.get_jobs()}

    _GLOBAL_TASKS = [
        {"task_type": "download_all_providers", "label": "Download All Active Providers"},
    ]

    schedules_with_next = []
    for s in schedules:
        job = scheduler_jobs.get(s["task_id"])
        s["next_run"] = (
            job.next_run_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            if job and job.next_run_time
            else None
        )
        schedules_with_next.append(s)

    schedules_by_task_id = {s["task_id"]: s for s in schedules_with_next}

    global_tasks = []
    for gt in _GLOBAL_TASKS:
        task_id = f"global:{gt['task_type']}"
        sched = schedules_by_task_id.get(task_id)
        global_tasks.append({
            "task_type": gt["task_type"],
            "label": gt["label"],
            "task_id": task_id,
            "sched": sched,
        })

    return templates.TemplateResponse(
        "schedules/index.html",
        {
            "request": request,
            "current_user": current_user,
            "schedules": schedules_with_next,
            "providers": providers,
            "global_tasks": global_tasks,
            "tz": __import__("os").getenv("TZ", "America/Los_Angeles"),
        },
    )


@router.post("/provider/{provider_slug}/save", response_class=HTMLResponse)
async def save_provider_schedule(
    provider_slug: str,
    request: Request,
    trigger_type: str = Form("cron"),
    cron_expression: str = Form(""),
    interval_seconds: str = Form(""),
    enabled: str = Form("off"),
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        provider = conn.execute(
            "SELECT name, slug, type FROM providers WHERE slug = ?", (provider_slug,)
        ).fetchone()

    if not provider:
        return RedirectResponse("/schedules", status_code=302)

    provider = dict(provider)
    task_type = f"{provider['type']}_download"
    task_id = f"provider_download:{provider_slug}"
    label = f"Download — {provider['name']}"
    is_enabled = enabled.lower() in ("on", "true", "1", "yes")

    cron_val = cron_expression.strip() or None
    interval_val = int(interval_seconds.strip()) if interval_seconds.strip().isdigit() else None

    _upsert_schedule(
        task_id=task_id,
        provider_slug=provider_slug,
        task_type=task_type,
        label=label,
        trigger_type=trigger_type,
        cron_expression=cron_val,
        interval_seconds=interval_val,
        enabled=is_enabled,
    )

    schedule = _get_schedule(task_id)
    if schedule:
        _apply_schedule_to_scheduler(schedule)

    logger.info(
        "Schedule saved for provider %s by %s", provider_slug, current_user.username
    )
    return RedirectResponse("/schedules", status_code=302)


@router.post("/global/{task_type}/save", response_class=HTMLResponse)
async def save_global_schedule(
    task_type: str,
    request: Request,
    trigger_type: str = Form("cron"),
    cron_expression: str = Form(""),
    interval_seconds: str = Form(""),
    enabled: str = Form("off"),
    current_user: TokenData = Depends(get_current_user),
):
    _GLOBAL_LABELS = {
        "download_all_providers": "Download All Active Providers",
    }
    if task_type not in _GLOBAL_LABELS:
        return RedirectResponse("/schedules", status_code=302)

    task_id = f"global:{task_type}"
    label = _GLOBAL_LABELS[task_type]
    is_enabled = enabled.lower() in ("on", "true", "1", "yes")
    cron_val = cron_expression.strip() or None
    interval_val = int(interval_seconds.strip()) if interval_seconds.strip().isdigit() else None

    _upsert_schedule(
        task_id=task_id,
        provider_slug=None,
        task_type=task_type,
        label=label,
        trigger_type=trigger_type,
        cron_expression=cron_val,
        interval_seconds=interval_val,
        enabled=is_enabled,
    )

    schedule = _get_schedule(task_id)
    if schedule:
        _apply_schedule_to_scheduler(schedule)

    logger.info("Global schedule saved: %s by %s", task_type, current_user.username)
    return RedirectResponse("/schedules", status_code=302)


@router.post("/global/{task_type}/run-now")
async def run_global_now(
    task_type: str,
    current_user: TokenData = Depends(get_current_user),
):
    fn = _resolve_task_fn(task_type, None)

    if fn is not None:
        import threading
        t = threading.Thread(target=fn, daemon=True)
        t.start()
        logger.info("Manual global trigger: %s by %s", task_type, current_user.username)
    else:
        logger.warning("Manual global trigger requested for unknown task_type: %s", task_type)

    return RedirectResponse("/schedules", status_code=302)


@router.post("/provider/{provider_slug}/run-now")
async def run_provider_now(
    provider_slug: str,
    current_user: TokenData = Depends(get_current_user),
):
    with get_db() as conn:
        provider = conn.execute(
            "SELECT name, slug, type FROM providers WHERE slug = ?", (provider_slug,)
        ).fetchone()

    if not provider:
        return RedirectResponse("/schedules", status_code=302)

    provider = dict(provider)
    task_type = f"{provider['type']}_download"
    fn = _resolve_task_fn(task_type, provider_slug)

    if fn is not None:
        import threading
        t = threading.Thread(target=fn, daemon=True)
        t.start()
        logger.info(
            "Manual trigger: %s for provider %s by %s",
            task_type, provider_slug, current_user.username,
        )
    else:
        logger.warning(
            "Manual trigger requested for %s but no task function defined yet",
            provider_slug,
        )

    return RedirectResponse("/schedules", status_code=302)

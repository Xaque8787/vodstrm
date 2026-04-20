"""
Central task registry.

On startup, replays every enabled schedule from the task_schedules DB table
so that user-configured jobs survive restarts.  No jobs are hardcoded here.

The actual job functions are resolved by the same _resolve_task_fn() helper
used by the schedules route, keeping both code paths in sync.
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger("app.tasks.registry")


def register_all(scheduler: BackgroundScheduler) -> None:
    """
    Load all enabled schedules from the database and register them with the
    scheduler.  Called once at startup from app.scheduler.start_scheduler().
    """
    try:
        from app.database import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM task_schedules WHERE enabled = 1"
            ).fetchall()
    except Exception as exc:
        logger.error("[REGISTRY] Failed to load schedules from DB: %s", exc)
        return

    if not rows:
        logger.info("[REGISTRY] No enabled schedules found in DB — nothing registered")
        return

    registered = 0
    for row in rows:
        schedule = dict(row)
        try:
            _apply(scheduler, schedule)
            registered += 1
        except Exception as exc:
            logger.error(
                "[REGISTRY] Failed to register schedule '%s': %s",
                schedule.get("task_id"), exc,
            )

    logger.info("[REGISTRY] Registered %d schedule(s) from DB", registered)


def _apply(scheduler: BackgroundScheduler, schedule: dict) -> None:
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from app.routes.schedules import _resolve_task_fn

    task_id = schedule["task_id"]
    fn = _resolve_task_fn(schedule["task_type"], schedule.get("provider_slug"))

    if fn is None:
        logger.warning(
            "[REGISTRY] No task function for task_type='%s' (task_id='%s'), skipping",
            schedule["task_type"], task_id,
        )
        return

    if schedule["trigger_type"] == "cron":
        trigger = CronTrigger.from_crontab(schedule["cron_expression"])
    else:
        trigger = IntervalTrigger(seconds=schedule["interval_seconds"])

    scheduler.add_job(fn, trigger=trigger, id=task_id, replace_existing=True)
    logger.info(
        "[REGISTRY] Registered '%s' trigger=%s %s",
        task_id,
        schedule["trigger_type"],
        schedule.get("cron_expression") or f"{schedule.get('interval_seconds')}s",
    )

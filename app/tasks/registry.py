"""
Central task registry.

On startup, syncs every schedule from the task_schedules DB table with the
APScheduler job store. Enabled schedules are registered; disabled schedules
are explicitly removed from the persistent store so they cannot fire after a
restart even if a stale record exists in scheduler.db.

The actual job functions are resolved by the same _resolve_task_fn() helper
used by the schedules route, keeping both code paths in sync.
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger("app.tasks.registry")


def register_all(scheduler: BackgroundScheduler) -> None:
    """
    Load all schedules from the database and sync them with the scheduler.
    Enabled schedules are registered; disabled schedules are explicitly removed
    from the persistent job store so they don't fire after a restart.
    Called once at startup from app.scheduler.start_scheduler().
    """
    try:
        from app.database import get_db
        with get_db() as conn:
            rows = conn.execute("SELECT * FROM task_schedules").fetchall()
    except Exception as exc:
        logger.error("[REGISTRY] Failed to load schedules from DB: %s", exc)
        return

    known_task_ids = {dict(row)["task_id"] for row in rows}

    # Purge any job in the persistent store that has no DB row at all (ghosts
    # left over from renamed or deleted schedules).
    for job in scheduler.get_jobs():
        if job.id not in known_task_ids:
            try:
                scheduler.remove_job(job.id)
                logger.info("[REGISTRY] Purged orphaned job (no DB row): '%s'", job.id)
            except Exception as exc:
                logger.warning("[REGISTRY] Could not remove orphaned job '%s': %s", job.id, exc)

    if not rows:
        logger.info("[REGISTRY] No schedules found in DB - nothing to register")
        return

    registered = 0
    removed = 0
    for row in rows:
        schedule = dict(row)
        task_id = schedule.get("task_id")
        if schedule.get("enabled"):
            try:
                _apply(scheduler, schedule)
                registered += 1
            except Exception as exc:
                logger.error("[REGISTRY] Failed to register schedule '%s': %s", task_id, exc)
        else:
            try:
                scheduler.remove_job(task_id)
                logger.info("[REGISTRY] Purged disabled job from job store: '%s'", task_id)
                removed += 1
            except Exception:
                # Job wasn't in the store - nothing to remove
                pass

    logger.info("[REGISTRY] Startup sync complete - registered=%d purged=%d", registered, removed)


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

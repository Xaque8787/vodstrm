import logging
import os

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from app.config import settings
from app.utils.env import resolve_path

logger = logging.getLogger("app.scheduler")

_scheduler: BackgroundScheduler | None = None


def _build_scheduler() -> BackgroundScheduler:
    scheduler_db_path = resolve_path(settings.scheduler_db_path)
    os.makedirs(os.path.dirname(scheduler_db_path), exist_ok=True)

    jobstores = {
        "default": SQLAlchemyJobStore(url=f"sqlite:///{scheduler_db_path}"),
    }
    executors = {
        "default": ThreadPoolExecutor(20),
    }
    job_defaults = {
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 604800,
    }

    return BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
        timezone=settings.timezone,
    )


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = _build_scheduler()
    return _scheduler


def start_scheduler() -> None:
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        from app.tasks import registry
        registry.register_all(scheduler)
        logger.info("Scheduler started (tz=%s)", settings.timezone)


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

"""
Central task registry.

This is the ONLY place where scheduled tasks are registered with the
scheduler. Import the task function here and add it via _register_task().
No task registration may happen anywhere else in the app.
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from app.tasks.downloader import download_all_providers
from app.tasks.ingestion import ingest_all_providers

logger = logging.getLogger("app.tasks.registry")


def _register_task(
    scheduler: BackgroundScheduler,
    fn,
    trigger: str,
    job_id: str,
    replace_existing: bool = True,
    **trigger_kwargs,
) -> None:
    scheduler.add_job(
        fn,
        trigger=trigger,
        id=job_id,
        replace_existing=replace_existing,
        **trigger_kwargs,
    )
    logger.info("Registered task: %s (trigger=%s)", job_id, trigger)


def register_all(scheduler: BackgroundScheduler) -> None:
    # Download all providers at 04:00 daily; ingestion is triggered inline
    # per-provider immediately after each successful download.
    _register_task(
        scheduler,
        download_all_providers,
        trigger="cron",
        job_id="download_all_providers",
        hour=4,
        minute=0,
    )

    # Standalone ingest job: picks up any .m3u files left in the directory
    # (e.g. manually placed files or partial runs). Runs 5 minutes after
    # the download job to allow for slow downloads.
    _register_task(
        scheduler,
        ingest_all_providers,
        trigger="cron",
        job_id="ingest_all_providers",
        hour=4,
        minute=5,
    )

    logger.info("All tasks registered with scheduler")

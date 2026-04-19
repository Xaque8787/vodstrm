"""
Central task registry.

This is the ONLY place where scheduled tasks are registered with the
scheduler. Import the task function here and add it via _register_task().
No task registration may happen anywhere else in the app.
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler

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
    """
    Register every scheduled task here.
    Tasks will be added as they are implemented.
    """
    logger.info("All tasks registered with scheduler")

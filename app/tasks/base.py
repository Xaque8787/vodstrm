"""
Base infrastructure for all scheduled tasks.

Every task in this app must:
  - Be a plain function that handles its own DB connections
  - Wrap its body in try/except and log start, finish, and failure
  - Be registered via the registry module — never scattered across files
  - Be manually triggerable (the function itself can be called directly)
"""
import functools
import logging
import traceback
from datetime import datetime, timezone

task_logger = logging.getLogger("app.tasks")


def task(name: str):
    """
    Decorator that wraps a task function with standard logging and error
    isolation so that a crash in one task never affects the scheduler or
    other tasks.

    Usage:
        @task("my_task_name")
        def my_task():
            ...
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            started_at = datetime.now(timezone.utc)
            task_logger.info("[TASK:%s] Starting at %s", name, started_at.isoformat())
            try:
                result = fn(*args, **kwargs)
                finished_at = datetime.now(timezone.utc)
                elapsed = (finished_at - started_at).total_seconds()
                task_logger.info(
                    "[TASK:%s] Finished successfully in %.2fs", name, elapsed
                )
                return result
            except Exception as exc:
                finished_at = datetime.now(timezone.utc)
                elapsed = (finished_at - started_at).total_seconds()
                task_logger.error(
                    "[TASK:%s] Failed after %.2fs — %s: %s\n%s",
                    name,
                    elapsed,
                    type(exc).__name__,
                    exc,
                    traceback.format_exc(),
                )
        return wrapper
    return decorator

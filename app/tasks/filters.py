"""Reapply filters task — schedulable and callable from routes."""
import logging

from app.database import get_db
from app.filters.engine import load_filters, run_filters_for_provider
from app.tasks.base import task

logger = logging.getLogger("app.tasks.filters")


@task("reapply_filters")
def reapply_filters(provider_slug: str | None = None) -> None:
    """
    Reapply all enabled filters to stream rows, always from entry.cleaned_title.
    provider_slug=None means reapply to all providers.
    """
    with get_db() as conn:
        filters = load_filters(conn)

    if not filters:
        logger.info("[FILTERS] No enabled filters — nothing to apply")
        return

    logger.info("[FILTERS] Reapply start — provider=%s filters=%d", provider_slug or "*", len(filters))

    with get_db() as conn:
        updated = run_filters_for_provider(conn, filters, provider=provider_slug)

    logger.info("[FILTERS] Reapply done — provider=%s streams_updated=%d", provider_slug or "*", updated)

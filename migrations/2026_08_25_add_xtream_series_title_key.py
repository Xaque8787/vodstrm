"""Add a normalized title key for fast series summary grouping and search."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(xtream_series_catalog)"
        ).fetchall()
    }
    if "title_key" not in columns:
        log.info("  Adding xtream_series_catalog.title_key column")
        conn.execute(
            "ALTER TABLE xtream_series_catalog ADD COLUMN title_key TEXT"
        )
    else:
        log.info("  xtream_series_catalog.title_key already exists, skipping")

    updated = conn.execute(
        """
        UPDATE xtream_series_catalog
        SET title_key = lower(trim(series_name))
        WHERE title_key IS NULL OR title_key = ''
        """
    ).rowcount
    if updated:
        log.info("  Backfilled title_key on %d rows", updated)
    else:
        log.info("  No rows needed title_key backfill")

    indexes = {
        row[1]
        for row in conn.execute(
            "PRAGMA index_list(xtream_series_catalog)"
        ).fetchall()
    }
    if "idx_xtream_series_catalog_title_key" not in indexes:
        log.info("  Creating idx_xtream_series_catalog_title_key index")
    else:
        log.info("  idx_xtream_series_catalog_title_key already exists, skipping")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xtream_series_catalog_title_key
        ON xtream_series_catalog(title_key)
        """
    )
    conn.commit()

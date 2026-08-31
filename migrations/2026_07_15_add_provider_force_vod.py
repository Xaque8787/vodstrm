"""
Add force_vod column to providers.

When enabled (1), the parser skips the duration == -1 live-TV check and
classifies all entries as VOD (series / tv_vod / movie / unsorted).
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "force_vod" in existing:
        log.info("  providers.force_vod already exists, skipping")
        return
    log.info("  Adding providers.force_vod column")
    conn.execute(
        "ALTER TABLE providers ADD COLUMN force_vod INTEGER NOT NULL DEFAULT 0"
    )
    conn.commit()

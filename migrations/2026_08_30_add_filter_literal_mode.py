"""Migration: add literal_mode column to filters table.

When literal_mode is 1, filter patterns are treated as literal text
(auto-escaped) instead of raw regex. This lets users remove special
characters like ( without manually escaping them.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(filters)").fetchall()}
    if "literal_mode" in existing:
        log.info("  filters.literal_mode already exists, skipping")
        return
    log.info("  Adding filters.literal_mode column")
    conn.execute("ALTER TABLE filters ADD COLUMN literal_mode INTEGER NOT NULL DEFAULT 0")
    conn.commit()

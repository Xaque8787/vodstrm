"""
Migration: add priority column to providers table

Adds a priority integer column that controls which provider's stream URL is
used when multiple providers supply the same entry.  Lower numbers win
(highest priority).  All existing providers default to 10.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "priority" in existing:
        log.info("  providers.priority already exists, skipping")
        return
    log.info("  Adding providers.priority column")
    conn.execute(
        "ALTER TABLE providers ADD COLUMN priority INTEGER NOT NULL DEFAULT 10"
    )
    conn.commit()

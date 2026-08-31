"""
Migration: add is_active column to providers table
Adds a boolean-style INTEGER column (1 = active, 0 = inactive) with a default of 1
so all existing providers remain active after the migration.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "is_active" in existing:
        log.info("  providers.is_active already exists, skipping")
        return
    log.info("  Adding providers.is_active column")
    conn.execute(
        "ALTER TABLE providers ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1"
    )
    conn.commit()

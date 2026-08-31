"""
Migration: add stream_format column to providers table
Adds a TEXT column 'stream_format' with a default of 'ts'.
Only meaningful for xtream-type providers.
Allowed values: 'ts', 'hls'
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "stream_format" in existing:
        log.info("  providers.stream_format already exists, skipping")
        return
    log.info("  Adding providers.stream_format column")
    conn.execute(
        "ALTER TABLE providers ADD COLUMN stream_format TEXT NOT NULL DEFAULT 'ts'"
    )
    conn.commit()

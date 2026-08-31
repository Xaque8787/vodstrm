"""
Add quality_terms column to providers table.

Adds an ordered JSON array of plain-text quality terms per provider.
The ingest pipeline uses these to score raw titles and decide whether
an incoming stream should overwrite an existing one for the same
(entry_id, provider) pair. Empty array (default) preserves the
existing unconditional-overwrite behaviour.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "quality_terms" in existing:
        log.info("  providers.quality_terms already exists, skipping")
        return
    log.info("  Adding providers.quality_terms column")
    conn.execute(
        "ALTER TABLE providers ADD COLUMN quality_terms TEXT NOT NULL DEFAULT '[]'"
    )
    conn.commit()

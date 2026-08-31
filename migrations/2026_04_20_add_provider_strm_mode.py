"""
Migration: add strm_mode column to providers table

Adds a strm_mode column that controls how .strm files are generated for a
provider. Existing providers default to 'generate_all'.

Values:
  generate_all    — generate a .strm file for every entry from this provider
  import_selected — only generate .strm files for manually selected entries
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "strm_mode" in existing:
        log.info("  providers.strm_mode already exists, skipping")
        return
    log.info("  Adding providers.strm_mode column")
    conn.execute(
        """
        ALTER TABLE providers
        ADD COLUMN strm_mode TEXT NOT NULL DEFAULT 'generate_all'
        CHECK(strm_mode IN ('generate_all', 'import_selected'))
        """
    )
    conn.commit()

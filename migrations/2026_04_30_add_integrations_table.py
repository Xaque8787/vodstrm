"""
Add integrations settings table.

New table:
  - integrations: one row per integration slug, stores key/value settings as
    a JSON blob. Replaces env-var configuration for all integrations.

Columns:
  - slug        TEXT PRIMARY KEY  e.g. 'tmdb'
  - settings    TEXT              JSON object of integration-specific settings
  - updated_at  TEXT              ISO timestamp of last save
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    tables = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "integrations" in tables:
        log.info("  integrations table already exists, skipping")
        conn.commit()
        return
    log.info("  Creating integrations table")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integrations (
            slug       TEXT PRIMARY KEY,
            settings   TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

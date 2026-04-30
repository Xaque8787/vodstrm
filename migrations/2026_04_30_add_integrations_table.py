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
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integrations (
            slug       TEXT PRIMARY KEY,
            settings   TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

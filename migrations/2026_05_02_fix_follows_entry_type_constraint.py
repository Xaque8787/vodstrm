"""
Fix follows table: expand entry_type CHECK constraint to include tv_vod.

Changes:
  - follows.entry_type: CHECK(entry_type IN ('movie', 'series')) →
                        CHECK(entry_type IN ('movie', 'series', 'tv_vod'))
  - All existing rows are preserved; the table is recreated in-place using
    SQLite's standard rename-create-insert-drop pattern.

No data is lost. Foreign key references are maintained.

On a fresh install, _SCHEMA already creates follows with the updated constraint,
so this migration detects that and skips the rebuild.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)

    # Check if the current constraint already includes tv_vod.
    schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='follows'"
    ).fetchone()
    if schema and "tv_vod" in (schema["sql"] or ""):
        log.info("  follows.entry_type already includes tv_vod, skipping rebuild")
        conn.commit()
        return

    log.info("  Rebuilding follows table with tv_vod entry_type support")
    conn.execute("PRAGMA foreign_keys = OFF")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS follows_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
            entry_type  TEXT NOT NULL CHECK(entry_type IN ('movie', 'series', 'tv_vod')),
            entry_title TEXT NOT NULL,
            season      INTEGER,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    conn.execute("""
        INSERT INTO follows_new (id, provider_id, entry_type, entry_title, season, created_at)
        SELECT id, provider_id, entry_type, entry_title, season, created_at FROM follows
    """)

    conn.execute("DROP TABLE follows")
    conn.execute("ALTER TABLE follows_new RENAME TO follows")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_follows_provider_id ON follows(provider_id)"
    )

    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()

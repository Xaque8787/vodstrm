"""
Fix follows table: expand entry_type CHECK constraint to include tv_vod.

Changes:
  - follows.entry_type: CHECK(entry_type IN ('movie', 'series')) →
                        CHECK(entry_type IN ('movie', 'series', 'tv_vod'))
  - All existing rows are preserved; the table is recreated in-place using
    SQLite's standard rename-create-insert-drop pattern.

No data is lost. Foreign key references are maintained.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
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

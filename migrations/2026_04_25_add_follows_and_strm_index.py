"""
Add follows table and unique strm owner index.

New tables:
  - follows: one row per follow rule. Scoped to a provider (by id FK) and
    entry_type + entry_title pattern (optional season). When a provider
    ingests matching content, the matching streams are marked imported=1,
    making them eligible for STRM ownership in import_selected mode.

    Columns:
      id          - PK
      provider_id - FK to providers(id), CASCADE DELETE
      entry_type  - 'movie' or 'series' only (tv_vod excluded by constraint)
      entry_title - case-insensitive LIKE match applied during ingest
      season      - NULL = all seasons; integer = exact season match
      created_at  - timestamp

New indexes:
  - idx_follows_provider_id: fast lookup of rules by provider
  - idx_unique_strm_owner: partial unique index on streams(entry_id) WHERE
    strm_path IS NOT NULL — enforces that only one stream per entry holds
    STRM ownership at any time. NULL rows (non-owners) are excluded so
    multiple providers can track the same entry.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS follows (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
            entry_type  TEXT NOT NULL CHECK(entry_type IN ('movie', 'series')),
            entry_title TEXT NOT NULL,
            season      INTEGER,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    existing_indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(follows)").fetchall()
    }
    if "idx_follows_provider_id" not in existing_indexes:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_follows_provider_id ON follows(provider_id)"
        )

    stream_indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(streams)").fetchall()
    }
    if "idx_unique_strm_owner" not in stream_indexes:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_strm_owner
                ON streams(entry_id)
                WHERE strm_path IS NOT NULL
        """)

    conn.commit()

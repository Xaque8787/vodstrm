"""
Add follows table; remove incorrect unique strm owner index.

New tables:
  - follows: one row per follow rule. Scoped to a provider (by id FK) and
    entry_type + entry_title pattern (optional season). When a provider
    ingests matching content, the matching streams are marked imported=1,
    making them eligible for STRM ownership in import_selected mode.

    Columns:
      id          - PK
      provider_id - FK to providers(id), CASCADE DELETE
      entry_type  - 'movie' or 'series' only
      entry_title - case-insensitive LIKE match applied during ingest
      season      - NULL = all seasons; integer = exact season match
      created_at  - timestamp

New indexes:
  - idx_follows_provider_id: fast lookup of rules by provider

Removed index:
  - idx_unique_strm_owner (if it was previously applied): this partial unique
    index on streams(entry_id) WHERE strm_path IS NOT NULL caused UNIQUE
    constraint violations in _sync_one because losers may not be cleared
    before the winner's strm_path is set in the same pass. The single-owner
    invariant is maintained by the sync engine logic, not at DB level.
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

    stream_indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(follows)").fetchall()
    }
    if "idx_follows_provider_id" not in stream_indexes:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_follows_provider_id ON follows(provider_id)"
        )

    # Drop the bad unique index if it was applied by an earlier version of this migration
    try:
        conn.execute("DROP INDEX IF EXISTS idx_unique_strm_owner")
    except Exception:
        pass

    conn.commit()

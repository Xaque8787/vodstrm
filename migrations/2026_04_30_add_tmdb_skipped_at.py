"""
Add tmdb_skipped_at column to entries table.

When TMDB enrichment searches for a title and finds no results, it stamps
this column with an ISO timestamp. The enrichment loop skips these entries
on subsequent runs. The column is cleared when an entry is re-ingested so
it gets another attempt. Users can also force a retry via the Integrations
page which clears all tmdb_skipped_at values before running.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(entries)").fetchall()}
    if "tmdb_skipped_at" not in existing:
        conn.execute("ALTER TABLE entries ADD COLUMN tmdb_skipped_at TEXT DEFAULT NULL")
    conn.commit()

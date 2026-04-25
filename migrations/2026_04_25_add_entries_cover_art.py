"""
Add cover_art column to entries table.

Modified tables:
  - entries: new nullable TEXT column cover_art
    Stores a URL extracted from the tvg-logo attribute in streams.metadata_json.
    Populated during ingest: for movies, the tvg-logo of the most recent stream
    processed is used. For series, the tvg-logo of the last stream processed for
    that title group is used (last ingested episode wins). May be NULL if no
    provider supplies a tvg-logo for a given entry.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(entries)").fetchall()}
    if "cover_art" not in existing:
        conn.execute("ALTER TABLE entries ADD COLUMN cover_art TEXT")
    conn.commit()

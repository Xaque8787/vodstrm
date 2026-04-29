"""
Add schedule_omitted column to providers table.

Adds a boolean flag that, when set, keeps the provider's data in the database
but excludes it from scheduled and global ingests. Unlike is_active=0 (which
removes data on the next ingest), schedule_omitted=1 preserves all streams and
entries while preventing future updates from this provider.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "schedule_omitted" not in existing:
        conn.execute(
            "ALTER TABLE providers ADD COLUMN schedule_omitted INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()

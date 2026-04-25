"""
Migration: add stream_format column to providers table
Adds a TEXT column 'stream_format' with a default of 'ts'.
Only meaningful for xtream-type providers.
Allowed values: 'ts', 'hls'
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "stream_format" not in existing:
        conn.execute(
            "ALTER TABLE providers ADD COLUMN stream_format TEXT NOT NULL DEFAULT 'ts'"
        )
    conn.commit()

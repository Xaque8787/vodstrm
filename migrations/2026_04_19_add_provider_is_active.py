"""
Migration: add is_active column to providers table
Adds a boolean-style INTEGER column (1 = active, 0 = inactive) with a default of 1
so all existing providers remain active after the migration.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "is_active" not in existing:
        conn.execute(
            "ALTER TABLE providers ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1"
        )
    conn.commit()

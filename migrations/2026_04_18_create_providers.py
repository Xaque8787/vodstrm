"""
Migration: Create providers table
Run directly: python migrations/2026_04_18_create_providers.py
"""
import sqlite3
import os
import sys


def up(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS providers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('m3u', 'xtream')),
            url TEXT,
            username TEXT,
            password TEXT,
            port TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def down(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS providers")


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from run_migrations import _get_connection, _ensure_migrations_table

    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    conn = _get_connection()
    _ensure_migrations_table(conn)
    up(conn)
    conn.commit()
    conn.close()
    print("Migration applied: create_providers")

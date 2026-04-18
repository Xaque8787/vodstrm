"""
Migration: Create users table
Run directly: python migrations/2026_04_18_create_users.py
"""
import sqlite3
import os
import sys


def up(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def down(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS users")


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from run_migrations import _get_connection, _ensure_migrations_table

    logging_import = __import__("logging")
    logging_import.basicConfig(level=logging_import.INFO, format="%(levelname)s | %(message)s")

    conn = _get_connection()
    _ensure_migrations_table(conn)
    up(conn)
    conn.commit()
    conn.close()
    print("Migration applied: create_users")

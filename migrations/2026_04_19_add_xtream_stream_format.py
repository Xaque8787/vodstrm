"""
Migration: add stream_format column to providers table
Adds a TEXT column 'stream_format' with a default of 'ts'.
Only meaningful for xtream-type providers.
Allowed values: 'ts', 'hls'
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        ALTER TABLE providers ADD COLUMN stream_format TEXT NOT NULL DEFAULT 'ts'
        """
    )
    conn.commit()

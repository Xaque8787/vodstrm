"""
Migration: add strm_mode column to providers table

Adds a strm_mode column that controls how .strm files are generated for a
provider. Existing providers default to 'generate_all'.

Values:
  generate_all    — generate a .strm file for every entry from this provider
  import_selected — only generate .strm files for manually selected entries
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        ALTER TABLE providers
        ADD COLUMN strm_mode TEXT NOT NULL DEFAULT 'generate_all'
        CHECK(strm_mode IN ('generate_all', 'import_selected'))
        """
    )
    conn.commit()

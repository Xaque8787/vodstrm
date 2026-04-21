"""
Migration: add local_file provider type support

Changes:
  1. providers.type CHECK constraint — extends allowed values to include
     'local_file'. SQLite cannot alter a CHECK constraint in-place, so
     this is handled by recreating the table with the updated constraint.

  2. providers.local_file_path (TEXT, nullable) — stores the filename of
     the .m3u file inside the container's /data/m3u directory for local_file
     providers. For m3u / xtream providers this column remains NULL.

Steps:
  - Rename existing providers table to a temporary name
  - Create new providers table with updated CHECK and new column
  - Copy all rows over
  - Drop the temporary table
  - Recreate indexes
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        ALTER TABLE providers RENAME TO _providers_old;

        CREATE TABLE IF NOT EXISTS providers (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT UNIQUE NOT NULL,
            slug             TEXT UNIQUE,
            type             TEXT NOT NULL CHECK(type IN ('m3u', 'xtream', 'local_file')),
            url              TEXT,
            username         TEXT,
            password         TEXT,
            port             TEXT,
            stream_format    TEXT NOT NULL DEFAULT 'ts',
            is_active        INTEGER NOT NULL DEFAULT 1,
            strm_mode        TEXT NOT NULL DEFAULT 'generate_all'
                             CHECK(strm_mode IN ('generate_all', 'import_selected')),
            local_file_path  TEXT,
            created_at       TEXT NOT NULL DEFAULT (datetime('now'))
        );

        INSERT INTO providers
            (id, name, slug, type, url, username, password, port,
             stream_format, is_active, strm_mode, local_file_path, created_at)
        SELECT
            id, name, slug, type, url, username, password, port,
            stream_format, is_active, strm_mode, NULL, created_at
        FROM _providers_old;

        DROP TABLE _providers_old;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_slug ON providers (slug);
    """)
    conn.commit()

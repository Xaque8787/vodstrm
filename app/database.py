import logging
import sqlite3
import os
from contextlib import contextmanager
from typing import Generator

from app.utils.env import resolve_path

_DATABASE_RELATIVE = os.getenv("DATABASE_PATH", "data/app.db")
DATABASE_PATH = resolve_path(_DATABASE_RELATIVE)

_sql_logger = logging.getLogger("app.sql")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS providers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    slug TEXT UNIQUE,
    type TEXT NOT NULL CHECK(type IN ('m3u', 'xtream')),
    url TEXT,
    username TEXT,
    password TEXT,
    port TEXT,
    stream_format TEXT NOT NULL DEFAULT 'ts',
    is_active INTEGER NOT NULL DEFAULT 1,
    strm_mode TEXT NOT NULL DEFAULT 'generate_all' CHECK(strm_mode IN ('generate_all', 'import_selected')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_slug ON providers (slug);

CREATE TABLE IF NOT EXISTS task_schedules (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT UNIQUE NOT NULL,
    provider_slug       TEXT,
    task_type           TEXT NOT NULL,
    label               TEXT NOT NULL,
    enabled             INTEGER NOT NULL DEFAULT 1,
    trigger_type        TEXT NOT NULL DEFAULT 'cron' CHECK(trigger_type IN ('cron', 'interval')),
    cron_expression     TEXT,
    interval_seconds    INTEGER,
    last_run_at         TEXT,
    last_run_status     TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -------------------------------------------------------
-- MEDIA LIBRARY: entries (what content is)
-- -------------------------------------------------------
-- One row per unique piece of content (movie, episode, channel, etc.)
-- entry_id is a deterministic hash of content identity fields so
-- re-ingesting the same content never creates a duplicate row.
CREATE TABLE IF NOT EXISTS entries (
    entry_id    TEXT PRIMARY KEY,
    type        TEXT NOT NULL CHECK(type IN ('movie', 'series', 'live', 'tv_vod', 'unsorted')),
    cleaned_title TEXT,
    raw_title   TEXT,
    year        INTEGER,
    season      INTEGER,
    episode     INTEGER,
    air_date    TEXT,
    series_type TEXT CHECK(series_type IN ('season_episode', 'air_date') OR series_type IS NULL),
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT
);

-- -------------------------------------------------------
-- MEDIA LIBRARY: streams (where content comes from)
-- -------------------------------------------------------
-- One row per provider per entry. The same content can be supplied
-- by multiple providers; each provider may only have one active
-- stream URL per entry at a time (enforced by unique index below).
CREATE TABLE IF NOT EXISTS streams (
    stream_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id      TEXT NOT NULL REFERENCES entries(entry_id),
    stream_url    TEXT NOT NULL,
    provider      TEXT NOT NULL,
    source_file   TEXT,
    ingested_at   TEXT,
    batch_id      TEXT NOT NULL,
    metadata_json TEXT
);

-- One stream per provider per entry (upsert key)
CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_entry_provider
    ON streams(entry_id, provider);

-- Fast lookup of all streams for a given entry
CREATE INDEX IF NOT EXISTS idx_streams_entry_id
    ON streams(entry_id);

-- Fast lookup of all streams belonging to a batch (used in cleanup)
CREATE INDEX IF NOT EXISTS idx_streams_batch_id
    ON streams(batch_id);
"""


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    if os.getenv("DEBUG", "false").lower() == "true":
        conn.set_trace_callback(_sql_logger.debug)
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

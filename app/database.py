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
    type TEXT NOT NULL CHECK(type IN ('m3u', 'xtream', 'local_file')),
    url TEXT,
    username TEXT,
    password TEXT,
    port TEXT,
    stream_format TEXT NOT NULL DEFAULT 'ts',
    is_active INTEGER NOT NULL DEFAULT 1,
    schedule_omitted INTEGER NOT NULL DEFAULT 0,
    strm_mode TEXT NOT NULL DEFAULT 'generate_all' CHECK(strm_mode IN ('generate_all', 'import_selected')),
    priority INTEGER NOT NULL DEFAULT 10,
    local_file_path TEXT,
    quality_terms TEXT NOT NULL DEFAULT '[]',
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
    cover_art   TEXT,
    tmdb_id         INTEGER DEFAULT NULL,
    tmdb_type       TEXT DEFAULT NULL,
    tmdb_skipped_at TEXT DEFAULT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT
);

-- -------------------------------------------------------
-- MEDIA LIBRARY: streams (where content comes from)
-- -------------------------------------------------------
-- One row per provider per entry. The same content can be supplied
-- by multiple providers; each provider may only have one active
-- stream URL per entry at a time (enforced by unique index below).
-- Filter output columns (filtered_title, filter_hits, exclude, include_only,
-- include_only_active) are populated by the filter engine after each ingest
-- or manual reapply. include_only_active=1 means at least one include_only
-- rule was in scope for this stream; combined with include_only=0 it means
-- the stream did not pass the filter and should be suppressed.
CREATE TABLE IF NOT EXISTS streams (
    stream_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id       TEXT NOT NULL REFERENCES entries(entry_id),
    stream_url     TEXT NOT NULL,
    provider       TEXT NOT NULL,
    source_file    TEXT,
    ingested_at    TEXT,
    batch_id       TEXT NOT NULL,
    metadata_json  TEXT,
    filtered_title   TEXT,
    filter_hits      TEXT DEFAULT '[]',
    exclude          INTEGER DEFAULT 0,
    include_only     INTEGER DEFAULT 0,
    include_only_active INTEGER NOT NULL DEFAULT 0,
    strm_path        TEXT,
    last_written_url TEXT,
    -- 1 = eligible for STRM ownership (set by manual Add or follow engine;
    --     cleared only by manual Remove; never cleared by Unfollow)
    imported         INTEGER NOT NULL DEFAULT 0
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

-- -------------------------------------------------------
-- FILTERS: rule definitions and scope
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS filters (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filter_type  TEXT NOT NULL
                 CHECK(filter_type IN ('remove', 'exclude', 'include_only', 'replace')),
    label        TEXT NOT NULL DEFAULT '',
    order_index  INTEGER NOT NULL DEFAULT 0,
    enabled      INTEGER NOT NULL DEFAULT 1,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Which providers a filter applies to ('*' = all)
CREATE TABLE IF NOT EXISTS filter_providers (
    filter_id INTEGER NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
    provider  TEXT NOT NULL,
    PRIMARY KEY (filter_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_filter_providers_filter ON filter_providers(filter_id);

-- Which entry types a filter applies to ('*' = all)
CREATE TABLE IF NOT EXISTS filter_entry_types (
    filter_id  INTEGER NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
    entry_type TEXT NOT NULL
               CHECK(entry_type IN ('movie', 'series', 'live', 'tv_vod', 'unsorted', '*')),
    PRIMARY KEY (filter_id, entry_type)
);

CREATE INDEX IF NOT EXISTS idx_filter_entry_types_filter ON filter_entry_types(filter_id);

-- Individual pattern rows per filter rule
CREATE TABLE IF NOT EXISTS filter_patterns (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filter_id   INTEGER NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
    pattern     TEXT NOT NULL,
    replacement TEXT,
    order_index INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_filter_patterns_filter ON filter_patterns(filter_id);

-- -------------------------------------------------------
-- INTEGRATIONS: per-integration settings (key/value as JSON)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS integrations (
    slug       TEXT PRIMARY KEY,
    settings   TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -------------------------------------------------------
-- TMDB: cached metadata from The Movie Database API
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS tmdb_shows (
    tmdb_id        INTEGER PRIMARY KEY,
    tmdb_title     TEXT,
    poster_path    TEXT,
    first_air_date TEXT,
    overview       TEXT,
    cached_at      TEXT
);

CREATE TABLE IF NOT EXISTS tmdb_seasons (
    tmdb_id        INTEGER NOT NULL,
    season_number  INTEGER NOT NULL,
    episode_count  INTEGER,
    poster_path    TEXT,
    PRIMARY KEY (tmdb_id, season_number)
);

CREATE TABLE IF NOT EXISTS tmdb_movies (
    tmdb_id      INTEGER PRIMARY KEY,
    tmdb_title   TEXT,
    poster_path  TEXT,
    release_date TEXT,
    overview     TEXT,
    cached_at    TEXT
);

CREATE TABLE IF NOT EXISTS tmdb_run_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at           TEXT,
    triggered_by     TEXT,
    entries_checked  INTEGER,
    api_calls_made   INTEGER,
    enriched         INTEGER,
    cache_hits       INTEGER,
    errors           INTEGER,
    error_detail     TEXT,
    duration_seconds REAL
);

-- -------------------------------------------------------
-- LIBRARY: follow rules (import_selected eligibility)
-- -------------------------------------------------------
-- One row per rule per provider: when any import_selected provider ingests content
-- matching entry_type + entry_title (+ optional season), mark those streams as
-- imported=1 so they become eligible for STRM ownership. Rules are matched globally
-- across all providers — provider_id is stored for FK integrity only.
-- season NULL = match all seasons; non-NULL = match exact season / year (tv_vod).
CREATE TABLE IF NOT EXISTS follows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    entry_type  TEXT NOT NULL CHECK(entry_type IN ('movie', 'series', 'tv_vod')),
    entry_title TEXT NOT NULL,
    season      INTEGER,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_follows_provider_id ON follows(provider_id);

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

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

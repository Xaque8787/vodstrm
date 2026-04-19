"""
Migration: create task_schedules table

1. New Tables
   - `task_schedules`
     - `id` (INTEGER, primary key)
     - `task_id` (TEXT, unique) — stable machine identifier for the task,
       e.g. "provider_download:my-iptv". Used to reference the APScheduler job.
     - `provider_slug` (TEXT, nullable) — slug of the provider this task is
       tied to, if applicable. NULL for global tasks.
     - `task_type` (TEXT) — category of the task, e.g. "m3u_download",
       "xtream_download".
     - `label` (TEXT) — human-readable display name shown in the UI.
     - `enabled` (INTEGER, default 1) — whether this schedule is active.
     - `trigger_type` (TEXT) — "cron" or "interval".
     - `cron_expression` (TEXT, nullable) — full cron string when
       trigger_type = "cron", e.g. "0 4 * * *".
     - `interval_seconds` (INTEGER, nullable) — repeat interval in seconds
       when trigger_type = "interval".
     - `last_run_at` (TEXT, nullable) — ISO-8601 timestamp of the last time
       the task ran (updated by the task itself).
     - `last_run_status` (TEXT, nullable) — "success" or "error" from the
       last run.
     - `created_at` (TEXT) — row creation timestamp.
     - `updated_at` (TEXT) — row last-modified timestamp.

2. Notes
   - This table stores user-defined schedule configuration only. The actual
     APScheduler job state lives in the separate scheduler.db file.
   - task_id is the join key between this table and the scheduler job store.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.executescript("""
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
    """)
    conn.commit()

"""
Migration: add filters system

Changes:
  1. filters table — stores filter rule definitions.
     Each row is one filter of a given type (remove, exclude, include_only, replace).
     order_index controls execution order within a filter_type.

  2. filter_providers (relation) — which providers a filter applies to.
     provider='*' means all providers.

  3. filter_entry_types (relation) — which content types a filter applies to.
     entry_type='*' means all types.

  4. filter_patterns table — individual pattern rows per filter rule.
     For replace filters: pattern=find text, replacement=with text.
     For all others: replacement is NULL.

  5. streams table — four new filter output columns:
       filtered_title  TEXT     — title after all filters applied
       filter_hits     TEXT     — JSON array of matched terms (debug)
       exclude         INTEGER  — 0/1 flag
       include_only    INTEGER  — 0/1 flag

No existing data is modified. New columns default to NULL/0.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.executescript("""
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

        CREATE TABLE IF NOT EXISTS filter_providers (
            filter_id INTEGER NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
            provider  TEXT NOT NULL,
            PRIMARY KEY (filter_id, provider)
        );

        CREATE INDEX IF NOT EXISTS idx_filter_providers_filter
            ON filter_providers(filter_id);

        CREATE TABLE IF NOT EXISTS filter_entry_types (
            filter_id  INTEGER NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
            entry_type TEXT NOT NULL
                       CHECK(entry_type IN ('movie', 'series', 'live', 'tv_vod', 'unsorted', '*')),
            PRIMARY KEY (filter_id, entry_type)
        );

        CREATE INDEX IF NOT EXISTS idx_filter_entry_types_filter
            ON filter_entry_types(filter_id);

        CREATE TABLE IF NOT EXISTS filter_patterns (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_id   INTEGER NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
            pattern     TEXT NOT NULL,
            replacement TEXT,
            order_index INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_filter_patterns_filter
            ON filter_patterns(filter_id);

        ALTER TABLE streams ADD COLUMN filtered_title TEXT;
        ALTER TABLE streams ADD COLUMN filter_hits     TEXT    DEFAULT '[]';
        ALTER TABLE streams ADD COLUMN exclude         INTEGER DEFAULT 0;
        ALTER TABLE streams ADD COLUMN include_only    INTEGER DEFAULT 0;
    """)
    conn.commit()

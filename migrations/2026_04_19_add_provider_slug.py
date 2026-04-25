"""
Migration: add slug column to providers table

1. New Columns
   - `slug` (TEXT, UNIQUE, NOT NULL) — a URL-safe identifier derived from the
     provider name. Used in edit/delete/toggle API endpoints instead of the
     numeric id.

2. Existing Rows
   - Slug values are generated from the existing `name` column: lowercased,
     spaces and non-alphanumeric characters replaced with hyphens, and
     consecutive hyphens collapsed.

3. Notes
   - The slug is kept in sync with the name on every UPDATE via the application
     layer. Uniqueness is enforced at the database level.
"""
import re
import sqlite3


def _slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[\s_]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "slug" not in existing:
        conn.execute("ALTER TABLE providers ADD COLUMN slug TEXT")

        rows = conn.execute("SELECT id, name FROM providers").fetchall()
        for row in rows:
            slug = _slugify(row["name"])
            conn.execute("UPDATE providers SET slug = ? WHERE id = ?", (slug, row["id"]))

    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_slug ON providers (slug)")
    conn.commit()

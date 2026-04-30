"""
Add TMDB integration tables and columns.

New tables:
  - tmdb_shows: cached show metadata keyed by TMDB ID
  - tmdb_seasons: per-season metadata (poster, episode count) child of tmdb_shows
  - tmdb_movies: cached movie metadata keyed by TMDB ID
  - tmdb_run_log: rolling log of the last 10 enrichment runs

Modified tables:
  - entries: adds tmdb_id (INTEGER) and tmdb_type (TEXT) columns

Security: no RLS needed — this is SQLite.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    conn.executescript("""
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
    """)

    existing = {row[1] for row in conn.execute("PRAGMA table_info(entries)").fetchall()}
    if "tmdb_id" not in existing:
        conn.execute("ALTER TABLE entries ADD COLUMN tmdb_id INTEGER DEFAULT NULL")
    if "tmdb_type" not in existing:
        conn.execute("ALTER TABLE entries ADD COLUMN tmdb_type TEXT DEFAULT NULL")

    conn.commit()

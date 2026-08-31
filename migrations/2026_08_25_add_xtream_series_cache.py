"""Add the incremental series cache for native Xtream Player API ingestion."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    tables = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "xtream_series_cache" in tables:
        log.info("  xtream_series_cache table already exists, skipping creation")
    else:
        log.info("  Creating xtream_series_cache table")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS xtream_series_cache (
            provider_slug   TEXT NOT NULL REFERENCES providers(slug)
                            ON UPDATE CASCADE ON DELETE CASCADE,
            series_id       TEXT NOT NULL,
            series_name     TEXT NOT NULL,
            last_modified  TEXT,
            episodes_json  TEXT NOT NULL DEFAULT '[]',
            fetch_status   TEXT NOT NULL DEFAULT 'ok'
                           CHECK(fetch_status IN ('ok', 'error')),
            fetched_at     TEXT NOT NULL,
            PRIMARY KEY (provider_slug, series_id)
        )
        """
    )
    indexes = {
        row[1]
        for row in conn.execute(
            "PRAGMA index_list(xtream_series_cache)"
        ).fetchall()
    }
    if "idx_xtream_series_cache_refresh" not in indexes:
        log.info("  Creating idx_xtream_series_cache_refresh index")
    else:
        log.info("  idx_xtream_series_cache_refresh already exists, skipping")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xtream_series_cache_refresh
        ON xtream_series_cache(provider_slug, fetch_status, fetched_at)
        """
    )
    conn.commit()

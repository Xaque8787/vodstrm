"""Persist native Xtream series summaries before episode details are loaded."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    tables = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "xtream_series_catalog" in tables:
        log.info("  xtream_series_catalog table already exists, skipping creation")
    else:
        log.info("  Creating xtream_series_catalog table")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS xtream_series_catalog (
            provider_slug  TEXT NOT NULL REFERENCES providers(slug)
                           ON UPDATE CASCADE ON DELETE CASCADE,
            series_id      TEXT NOT NULL,
            series_name    TEXT NOT NULL,
            cover          TEXT,
            category_id    TEXT,
            last_modified TEXT,
            metadata_json  TEXT NOT NULL DEFAULT '{}',
            updated_at     TEXT NOT NULL,
            PRIMARY KEY (provider_slug, series_id)
        )
        """
    )
    indexes = {
        row[1]
        for row in conn.execute(
            "PRAGMA index_list(xtream_series_catalog)"
        ).fetchall()
    }
    for idx_name, idx_sql in [
        ("idx_xtream_series_catalog_name",
         "CREATE INDEX IF NOT EXISTS idx_xtream_series_catalog_name "
         "ON xtream_series_catalog(series_name COLLATE NOCASE)"),
        ("idx_xtream_series_catalog_provider_name",
         "CREATE INDEX IF NOT EXISTS idx_xtream_series_catalog_provider_name "
         "ON xtream_series_catalog(provider_slug, series_name COLLATE NOCASE)"),
    ]:
        if idx_name not in indexes:
            log.info("  Creating %s index", idx_name)
        else:
            log.info("  %s already exists, skipping", idx_name)
        conn.execute(idx_sql)
    conn.commit()

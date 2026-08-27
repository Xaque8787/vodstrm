"""Persist native Xtream series summaries before episode details are loaded."""


def up(conn) -> None:
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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xtream_series_catalog_name
        ON xtream_series_catalog(series_name COLLATE NOCASE)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xtream_series_catalog_provider_name
        ON xtream_series_catalog(provider_slug, series_name COLLATE NOCASE)
        """
    )
    conn.commit()
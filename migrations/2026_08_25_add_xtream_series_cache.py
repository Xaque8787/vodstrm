"""Add the incremental series cache for native Xtream Player API ingestion."""


def up(conn) -> None:
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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xtream_series_cache_refresh
        ON xtream_series_cache(provider_slug, fetch_status, fetched_at)
        """
    )
    conn.commit()
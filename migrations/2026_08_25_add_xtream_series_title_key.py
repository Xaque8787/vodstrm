"""Add a normalized title key for fast series summary grouping and search."""


def up(conn) -> None:
    columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(xtream_series_catalog)"
        ).fetchall()
    }
    if "title_key" not in columns:
        conn.execute(
            "ALTER TABLE xtream_series_catalog ADD COLUMN title_key TEXT"
        )
    conn.execute(
        """
        UPDATE xtream_series_catalog
        SET title_key = lower(trim(series_name))
        WHERE title_key IS NULL OR title_key = ''
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xtream_series_catalog_title_key
        ON xtream_series_catalog(title_key)
        """
    )
    conn.commit()
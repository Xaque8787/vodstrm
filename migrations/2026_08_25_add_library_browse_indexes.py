"""Add indexes used by Library grouping, ordering, and provider lookups."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)

    indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(entries)").fetchall()
    }
    if "idx_entries_type_cleaned_title" not in indexes:
        log.info("  Creating idx_entries_type_cleaned_title index")
    else:
        log.info("  idx_entries_type_cleaned_title already exists, skipping")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entries_type_cleaned_title
        ON entries(type, cleaned_title)
        """
    )

    indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(streams)").fetchall()
    }
    if "idx_streams_provider_entry" not in indexes:
        log.info("  Creating idx_streams_provider_entry index")
    else:
        log.info("  idx_streams_provider_entry already exists, skipping")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_streams_provider_entry
        ON streams(provider, entry_id)
        """
    )
    conn.commit()

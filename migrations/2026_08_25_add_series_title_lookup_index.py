"""Index case-insensitive series title lookups used by Library cards."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(entries)").fetchall()
    }
    if "idx_entries_type_lower_cleaned_title" not in indexes:
        log.info("  Creating idx_entries_type_lower_cleaned_title index")
    else:
        log.info("  idx_entries_type_lower_cleaned_title already exists, skipping")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_entries_type_lower_cleaned_title
        ON entries(type, lower(cleaned_title))
        """
    )
    conn.commit()

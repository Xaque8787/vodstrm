"""Track the persistent offline file separately from its VOD hard link."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(downloads)").fetchall()
    }
    if "offline_path" in existing:
        log.info("  downloads.offline_path already exists, skipping")
        conn.commit()
        return
    log.info("  Adding downloads.offline_path column")
    conn.execute("ALTER TABLE downloads ADD COLUMN offline_path TEXT")
    conn.commit()

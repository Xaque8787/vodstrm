"""Add persistent byte counters for download progress reporting."""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(downloads)").fetchall()
    }
    added = []
    if "expected_size" not in existing:
        conn.execute("ALTER TABLE downloads ADD COLUMN expected_size INTEGER")
        added.append("expected_size")
    if "downloaded_bytes" not in existing:
        conn.execute(
            "ALTER TABLE downloads ADD COLUMN downloaded_bytes INTEGER NOT NULL DEFAULT 0"
        )
        added.append("downloaded_bytes")
    if added:
        log.info("  Added downloads columns: %s", ", ".join(added))
    else:
        log.info("  Progress columns already exist on downloads, skipping")
    conn.commit()

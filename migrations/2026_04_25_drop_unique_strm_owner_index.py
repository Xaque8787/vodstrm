"""
Drop idx_unique_strm_owner if it exists.

This index was added in error. It created a partial UNIQUE constraint on
streams(entry_id) WHERE strm_path IS NOT NULL, which caused UNIQUE constraint
violations during STRM sync because _sync_one sets strm_path on the winner
before losers are cleared in the same iteration. The invariant is enforced
by the sync engine logic, not at the DB level.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)

    indexes = {
        row[1]
        for row in conn.execute("PRAGMA index_list(streams)").fetchall()
    }
    if "idx_unique_strm_owner" not in indexes:
        log.info("  idx_unique_strm_owner does not exist, skipping")
        conn.commit()
        return

    log.info("  Dropping idx_unique_strm_owner index")
    try:
        conn.execute("DROP INDEX IF EXISTS idx_unique_strm_owner")
    except Exception:
        pass
    conn.commit()

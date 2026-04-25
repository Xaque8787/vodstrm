"""
Drop idx_unique_strm_owner if it exists.

This index was added in error. It created a partial UNIQUE constraint on
streams(entry_id) WHERE strm_path IS NOT NULL, which caused UNIQUE constraint
violations during STRM sync because _sync_one sets strm_path on the winner
before losers are cleared in the same iteration. The invariant is enforced
by the sync engine logic, not at the DB level.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("DROP INDEX IF EXISTS idx_unique_strm_owner")
        conn.commit()
    except Exception:
        pass

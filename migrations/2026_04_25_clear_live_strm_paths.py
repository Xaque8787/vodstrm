"""
Clear strm_path and last_written_url on live streams.

Live TV entries no longer generate .strm files — they are served exclusively
via per-provider and combined .m3u files in data/vod/livetv/. This migration
clears any strm_path values that were written before this change so the STRM
orphan cleanup pass can remove the now-redundant files from disk on the next
generate_strm run.
"""
import logging
import sqlite3


def up(conn: sqlite3.Connection, logger: logging.Logger = None) -> None:
    log = logger or logging.getLogger(__name__)

    count = conn.execute(
        """
        SELECT COUNT(*) FROM streams
        WHERE strm_path IS NOT NULL
          AND entry_id IN (SELECT entry_id FROM entries WHERE type = 'live')
        """
    ).fetchone()[0]

    if count == 0:
        log.info("  No live streams with strm_path set, skipping")
        conn.commit()
        return

    log.info("  Clearing strm_path on %d live streams", count)
    conn.execute(
        """
        UPDATE streams
        SET strm_path = NULL, last_written_url = NULL
        WHERE strm_path IS NOT NULL
          AND entry_id IN (SELECT entry_id FROM entries WHERE type = 'live')
        """
    )
    conn.commit()

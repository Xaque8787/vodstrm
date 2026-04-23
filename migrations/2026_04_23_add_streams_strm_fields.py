"""
Migration: add STRM tracking and import-selection fields to streams

Adds three columns to the streams table to support the STRM file sync engine
and the future import-selection pipeline:

  strm_path        — absolute filesystem path of the .strm file written for
                     this stream; NULL until the file is first generated.

  last_written_url — the stream URL that was written into the .strm file on
                     the most recent sync; used to detect URL changes so that
                     unchanged files are not rewritten unnecessarily.

  imported         — boolean flag (0/1) reserved for the future import-
                     selection pipeline.  When TRUE the stream was explicitly
                     chosen by the user for STRM generation via the UI.
                     Defaults to FALSE; not used by the automatic generator.

No data is changed; all three columns are nullable / default-zero so the
migration is safe on any existing dataset.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(streams)").fetchall()}

    if "strm_path" not in existing:
        conn.execute("ALTER TABLE streams ADD COLUMN strm_path TEXT")

    if "last_written_url" not in existing:
        conn.execute("ALTER TABLE streams ADD COLUMN last_written_url TEXT")

    if "imported" not in existing:
        conn.execute(
            "ALTER TABLE streams ADD COLUMN imported INTEGER NOT NULL DEFAULT 0"
        )

    conn.commit()

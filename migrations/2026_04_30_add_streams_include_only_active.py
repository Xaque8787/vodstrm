"""
Add include_only_active column to streams table.

This column records whether at least one include_only filter rule was in scope
for a given stream at the time filters were last evaluated. Combined with the
existing include_only column it allows eligibility checks to distinguish:

  include_only_active=0               → no include_only rule applied, stream is eligible
  include_only_active=1, include_only=1 → rule applied and stream matched, eligible
  include_only_active=1, include_only=0 → rule applied but stream did not match, suppress

Without this column, include_only=0 is ambiguous: it could mean "no rule" or
"rule existed but stream didn't match". The STRM engine and library queries
now use:  AND (s.include_only_active = 0 OR s.include_only = 1)

Default 0 is correct for existing rows — they will get the accurate value on
the next filter reapply run.
"""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(streams)").fetchall()}
    if "include_only_active" not in existing:
        conn.execute(
            "ALTER TABLE streams ADD COLUMN include_only_active INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()

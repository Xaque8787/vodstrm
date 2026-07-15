"""Create the downloads queue table and add mode column to follows."""
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    # ── follows: add mode column ────────────────────────────────────────────
    existing = {row[1] for row in conn.execute("PRAGMA table_info(follows)").fetchall()}
    if "mode" not in existing:
        conn.execute(
            "ALTER TABLE follows ADD COLUMN mode TEXT NOT NULL DEFAULT 'strm' "
            "CHECK(mode IN ('strm', 'download'))"
        )

    # ── downloads table ─────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            entry_id          TEXT PRIMARY KEY REFERENCES entries(entry_id) ON DELETE SET NULL,
            status            TEXT NOT NULL DEFAULT 'pending'
                              CHECK(status IN ('pending','probing','downloading','completed','failed','cancelled')),
            mode              TEXT NOT NULL DEFAULT 'download',
            stream_url        TEXT,
            provider          TEXT,
            container         TEXT DEFAULT 'mkv',
            staging_path      TEXT,
            local_path        TEXT,
            probe_data        TEXT,
            file_size         INTEGER,
            fail_reason       TEXT,
            retry_count       INTEGER NOT NULL DEFAULT 0,
            reencode_eligible INTEGER NOT NULL DEFAULT 0,
            queued_at         TEXT,
            probing_at        TEXT,
            downloading_at    TEXT,
            completed_at      TEXT,
            failed_at         TEXT,
            cancelled_at      TEXT,
            updated_at        TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_entry_id ON downloads(entry_id)")
    conn.commit()

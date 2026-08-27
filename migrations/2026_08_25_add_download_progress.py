"""Add persistent byte counters for download progress reporting."""


def up(conn) -> None:
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(downloads)").fetchall()
    }
    if "expected_size" not in existing:
        conn.execute("ALTER TABLE downloads ADD COLUMN expected_size INTEGER")
    if "downloaded_bytes" not in existing:
        conn.execute(
            "ALTER TABLE downloads ADD COLUMN downloaded_bytes INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()
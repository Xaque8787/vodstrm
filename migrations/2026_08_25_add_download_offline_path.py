"""Track the persistent offline file separately from its VOD hard link."""


def up(conn) -> None:
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(downloads)").fetchall()
    }
    if "offline_path" not in existing:
        conn.execute("ALTER TABLE downloads ADD COLUMN offline_path TEXT")
    conn.commit()
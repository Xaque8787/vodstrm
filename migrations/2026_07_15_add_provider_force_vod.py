"""
Add force_vod column to providers.

When enabled (1), the parser skips the duration == -1 live-TV check and
classifies all entries as VOD (series / tv_vod / movie / unsorted).
"""


def up(conn) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
    if "force_vod" not in existing:
        conn.execute(
            "ALTER TABLE providers ADD COLUMN force_vod INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()
